// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type addrMsgBundle struct {
	addr *lspnet.UDPAddr
	msg  *Message
}

type idPayloadBundle struct {
	connId  int
	payload []byte
}

type server struct {
	conn         *lspnet.UDPConn
	closed       bool
	safelyClosed bool // No client is lost during server close
	currConnId   int
	windowSize   int
	pendingRead  bool

	// Client status
	addr2ConnId map[string]int
	connId2Addr map[int]*lspnet.UDPAddr
	connClosed  map[int]bool
	connLost    map[int]bool
	readSeqNum  map[int]int
	writeSeqNum map[int]int
	inMsgQueue  map[int]*msgQueue
	outMsgQueue map[int]*msgQueue

	// Epoch
	epochTicker *time.Ticker
	epochCount  map[int]int
	epochLimit  int
	epochNoData map[int]bool

	// Channels
	receiveMsgFromAddr chan addrMsgBundle
	requestRead        chan bool
	responseRead       chan *Message
	requestWrite       chan idPayloadBundle
	responseWrite      chan bool
	requestCloseConn   chan int
	responseCloseConn  chan bool
	requestClose       chan bool
	readyToClose       chan bool
	quitReceiveData    chan bool
	quitHandleEvents   chan bool
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	s := server{
		conn:               nil,
		closed:             false,
		safelyClosed:       true,
		currConnId:         1,
		windowSize:         params.WindowSize,
		pendingRead:        false,
		addr2ConnId:        make(map[string]int),
		connId2Addr:        make(map[int]*lspnet.UDPAddr),
		connClosed:         make(map[int]bool),
		connLost:           make(map[int]bool),
		readSeqNum:         make(map[int]int),
		writeSeqNum:        make(map[int]int),
		inMsgQueue:         make(map[int]*msgQueue),
		outMsgQueue:        make(map[int]*msgQueue),
		epochTicker:        nil,
		epochCount:         make(map[int]int),
		epochLimit:         params.EpochLimit,
		epochNoData:        make(map[int]bool),
		receiveMsgFromAddr: make(chan addrMsgBundle),
		requestRead:        make(chan bool),
		responseRead:       make(chan *Message),
		requestWrite:       make(chan idPayloadBundle),
		responseWrite:      make(chan bool),
		requestCloseConn:   make(chan int),
		responseCloseConn:  make(chan bool),
		requestClose:       make(chan bool),
		readyToClose:       make(chan bool),
		quitReceiveData:    make(chan bool),
		quitHandleEvents:   make(chan bool),
	}
	hostport := lspnet.JoinHostPort("localhost", strconv.Itoa(port))
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	s.conn = conn
	s.epochTicker = time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis))

	go s.receiveData()
	go s.handleEvents()
	return &s, nil
}

func (s *server) receiveData() {
	var buf [2000]byte
	for {
		select {
		case <-s.quitReceiveData:
			return
		default:
			n, addr, err := s.conn.ReadFromUDP(buf[:])
			if err != nil {
				continue
			}
			msg := &Message{}
			if err := json.Unmarshal(buf[:n], msg); err != nil {
				continue
			}
			s.receiveMsgFromAddr <- addrMsgBundle{addr, msg}
		}
	}
}

func (s *server) handleEvents() {
	for {
		select {
		case bundle := <-s.receiveMsgFromAddr:
			addr := bundle.addr
			addrStr := addr.String()
			msg := bundle.msg
			switch msg.Type {
			case MsgConnect:
				if _, exist := s.addr2ConnId[addrStr]; !exist {
					s.addr2ConnId[addrStr] = s.currConnId
					s.connId2Addr[s.currConnId] = addr
					s.connClosed[s.currConnId] = false
					s.connLost[s.currConnId] = false
					s.readSeqNum[s.currConnId] = 1
					s.writeSeqNum[s.currConnId] = 1
					s.inMsgQueue[s.currConnId] = newQueue(s.windowSize)
					s.outMsgQueue[s.currConnId] = newQueue(s.windowSize)
					s.epochCount[s.currConnId] = 0
					s.currConnId++
				}
				s.sendDataToAddr(NewAck(s.addr2ConnId[addrStr], 0), addr)
			case MsgData:
				if id, exist := s.addr2ConnId[addrStr]; exist {
					// Reject message if size of the message is shorter than given size
					if s.closed || s.connClosed[id] || len(msg.Payload) < msg.Size {
						continue
					}
					s.sendDataToAddr(NewAck(s.addr2ConnId[addrStr], msg.SeqNum), addr)
					// Discard message that has already been read before
					if msg.SeqNum < s.readSeqNum[id] {
						continue
					}
					// Truncate if size of message is longer than given size
					if len(msg.Payload) > msg.Size {
						msg.Payload = msg.Payload[:msg.Size]
					}
					s.inMsgQueue[id].Offer(msg)
					if s.pendingRead && s.inMsgQueue[id].Peek().SeqNum == s.readSeqNum[id] {
						msg := s.inMsgQueue[id].Poll()
						s.readSeqNum[id]++
						s.pendingRead = false
						s.responseRead <- msg
					}
					s.epochCount[id] = 0
					s.epochNoData[id] = false
				}
			case MsgAck:
				if id, exist := s.addr2ConnId[addrStr]; exist {
					if s.outMsgQueue[id].SetAcked(msg.SeqNum) {
						if exist, msgs := s.outMsgQueue[id].ForwardWindow(); exist {
							for _, msg := range msgs {
								s.sendDataToAddr(msg, addr)
							}
						}
					}
					s.epochCount[id] = 0
					// Check if this client can close
					if s.connClosed[id] && s.outMsgQueue[id].Len() == 0 {
						if s.pendingRead {
							// Return an error to Read() request indicating that this client has closed
							s.pendingRead = false
							s.responseRead <- NewData(id, 0, 0, nil)
						}
						s.deleteClient(id)
					}
					if s.closed {
						s.closeIfReady()
					}
				}
			}
		case <-s.epochTicker.C:
			for id, _ := range s.connId2Addr {
				s.epochCount[id]++
				// Reach epoch limit
				if !s.connLost[id] && s.epochCount[id] >= s.epochLimit {
					s.connLost[id] = true
					if s.closed {
						s.safelyClosed = false
					}
					if s.pendingRead {
						// Return an error to Read() request indicating that this client is lost
						s.pendingRead = false
						s.responseRead <- NewData(id, 0, 0, nil)
						break
					}
				}
				// Hear nothing or receive no data for at least 1 epoch
				if s.epochNoData[id] || s.epochCount[id] >= 1 {
					s.sendDataToAddr(NewAck(id, 0), s.connId2Addr[id])
				}
				s.epochNoData[id] = true
				// Send all unacked messages
				if exist, msgs := s.outMsgQueue[id].UnackedMsgs(); exist {
					for _, msg := range msgs {
						s.sendDataToAddr(msg, s.connId2Addr[id])
					}
				}
				// Check if this client can close
				if s.connClosed[id] && s.connLost[id] {
					if s.pendingRead {
						// Return an error to Read() request indicating that this client has closed
						s.pendingRead = false
						s.responseRead <- NewData(id, 0, 0, nil)
					}
					s.deleteClient(id)
				}
				// Check if server can close
				if s.closed {
					s.closeIfReady()
				}
			}
		case <-s.requestRead:
			available := false
			for id, msgQueue := range s.inMsgQueue {
				if !s.connClosed[id] &&
					msgQueue.Len() > 0 && msgQueue.Peek().SeqNum == s.readSeqNum[id] {
					msg := msgQueue.Poll()
					available = true
					s.readSeqNum[id]++
					s.responseRead <- msg
					break
				}
			}
			if !available {
				s.pendingRead = true
			}
		case bundle := <-s.requestWrite:
			connId := bundle.connId
			payload := bundle.payload
			if lost, exist := s.connLost[connId]; exist && !lost {
				msg := NewData(connId, s.writeSeqNum[connId], len(payload), payload)
				s.outMsgQueue[connId].Offer(msg)
				if s.outMsgQueue[connId].WithinWindow() {
					s.sendDataToAddr(msg, s.connId2Addr[connId])
				}
				s.writeSeqNum[connId]++
				s.responseWrite <- true
			} else {
				s.responseWrite <- false
			}
		case id := <-s.requestCloseConn:
			if closed, exist := s.connClosed[id]; exist && !closed {
				s.connClosed[id] = true
				s.responseCloseConn <- true
			} else {
				s.responseCloseConn <- false
			}
		case <-s.requestClose:
			s.closed = true
			s.closeIfReady()
		case <-s.quitHandleEvents:
			return
		}
	}
}

func (s *server) sendDataToAddr(msg *Message, addr *lspnet.UDPAddr) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := s.conn.WriteToUDP(bytes, addr); err != nil {
		return err
	}
	return nil
}

func (s *server) deleteClient(id int) {
	addrStr := s.connId2Addr[id].String()
	delete(s.addr2ConnId, addrStr)
	delete(s.connId2Addr, id)
	delete(s.connClosed, id)
	delete(s.connLost, id)
	delete(s.readSeqNum, id)
	delete(s.writeSeqNum, id)
	delete(s.inMsgQueue, id)
	delete(s.outMsgQueue, id)
	delete(s.epochCount, id)
	delete(s.epochNoData, id)
}

func (s *server) closeIfReady() {
	canClose := true
	for id, msgQueue := range s.outMsgQueue {
		if msgQueue.Len() != 0 && !s.connLost[id] {
			canClose = false
			break
		}
	}
	if canClose {
		s.readyToClose <- s.safelyClosed
	}
}

func (s *server) Read() (int, []byte, error) {
	if s.closed {
		return 0, nil, errors.New("Server has been closed")
	}
	s.requestRead <- true
	msg := <-s.responseRead
	if msg.Payload == nil {
		return msg.ConnID, nil, errors.New("Client " + strconv.Itoa(msg.ConnID) + " is closed or has lost")
	}
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connID int, payload []byte) error {
	s.requestWrite <- idPayloadBundle{connID, payload}
	if !<-s.responseWrite {
		return errors.New("Client " + strconv.Itoa(connID) + " has lost")
	}
	return nil
}

func (s *server) CloseConn(connID int) error {
	s.requestCloseConn <- connID
	if !<-s.responseCloseConn {
		return errors.New("Client" + strconv.Itoa(connID) + " does not exist")
	}
	return nil
}

func (s *server) Close() error {
	s.requestClose <- true
	if !<-s.readyToClose {
		return errors.New("One or more clients are lost during server close")
	}
	s.conn.Close()
	s.quitReceiveData <- true
	s.quitHandleEvents <- true
	return nil
}
