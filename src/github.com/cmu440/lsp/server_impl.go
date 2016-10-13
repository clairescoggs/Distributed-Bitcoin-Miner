// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
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
	conn       *lspnet.UDPConn
	closed     bool
	currConnId int
	windowSize int

	// Client status
	addr2ConnId map[string]int
	connId2Addr map[int]*lspnet.UDPAddr
	connClosed  map[int]bool
	readSeqNum  map[int]int
	writeSeqNum map[int]int
	inMsgQueue  map[int]*msgQueue
	outMsgQueue map[int]*msgQueue

	// Epoch
	epochTicker *time.Ticker
	epochCount  map[int]int
	epochLimit  int

	// Channels
	receiveMsgFromAddr chan addrMsgBundle
	requestRead        chan bool
	pendingRead        bool
	responseRead       chan *Message
	requestWrite       chan idPayloadBundle
	requestCloseConn   chan int
	requestClose       chan bool
	readyToClose       chan bool
	quitReceiveData    chan bool
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
		currConnId:         1,
		windowSize:         params.WindowSize,
		addr2ConnId:        make(map[string]int),
		connId2Addr:        make(map[int]*lspnet.UDPAddr),
		connClosed:         make(map[int]bool),
		readSeqNum:         make(map[int]int),
		writeSeqNum:        make(map[int]int),
		inMsgQueue:         make(map[int]*msgQueue),
		outMsgQueue:        make(map[int]*msgQueue),
		epochTicker:        nil,
		epochCount:         make(map[int]int),
		epochLimit:         params.EpochLimit,
		receiveMsgFromAddr: make(chan addrMsgBundle),
		requestRead:        make(chan bool),
		pendingRead:        false,
		responseRead:       make(chan *Message),
		requestWrite:       make(chan idPayloadBundle),
		requestCloseConn:   make(chan int),
		requestClose:       make(chan bool),
		readyToClose:       make(chan bool),
		quitReceiveData:    make(chan bool),
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
	defer fmt.Println("Quit from receiveData")
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
			//fmt.Println(msg.String() + " from " + addr.String())

			switch msg.Type {
			case MsgConnect:
				if _, exist := s.addr2ConnId[addrStr]; !exist {
					s.addr2ConnId[addrStr] = s.currConnId
					s.connId2Addr[s.currConnId] = addr
					s.connClosed[s.currConnId] = false
					s.readSeqNum[s.currConnId] = 1
					s.writeSeqNum[s.currConnId] = 1
					s.inMsgQueue[s.currConnId] = NewQueue(s.windowSize)
					s.outMsgQueue[s.currConnId] = NewQueue(s.windowSize)
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
					// Truncate if size of message is longer than given size
					if len(msg.Payload) > msg.Size {
						msg.Payload = msg.Payload[:msg.Size]
					}
					s.inMsgQueue[id].Offer(msg)
					if s.pendingRead &&
						s.inMsgQueue[id].Peek().SeqNum == s.readSeqNum[id] {
						msg := s.inMsgQueue[id].Poll()
						s.readSeqNum[id]++
						s.pendingRead = false
						s.responseRead <- msg
					}
					s.epochCount[id] = 0
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
				}
			}
		case <-s.epochTicker.C:

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
			msg := NewData(connId, s.writeSeqNum[connId], len(payload), payload)
			s.outMsgQueue[connId].Offer(msg)
			if s.outMsgQueue[connId].WithinWindow() {
				s.sendDataToAddr(msg, s.connId2Addr[connId])
			}
			s.writeSeqNum[connId]++
		case id := <-s.requestCloseConn:
			s.connClosed[id] = true
		case <-s.requestClose:
			s.closed = true
			s.readyToClose <- true
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

func (s *server) Read() (int, []byte, error) {
	if s.closed {
		return 0, nil, errors.New("Server has been closed")
	}
	s.requestRead <- true
	msg := <-s.responseRead
	return msg.ConnID, msg.Payload, nil
}

func (s *server) Write(connID int, payload []byte) error {
	if s.connClosed[connID] {
		return errors.New("Connection with client " + strconv.Itoa(connID) + " has lost")
	}
	s.requestWrite <- idPayloadBundle{connID, payload}
	return nil
}

func (s *server) CloseConn(connID int) error {
	if closed, exist := s.connClosed[connID]; !exist || closed {
		return errors.New("Connection with " + strconv.Itoa(connID) + " does not exist")
	}
	s.requestCloseConn <- connID
	return nil
}

func (s *server) Close() error {
	s.quitReceiveData <- true
	s.requestClose <- true
	<-s.readyToClose
	s.conn.Close()
	return nil
}
