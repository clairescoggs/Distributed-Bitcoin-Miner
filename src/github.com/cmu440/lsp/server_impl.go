// Contains the implementation of a LSP server.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
)

type server struct {
	currConnId         int
	listener           *lspnet.UDPConn
	closed             bool
	addr2ConnId        map[string]int
	connId2Addr        map[int]*lspnet.UDPAddr
	connAlive          map[int]bool
	outSeqNum          map[int]int
	inMsgQueue         map[int]*list.List
	receiveMsgFromAddr chan addrMsgBundle
	requestRead        chan bool
	pendingRead        bool
	responseRead       chan *Message
	requestWrite       chan idPayloadBundle
	requestCloseConn   chan int
	requestClose       chan bool
	quitReceiveData    chan bool
	readyToClose       chan bool
}

type addrMsgBundle struct {
	addr *lspnet.UDPAddr
	msg  *Message
}

type idPayloadBundle struct {
	connId  int
	payload []byte
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	s := server{
		0,
		nil,
		false,
		make(map[string]int),
		make(map[int]*lspnet.UDPAddr),
		make(map[int]bool),
		make(map[int]int),
		make(map[int]*list.List),
		make(chan addrMsgBundle),
		make(chan bool),
		false,
		make(chan *Message),
		make(chan idPayloadBundle),
		make(chan int),
		make(chan bool),
		make(chan bool),
		make(chan bool),
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

	s.listener = conn

	go s.receiveData()
	go s.handleEvents()
	return &s, nil
}

func (s *server) receiveData() {
	var buf [2000]byte
	for {
		select {
		case <-s.quitReceiveData:
			fmt.Println("Quit from receiveData()")
			return
		default:
		}

		n, addr, err := s.listener.ReadFromUDP(buf[:])
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
					s.currConnId++
					s.addr2ConnId[addrStr] = s.currConnId
					s.connId2Addr[s.currConnId] = addr
					s.connAlive[s.currConnId] = true
					s.outSeqNum[s.currConnId] = 0
					s.inMsgQueue[s.currConnId] = list.New()
				}
				s.sendDataToAddr(NewAck(s.addr2ConnId[addrStr], 0), addr)
			case MsgData:
				if clientId, exist := s.addr2ConnId[addrStr]; exist && s.connAlive[clientId] {
					if s.pendingRead {
						s.pendingRead = false
						s.responseRead <- msg
					} else {
						s.inMsgQueue[clientId].PushBack(msg)
					}
				}
				s.sendDataToAddr(NewAck(s.addr2ConnId[addrStr], msg.SeqNum), addr)
			case MsgAck:
			}
		case <-s.requestRead:
			available := false
			for _, list := range s.inMsgQueue {
				if list.Len() > 0 {
					head := list.Front()
					list.Remove(head)
					available = true
					s.responseRead <- head.Value.(*Message)
					break
				}
			}
			if !available {
				s.pendingRead = true
			}
		case bundle := <-s.requestWrite:
			connId := bundle.connId
			payload := bundle.payload
			s.outSeqNum[connId]++
			msg := NewData(connId, s.outSeqNum[connId], len(payload), payload)
			s.sendDataToAddr(msg, s.connId2Addr[connId])
		case id := <-s.requestCloseConn:
			s.connAlive[id] = false
		case <-s.requestClose:
			s.closed = true
			s.readyToClose <- true
		}
	}
}

func (s *server) sendDataToAddr(msg *Message, addr *lspnet.UDPAddr) error {
	bytes, _ := json.Marshal(msg)
	if _, err := s.listener.WriteToUDP(bytes, addr); err != nil {
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
	if !s.connAlive[connID] {
		return errors.New("Connection with client " + strconv.Itoa(connID) + " has lost")
	}
	s.requestWrite <- idPayloadBundle{connID, payload}
	return nil
}

func (s *server) CloseConn(connID int) error {
	if alive, exist := s.connAlive[connID]; !exist || !alive {
		return errors.New("Connection with " + strconv.Itoa(connID) + " does not exist")
	}
	s.requestCloseConn <- connID
	return nil
}

func (s *server) Close() error {
	s.quitReceiveData <- true
	s.requestClose <- true
	<-s.readyToClose
	s.listener.Close()
	return nil
}
