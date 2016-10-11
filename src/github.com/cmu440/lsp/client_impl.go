// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

type client struct {
	params      *Params
	connId      int
	outSeqNum   int
	inMsgQueue  *msgQueue
	outMsgQueue *msgQueue
	serverAddr  *lspnet.UDPAddr
	conn        *lspnet.UDPConn

	// Epoch
	epochTicker *time.Ticker
	epochCount  int

	// Client Status
	connected   bool
	pendingRead bool // There is a Read request waiting to be responsed
	closed      bool // Explicitly closed
	connLost    bool // Connection lost due to timeout

	// Channels
	connSucceed     chan bool
	connFail        chan bool
	receiveMsg      chan *Message
	requestRead     chan bool
	responseRead    chan *Message
	requestWrite    chan []byte
	requestClose    chan bool
	readyToClose    chan bool
	quitReceiveData chan bool
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	c := client{
		params:          params,
		connId:          0,
		outSeqNum:       0,
		inMsgQueue:      NewQueue(params.WindowSize),
		outMsgQueue:     NewQueue(params.WindowSize),
		serverAddr:      nil,
		conn:            nil,
		epochTicker:     nil,
		epochCount:      0,
		connected:       false,
		pendingRead:     false,
		closed:          false,
		connLost:        false,
		connSucceed:     make(chan bool),
		connFail:        make(chan bool),
		receiveMsg:      make(chan *Message),
		requestRead:     make(chan bool),
		responseRead:    make(chan *Message),
		requestWrite:    make(chan []byte),
		requestClose:    make(chan bool),
		readyToClose:    make(chan bool),
		quitReceiveData: make(chan bool, 2),
	}

	serverAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return nil, err
	}
	c.serverAddr = serverAddr
	c.conn = conn

	if err := c.sendData(NewConnect()); err != nil {
		return nil, err
	}

	c.epochTicker = time.NewTicker(time.Millisecond * time.Duration(params.EpochMillis))

	go c.receiveData()
	go c.handleEvents()

	select {
	case <-c.connSucceed:
		return &c, nil
	case <-c.connFail:
		c.conn.Close()
		return nil, errors.New("Timeout when trying to make connection")
	}
}

func (c *client) receiveData() {
	defer fmt.Println("Quit from receiveData()")
	var buf [2000]byte
	for {
		select {
		case <-c.quitReceiveData:
			return
		default:
			n, _, err := c.conn.ReadFromUDP(buf[:])
			if err != nil {
				fmt.Println("Error occured when receiving data:", err.Error())
				continue
			}
			msg := &Message{}
			if err := json.Unmarshal(buf[:n], msg); err != nil {
				continue
			}
			c.receiveMsg <- msg
		}
	}
}

func (c *client) handleEvents() {
	for {
		select {
		case msg := <-c.receiveMsg:
			switch msg.Type {
			case MsgData:
				if c.pendingRead {
					c.pendingRead = false
					c.responseRead <- msg
				} else {
					c.inMsgQueue.Offer(msg)
				}
				c.sendData(NewAck(c.connId, msg.SeqNum))
			case MsgAck:
				//fmt.Println("***Received ack: " + msg.String())
				if msg.SeqNum == 0 {
					if !c.connected {
						c.connected = true
						c.connId = msg.ConnID
						c.connSucceed <- true
					}
				}
			}
		case <-c.epochTicker.C:
			c.epochCount++
			if c.epochCount > c.params.EpochLimit {
				// Limit reached
				fmt.Println("Reach epoch limit.")
				if !c.connected {
					c.quitReceiveData <- true
					c.connFail <- true
					return
				}
				if c.pendingRead {
					c.responseRead <- nil
				}
				c.connLost = true
			}
			if !c.connected {
				c.sendData(NewConnect())
			}

		case <-c.requestRead:
			if c.inMsgQueue.Len() > 0 {
				msg := c.inMsgQueue.Poll()
				c.responseRead <- msg
			} else {
				c.pendingRead = true
			}
		case payload := <-c.requestWrite:
			c.outSeqNum++
			msg := NewData(c.connId, c.outSeqNum, len(payload), payload)
			//c.outMsgQueue.PushBack(msg)
			//fmt.Println("***Sending data: " + msg.String())
			c.sendData(msg)
		case <-c.requestClose:
			c.closed = true
			c.readyToClose <- true
		}
	}
}

func (c *client) sendData(msg *Message) error {
	bytes, _ := json.Marshal(msg)
	if _, err := c.conn.Write(bytes); err != nil {
		return err
	}
	return nil
}

func (c *client) ConnID() int {
	return c.connId
}

func (c *client) Read() ([]byte, error) {
	if c.closed {
		return nil, errors.New("Connection has been closed")
	}
	if c.connLost {
		// Already timed out
		return nil, errors.New("Connection is lost due to timeout")
	}
	c.requestRead <- true
	msg := <-c.responseRead
	if msg != nil {
		return msg.Payload, nil
	} else {
		// Timed out during waiting for message
		return nil, errors.New("Connection is lost due to timeout")
	}
}

func (c *client) Write(payload []byte) error {
	if c.connLost {
		return errors.New("Connection is lost due to timeout")
	}
	c.requestWrite <- payload
	return nil
}

func (c *client) Close() error {
	c.quitReceiveData <- true
	c.requestClose <- true
	<-c.readyToClose
	return nil
}
