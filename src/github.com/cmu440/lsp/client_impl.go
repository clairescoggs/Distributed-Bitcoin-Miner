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
	connId      int
	readSeqNum  int // The next seq num of message returned to Read()
	writeSeqNum int // The next seq num of message added by Write()
	inMsgQueue  *msgQueue
	outMsgQueue *msgQueue
	serverAddr  *lspnet.UDPAddr
	conn        *lspnet.UDPConn

	// Epoch
	epochTicker *time.Ticker
	epochCount  int
	epochLimit  int

	// Client Status
	connected   bool
	pendingRead bool // A Read request is waiting to be responded
	closed      bool // Explicitly closed
	connLost    bool // Connection lost due to timeout

	// Channels
	connSucceed      chan bool
	connFail         chan bool
	receiveMsg       chan *Message
	requestRead      chan bool
	responseRead     chan *Message
	requestWrite     chan []byte
	requestClose     chan bool
	readyToClose     chan bool
	quitReceiveData  chan bool
	quitHandleEvents chan bool
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
		connId:           0,
		readSeqNum:       1,
		writeSeqNum:      1,
		inMsgQueue:       NewQueue(params.WindowSize),
		outMsgQueue:      NewQueue(params.WindowSize),
		serverAddr:       nil,
		conn:             nil,
		epochTicker:      nil,
		epochCount:       0,
		epochLimit:       params.EpochLimit,
		connected:        false,
		pendingRead:      false,
		closed:           false,
		connLost:         false,
		connSucceed:      make(chan bool),
		connFail:         make(chan bool),
		receiveMsg:       make(chan *Message),
		requestRead:      make(chan bool),
		responseRead:     make(chan *Message),
		requestWrite:     make(chan []byte),
		requestClose:     make(chan bool),
		readyToClose:     make(chan bool),
		quitReceiveData:  make(chan bool),
		quitHandleEvents: make(chan bool),
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
		c.quitReceiveData <- true
		c.quitHandleEvents <- true
		c.conn.Close()
		return nil, errors.New("Timeout when trying to make connection")
	}
}

func (c *client) receiveData() {
	defer fmt.Println("Client quit from receiveData")
	var buf [2000]byte
	for {
		select {
		case <-c.quitReceiveData:
			return
		default:
			n, _, err := c.conn.ReadFromUDP(buf[:])
			if err != nil {
				//fmt.Println("Error occured when receiving data:", err.Error())
				continue
			}
			msg := &Message{}
			if err := json.Unmarshal(buf[:n], msg); err != nil {
				continue
			}
			//fmt.Println("--C-Receive", msg.String())
			c.receiveMsg <- msg
		}
	}
}

func (c *client) handleEvents() {
	defer fmt.Println("Client quit from handleEvents")
	for {
		select {
		case msg := <-c.receiveMsg:
			c.epochCount = 0
			switch msg.Type {
			case MsgData:
				// Reject message if size of the message is shorter than given size
				if !c.connected || c.closed || len(msg.Payload) < msg.Size {
					continue
				}
				c.sendData(NewAck(c.connId, msg.SeqNum))
				// Discard message that has already been read before
				if msg.SeqNum < c.readSeqNum {
					continue
				}
				// Truncate if size of message is longer than given size
				if len(msg.Payload) > msg.Size {
					msg.Payload = msg.Payload[:msg.Size]
				}
				c.inMsgQueue.Offer(msg)
				if c.pendingRead && c.inMsgQueue.Peek().SeqNum == c.readSeqNum {
					msg := c.inMsgQueue.Poll()
					c.readSeqNum++
					c.pendingRead = false
					c.responseRead <- msg
				}
			case MsgAck:
				//fmt.Println("***Received ack: " + msg.String())
				if msg.SeqNum == 0 {
					if !c.connected {
						c.connected = true
						c.connId = msg.ConnID
						c.connSucceed <- true
					}
				} else {
					if c.outMsgQueue.SetAcked(msg.SeqNum) {
						if exist, msgs := c.outMsgQueue.ForwardWindow(); exist {
							for _, msg := range msgs {
								c.sendData(msg)
							}
						}
					}
				}
			}
		case <-c.epochTicker.C:
			c.epochCount++
			// Reach epoch limit
			if c.epochCount > c.epochLimit {
				if !c.connected {
					c.connFail <- true
				} else if !c.connLost {
					if c.pendingRead {
						c.pendingRead = false
						c.responseRead <- nil
					}
					c.connLost = true
				}
			}
			if !c.connected {
				c.sendData(NewConnect())
			} else {
				// Check if can close
				// fmt.Println("Check if can close", c.closed, c.outMsgQueue.Len(), c.pendingRead)
				if c.closed && c.outMsgQueue.Len() == 0 {
					if c.pendingRead {
						c.pendingRead = false
						c.responseRead <- nil
					}
					c.readyToClose <- true
				}
				// Hear nothing from server for at least 1 epoch
				if c.epochCount > 1 {
					c.sendData(NewAck(c.connId, 0))
				}
				// Send unacked message again
				if exist, msgs := c.outMsgQueue.UnackedMsgs(); exist {
					for _, msg := range msgs {
						c.sendData(msg)
					}
				}
			}
		case <-c.requestRead:
			if c.inMsgQueue.Len() > 0 && c.inMsgQueue.Peek().SeqNum == c.readSeqNum {
				msg := c.inMsgQueue.Poll()
				c.readSeqNum++
				c.responseRead <- msg
				//fmt.Println("Client requesting read return directly")
			} else {
				c.pendingRead = true
				//fmt.Println("Client requesting read pending")
			}
		case payload := <-c.requestWrite:
			msg := NewData(c.connId, c.writeSeqNum, len(payload), payload)
			c.outMsgQueue.Offer(msg)
			// Send message out if within window size
			if c.outMsgQueue.WithinWindow() {
				c.sendData(msg)
			}
			c.writeSeqNum++
		case <-c.requestClose:
			c.closed = true
			if c.outMsgQueue.Len() == 0 {
				if c.pendingRead {
					c.pendingRead = false
					c.responseRead <- nil
				}
				c.readyToClose <- true
			}
		case <-c.quitHandleEvents:
			return
		}
	}
}

func (c *client) sendData(msg *Message) error {
	bytes, _ := json.Marshal(msg)
	if _, err := c.conn.Write(bytes); err != nil {
		return err
	}
	//fmt.Println("--C-Sent   ", msg.String())
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
	//fmt.Println("Client got something from read")
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
	c.requestClose <- true
	<-c.readyToClose
	c.conn.Close()
	c.quitReceiveData <- true
	c.quitHandleEvents <- true
	return nil
}
