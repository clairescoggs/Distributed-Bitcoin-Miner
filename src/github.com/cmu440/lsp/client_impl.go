// Contains the implementation of a LSP client.

package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

type client struct {
	connected       bool
	connId          int
	outSeqNum       int
	inMsgQueue      *list.List
	outMsgQueue     *list.List
	serverAddr      *lspnet.UDPAddr
	conn            *lspnet.UDPConn
	closed          bool
	connAcked       chan bool
	connLost        chan bool
	receiveMsg      chan *Message
	requestRead     chan bool
	pendingRead     bool
	responseRead    chan *Message
	requestWrite    chan []byte
	requestClose    chan bool
	quitReceiveData chan bool
	readyToClose    chan bool
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
		false,
		0,
		0,
		list.New(),
		list.New(),
		nil,
		nil,
		false,
		make(chan bool),
		make(chan bool),
		make(chan *Message),
		make(chan bool),
		false,
		make(chan *Message),
		make(chan []byte),
		make(chan bool),
		make(chan bool),
		make(chan bool),
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

	go c.receiveData()
	go c.handleEvents()

	<-c.connAcked
	return &c, nil
}

func (c *client) receiveData() {
	var buf [2000]byte
	for {
		select {
		case <-c.quitReceiveData:
			fmt.Println("Quit from receiveData()")
			return
		default:
		}

		n, _, err := c.conn.ReadFromUDP(buf[:])
		if err != nil {
			fmt.Println("Error occured when receiving data")
			c.connLost <- true
			return
		}
		msg := &Message{}
		if err := json.Unmarshal(buf[:n], msg); err != nil {
			continue
		}
		c.receiveMsg <- msg
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
					c.inMsgQueue.PushBack(msg)
				}
				c.sendData(NewAck(c.connId, msg.SeqNum))
			case MsgAck:
				//fmt.Println("***Received ack: " + msg.String())
				if msg.SeqNum == 0 {
					if !c.connected {
						c.connected = true
						c.connId = msg.ConnID
						c.connAcked <- true
					}
				}
			}
		case <-c.requestRead:
			if c.inMsgQueue.Len() > 0 {
				head := c.inMsgQueue.Front()
				c.inMsgQueue.Remove(head)
				c.responseRead <- head.Value.(*Message)
			} else {
				c.pendingRead = true
			}
		case payload := <-c.requestWrite:
			c.outSeqNum++
			msg := NewData(c.connId, c.outSeqNum, len(payload), payload)
			//c.outMsgQueue.PushBack(msg)
			//fmt.Println("***Sending data: " + msg.String())
			c.sendData(msg)
		case <-c.connLost:
			fmt.Println("Set the status to closed")
			c.closed = true
		case <-c.requestClose:
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
	c.requestRead <- true
	msg := <-c.responseRead
	return msg.Payload, nil
}

func (c *client) Write(payload []byte) error {
	if c.closed {
		return errors.New("Connection has been closed")
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
