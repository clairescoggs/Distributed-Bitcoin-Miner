package lsp

import (
	"container/list"
)

type msgQueue struct {
	list       *list.List
	windowSize int
}

type node struct {
	msg   *Message
	acked bool
}

// Create and return a new queue holding all messages ordered by sequence number
func newQueue(windowSize int) *msgQueue {
	return &msgQueue{
		list.New(),
		windowSize,
	}
}

func (q *msgQueue) Offer(msg *Message) {
	if q.list.Len() == 0 || msg.SeqNum > q.list.Back().Value.(*node).msg.SeqNum {
		q.list.PushBack(&node{msg, false})
		return
	}
	elem := q.list.Back()
	for elem != nil && elem.Value.(*node).msg.SeqNum > msg.SeqNum {
		elem = elem.Prev()
	}
	if elem == nil {
		q.list.PushFront(&node{msg, false})
	} else if elem.Value.(*node).msg.SeqNum < msg.SeqNum {
		// Do this check to prevent duplicate message
		q.list.InsertAfter(&node{msg, false}, elem)
	}
}

func (q *msgQueue) Poll() *Message {
	msg := q.list.Front().Value.(*node).msg
	q.list.Remove(q.list.Front())
	return msg
}

func (q *msgQueue) Peek() *Message {
	return q.list.Front().Value.(*node).msg
}

func (q *msgQueue) Len() int {
	return q.list.Len()
}

func (q *msgQueue) SetAcked(seqNum int) bool {
	for e := q.list.Front(); e != nil; e = e.Next() {
		if e.Value.(*node).msg.SeqNum == seqNum {
			e.Value.(*node).acked = true
			return true
		}
	}
	return false
}

func (q *msgQueue) WithinWindow() bool {
	return q.list.Len() <= q.windowSize
}

// Return all messages in the window that has not yet been acked
func (q *msgQueue) UnackedMsgs() (bool, []*Message) {
	numUnacked := 0
	for i, e := 0, q.list.Front(); i < q.windowSize && e != nil; i++ {
		if !e.Value.(*node).acked {
			numUnacked++
		}
		e = e.Next()
	}
	if numUnacked == 0 {
		return false, nil
	}
	msgs := make([]*Message, numUnacked)
	for i, e := 0, q.list.Front(); i < numUnacked; e = e.Next() {
		if !e.Value.(*node).acked {
			msgs[i] = e.Value.(*node).msg
			i++
		}
	}
	return true, msgs
}

// Move window forward and return all messages that can now be sent
func (q *msgQueue) ForwardWindow() (bool, []*Message) {
	if !q.list.Front().Value.(*node).acked {
		return false, nil
	}
	numAcked := 0
	for q.list.Front() != nil && q.list.Front().Value.(*node).acked {
		numAcked++
		q.list.Remove(q.list.Front())
	}
	e := q.list.Front()
	for i := 0; i < q.windowSize-numAcked; i++ {
		if e == nil {
			return false, nil
		}
		e = e.Next()
	}
	msgcount := 0
	for ee := e; msgcount < numAcked && ee != nil; ee = ee.Next() {
		msgcount++
	}
	msgs := make([]*Message, msgcount)
	for i := 0; i < msgcount; i++ {
		msgs[i] = e.Value.(*node).msg
		e = e.Next()
	}
	return true, msgs
}
