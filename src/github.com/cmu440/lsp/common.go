package lsp

import (
	"container/list"
)

//
type msgStatus int

const (
	QUEUED msgStatus = iota
	PROCESSED
	ACKED
)

type msgQueue struct {
	list   *list.List
	window int
}

type node struct {
	msg    *Message
	status msgStatus
}

func NewQueue(windowSize int) *msgQueue {
	return &msgQueue{
		list.New(),
		windowSize,
	}
}

func (q *msgQueue) Offer(msg *Message) {
	if q.list.Len() == 0 || msg.SeqNum > q.list.Back().Value.(node).msg.SeqNum {
		q.list.PushBack(node{msg, QUEUED})
		return
	}
	elem := q.list.Back()
	for elem != nil && elem.Value.(node).msg.SeqNum > msg.SeqNum {
		elem = elem.Prev()
	}
	if elem == nil {
		q.list.PushFront(node{msg, QUEUED})
	} else if elem.Value.(node).msg.SeqNum < msg.SeqNum {
		// Do this check to prevent duplicate message
		q.list.InsertAfter(node{msg, QUEUED}, elem)
	}
}

func (q *msgQueue) Poll() *Message {
	msg := q.list.Front().Value.(node).msg
	q.list.Remove(q.list.Front())
	return msg
}

func (q *msgQueue) Peek() *Message {
	return q.list.Front().Value.(node).msg
}

func (q *msgQueue) Len() int {
	return q.list.Len()
}
