package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
	"strconv"
)

const maxJobSize = 4000

type idMsgBundle struct {
	id  int
	msg *bitcoin.Message
}

type job struct {
	clientid int
	message  string
	minNonce uint64
	maxNonce uint64
}

// Storing the progress and result of each client request
type request struct {
	jobsRemain int
	minHash    uint64
	nonce      uint64
}

type server struct {
	lspServer  lsp.Server
	jobQueue   *list.List
	miners     map[int]*job
	clients    map[int]*request
	receiveMsg chan idMsgBundle
	connLost   chan int
}

func startServer(port int) (*server, error) {
	lspserver, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}

	srv := &server{
		lspServer:  lspserver,
		jobQueue:   list.New(),
		miners:     make(map[int]*job),
		clients:    make(map[int]*request),
		receiveMsg: make(chan idMsgBundle),
		connLost:   make(chan int),
	}
	return srv, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println("Failed to start server:", err)
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	go srv.receiveMessage()

	srv.handleMessage()
}

func (s *server) handleMessage() {
	for {
		select {
		case bundle := <-s.receiveMsg:
			id := bundle.id
			msg := bundle.msg
			switch msg.Type {
			case bitcoin.Join:
				if _, exist := s.miners[id]; !exist {
					s.miners[id] = nil
					s.dispatchJobsToMiners()
				}
			case bitcoin.Request:
				fmt.Println(msg)
				s.addJob(id, msg)
				s.dispatchJobsToMiners()
			case bitcoin.Result:
				clientId := s.miners[id].clientid
				clientJob := s.clients[clientId]
				clientJob.jobsRemain--
				if msg.Hash < s.clients[clientId].minHash {
					clientJob.minHash = msg.Hash
					clientJob.nonce = msg.Nonce
				}
				s.miners[id] = nil
				s.dispatchJobsToMiners()
				// Has finished client's job. Return result to client
				if clientJob.jobsRemain == 0 {
					s.sendMessage(clientId, bitcoin.NewResult(clientJob.minHash, clientJob.nonce))
					s.lspServer.CloseConn(clientId)
					delete(s.clients, clientId)
				}
			}
		case id := <-s.connLost:
			fmt.Println(id, "is lost")
		}
	}
}

func (s *server) receiveMessage() {
	for {
		id, bytes, err := s.lspServer.Read()
		if err != nil {
			s.connLost <- id
		} else {
			msg := &bitcoin.Message{}
			if err := json.Unmarshal(bytes, msg); err != nil {
				continue
			}
			s.receiveMsg <- idMsgBundle{id, msg}
		}
	}
}

func (s *server) sendMessage(id int, msg *bitcoin.Message) error {
	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if err := s.lspServer.Write(id, bytes); err != nil {
		return err
	}
	return nil
}

func (s *server) addJob(id int, task *bitcoin.Message) {
	newRequest := &request{0, ^uint64(0), uint64(0)}
	lower, upper := task.Lower, task.Lower+maxJobSize-1

	for upper < task.Upper {
		newRequest.jobsRemain++
		s.jobQueue.PushBack(job{id, task.Data, lower, upper})
		lower = upper + 1
		upper = lower + maxJobSize - 1
	}
	newRequest.jobsRemain++
	s.jobQueue.PushBack(job{id, task.Data, lower, task.Upper})
	s.clients[id] = newRequest
}

func (s *server) findIdleMiner() (int, bool) {
	for minerId, job := range s.miners {
		if job == nil {
			return minerId, true
		}
	}
	return 0, false
}

func (s *server) dispatchJobsToMiners() {
	for s.jobQueue.Len() > 0 {
		if minerId, exist := s.findIdleMiner(); exist {
			job := s.jobQueue.Front().Value.(job)
			s.jobQueue.Remove(s.jobQueue.Front())
			s.miners[minerId] = &job
			s.sendMessage(minerId, bitcoin.NewRequest(job.message, job.minNonce, job.maxNonce))
		} else {
			break
		}
	}
}
