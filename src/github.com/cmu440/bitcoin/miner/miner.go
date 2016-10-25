package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	miner, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		return nil, err
	}

	bytes, _ := json.Marshal(bitcoin.NewJoin())
	if err := miner.Write(bytes); err != nil {
		return nil, err
	}
	return miner, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	for {
		bytes, err := miner.Read()
		if err != nil {
			fmt.Println("Failed to read from server:", err)
			return
		}

		task := &bitcoin.Message{}
		if err := json.Unmarshal(bytes, task); err != nil {
			fmt.Println("Unable to unmarshal response from server:", err)
			return
		}

		minHash := ^uint64(0) // Largest uint64
		nonce := task.Lower

		for n := task.Lower; n <= task.Upper; n++ {
			hash := bitcoin.Hash(task.Data, n)
			if hash < minHash {
				minHash = hash
				nonce = n
			}
		}

		response, _ := json.Marshal(bitcoin.NewResult(minHash, nonce))

		if err := miner.Write(response); err != nil {
			fmt.Println("Failed to write to server:", err)
			return
		}
	}
}
