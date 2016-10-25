package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
	"strconv"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}
	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a number.\n", os.Args[3])
		return
	}

	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	defer client.Close()

	bytes, _ := json.Marshal(bitcoin.NewRequest(message, 0, maxNonce))
	if err := client.Write(bytes); err != nil {
		fmt.Println("Failed to send request to server:", err)
		return
	}

	if response, err := client.Read(); err != nil {
		printDisconnected()
	} else {
		result := &bitcoin.Message{}
		if err := json.Unmarshal(response, result); err != nil {
			fmt.Println("Unable to unmarshal response from server:", err)
			return
		}
		printResult(result.Hash, result.Nonce)
	}
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
