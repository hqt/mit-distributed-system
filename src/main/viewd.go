package main

//
// see directions in pbc.go
//

import (
	"fmt"
	"os"
	"time"

	"github.com/hqt/mit-distributed-system/src/viewservice"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: viewd port\n")
		os.Exit(1)
	}

	viewservice.StartServer(os.Args[1])

	for {
		time.Sleep(100 * time.Second)
	}
}
