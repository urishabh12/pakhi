package main

import (
	"log"

	"github.com/urishabh12/pakhi/broker"
)

func main() {
	b, err := broker.NewBrokerServer(5565)
	if err != nil {
		log.Fatal("broker failed", err)
	}

	b.Start()
}
