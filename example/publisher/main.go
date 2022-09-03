package main

import (
	"log"
	"time"

	"github.com/urishabh12/pakhi/publisher"
)

func main() {
	p, err := publisher.NewPublisher(":5565")
	if err != nil {
		log.Fatal("publisher failed", err)
	}

	singleTopicPublishing(p)
}

func singleTopicPublishing(p *publisher.Publisher) {
	for i := 0; i < 10; i++ {
		time.Sleep(2 * time.Second)
		p.Publish("car", "hello I am a car")
	}
}
