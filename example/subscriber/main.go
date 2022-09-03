package main

import (
	"log"

	bp "github.com/urishabh12/pakhi/proto"
	"github.com/urishabh12/pakhi/subscriber"
)

func main() {
	s, err := subscriber.NewSubscriber(":5565")
	handleErr(err)

	err = s.Subscribe("car")
	handleErr(err)

	err = s.Listen(receiver)
	handleErr(err)
}

func receiver(msg *bp.Message) {
	log.Printf("msg '%s' received on topic %s", msg.Message, msg.Topic)
}

func handleErr(err error) {
	if err != nil {
		log.Fatal("subscriber failed", err)
	}
}
