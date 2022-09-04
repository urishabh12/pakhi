package broker

import (
	te "testing"

	bp "github.com/urishabh12/pakhi/proto"
)

func Test_AddTopic(t *te.T) {
	s := CreateNewSubscriber()
	topic := "abc"
	s.AddTopic(topic)
	ok := false
	for _, t := range s.GetTopics() {
		if t == topic {
			ok = true
		}
	}

	if !ok {
		t.Fatal("topic was not added to subscriber")
	}
}

func Test_RemoveTopic(t *te.T) {
	s := CreateNewSubscriber()
	topic := "abc"
	s.AddTopic(topic)
	s.RemoveTopic(topic)
	ok := true
	for _, t := range s.GetTopics() {
		if t == topic {
			ok = false
		}
	}

	if !ok {
		t.Fatal("topic was not removed from subscriber")
	}
}

func Test_RemoveTopicThatDoesNotExists(t *te.T) {
	s := CreateNewSubscriber()
	topic := "abc"

	err := s.RemoveTopic(topic)

	if err != nil {
		t.Fatal("should not throw error when topic does not exists")
	}
}

func Test_CloseSubscriber(t *te.T) {
	s := CreateNewSubscriber()
	s.Close()

	if !s.IsClosed() {
		t.Fatal("subscriber not marked as closed")
	}
}

func Test_SendWhenReceiverReady(t *te.T) {
	s := CreateNewSubscriber()

	m := &bp.Message{
		Topic:   "a",
		Message: "b",
	}
	end := make(chan bool)

	//receiver
	go func(t *te.T, r chan *bp.Message, end chan bool) {
		msg, ok := <-r
		if !ok {
			t.Error("received error on receiver channel")
			end <- true
		}

		if msg.Topic != m.Topic || msg.Message != m.Message {
			t.Error("received a different message")
			end <- false
		}

		end <- true
	}(t, s.receiver, end)

	s.receiver <- m

	ok := <-end
	if !ok {
		t.Fatal("error occured in receiver")
	}
}

//need to find better way to test it
//if this test case fails the channel will be stuck and timeout
func Test_ReceiverClosesWhenSubscriberClosed(t *te.T) {
	s := CreateNewSubscriber()
	c := make(chan bool)

	//receiver
	go func(t *te.T, r chan *bp.Message, c chan bool) {
		msg, ok := <-r
		if !ok {
			t.Error("received error on receiver channel")
			c <- false
		}

		if !msg.Closed {
			t.Error("message is not closed")
			c <- false
		}

		c <- true
	}(t, s.receiver, c)

	s.Close()

	ok := <-c
	if !ok {
		t.Fatal("error occured in receiver")
	}
}
