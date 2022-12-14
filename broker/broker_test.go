package broker

import (
	te "testing"
)

func Test_AddSubscriber(t *te.T) {
	b := NewBroker()
	sub := b.AddSubscriber()
	_, err := b.GetSubscriberById(sub.id)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func Test_GetInvalidSubscriber(t *te.T) {
	b := NewBroker()
	_, err := b.GetSubscriberById("")
	if err == nil {
		t.Fatal("should get error when invalid subscriber id")
	}
}

func Test_GetRemovedSubscriber(t *te.T) {
	b := NewBroker()
	sub := b.AddSubscriber()
	err := b.RemoveSubscriber(sub.id)
	if err != nil {
		t.Fatal("subscriber was not removed")
	}
	_, err = b.GetSubscriberById(sub.id)
	if err == nil {
		t.Fatal("should get error when subscriber removed")
	}
}

func Test_RemoveSubscriber(t *te.T) {
	b := NewBroker()
	sub := b.AddSubscriber()
	err := b.RemoveSubscriber(sub.id)
	if err != nil {
		t.Fatal("error while removing subscriber", err)
	}

	res_sub, err := b.GetSubscriberById(sub.id)
	if res_sub != nil {
		t.Error("subscriber was not deleted")
	}

	if err == nil {
		t.Error("subscriber does not exists it should throw error")
	}
}

func Test_GetSubscribers(t *te.T) {
	b := NewBroker()
	subs := make(map[string]bool)
	for i := 0; i < 10; i++ {
		s := b.AddSubscriber()
		subs[s.id] = true
	}

	subs_b, err := b.GetSubscribers()
	if err != nil {
		t.Error("error getting all subscribers")
	}

	if len(subs) != len(subs_b) {
		t.Errorf("broker returned %d subscribers should have returned %d", len(subs_b), len(subs))
	}

	match_count := 0
	for i := 0; i < len(subs_b); i++ {
		if subs[subs_b[i].id] {
			match_count++
		}
	}

	if match_count != len(subs) {
		t.Error("broker did not returned same subscribers")
	}
}

func Test_GetSubscribersAfterRemoving(t *te.T) {
	b := NewBroker()
	subs := make(map[string]bool)
	for i := 0; i < 10; i++ {
		s := b.AddSubscriber()
		if i%2 == 0 {
			b.RemoveSubscriber(s.id)
			continue
		}
		subs[s.id] = true
	}

	subs_b, err := b.GetSubscribers()
	if err != nil {
		t.Error("error getting all subscribers")
	}

	if len(subs) != len(subs_b) {
		t.Errorf("broker returned %d subscribers should have returned %d", len(subs_b), len(subs))
	}

	match_count := 0
	for i := 0; i < len(subs_b); i++ {
		if subs[subs_b[i].id] {
			match_count++
		}
	}

	if match_count != len(subs) {
		t.Error("broker did not returned all valid subscribers")
	}
}

func Test_SubscribeToATopic(t *te.T) {
	b := NewBroker()
	sub := b.AddSubscriber()

	topics := map[string]bool{
		"a": false,
		"b": false,
		"c": false,
		"d": false,
	}
	for t := range topics {
		b.Subscribe(sub.id, t)
	}

	topics_b, err := b.GetTopicsBySubscriberId(sub.id)
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(topics) != len(topics_b) {
		t.Error("length of returned topics not equal")
	}

	for i := 0; i < len(topics_b); i++ {
		topics[topics_b[i]] = true
	}

	for _, v := range topics {
		if !v {
			t.Fatal("topic did not exists for subscriber")
		}
	}
}
