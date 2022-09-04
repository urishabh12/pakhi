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
		t.Fatal("subscriber was not deleted")
	}

	if err == nil {
		t.Fatal("subscriber does not exists it should throw error")
	}
}
