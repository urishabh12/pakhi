package broker

import (
	"errors"
	"fmt"
	"log"
	"sync"

	bp "github.com/urishabh12/pakhi/proto"
)

type Subscribers map[string]*Subscriber

type Broker struct {
	subscriber Subscribers
	topics     map[string]Subscribers
	lock       sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		subscriber: Subscribers{},
		topics:     map[string]Subscribers{},
	}
}

func (b *Broker) AddSubscriber() *Subscriber {
	b.lock.RLock()
	defer b.lock.RUnlock()
	sub := CreateNewSubscriber()
	b.subscriber[sub.id] = sub
	log.Printf("subscriber %s has joined", sub.id)
	return sub
}

func (b *Broker) RemoveSubscriber(id string) error {
	if b.subscriber[id] == nil {
		return fmt.Errorf("subscriber %s not subscribed", id)
	}
	b.subscriber[id].Close()
	log.Printf("subscriber %s has left", id)
	return nil
}

func (b *Broker) GetSubscriberById(id string) (*Subscriber, error) {
	if b.subscriber[id] == nil || b.subscriber[id].IsClosed() {
		return nil, errors.New("subscriber does not exists")
	}

	return b.subscriber[id], nil
}

func (b *Broker) GetSubscribers() ([]*Subscriber, error) {
	s := []*Subscriber{}
	for _, sub := range b.subscriber {
		if sub.IsClosed() {
			continue
		}
		s = append(s, sub)
	}

	return s, nil
}

func (b *Broker) Subscribe(id string, topic string) error {
	if b.subscriber[id] == nil {
		return fmt.Errorf("subscriber %s does not exist", id)
	}

	b.subscriber[id].AddTopic(topic)

	b.lock.RLock()
	defer b.lock.RUnlock()
	if b.topics[topic] == nil {
		b.topics[topic] = Subscribers{}
	}
	b.topics[topic][id] = b.subscriber[id]
	log.Printf("subscriber %s has subscribed to %s", id, topic)
	return nil
}

func (b *Broker) Unsubscribe(id string, topic string) error {
	if b.subscriber[id] == nil {
		return fmt.Errorf("subscriber %s does not exist", id)
	}

	b.subscriber[id].RemoveTopic(topic)

	b.lock.RLock()
	defer b.lock.RUnlock()
	if b.topics[topic] == nil {
		return errors.New("topic is not subscribed by subscriber")
	}
	delete(b.topics[topic], id)
	log.Printf("subscriber %s has unsubscribed to %s", id, topic)
	return nil
}

func (b *Broker) Publish(msg *bp.Message) error {
	if b.topics[msg.Topic] == nil {
		return errors.New("no subscriber for topic")
	}
	for _, s := range b.topics[msg.Topic] {
		go func(s *Subscriber) {
			if s.IsClosed() {
				return
			}
			s.Send(msg)
		}(s)
	}

	return nil
}

func (b *Broker) Listen(id string, str bp.BrokerService_ListenServer) error {
	sub, err := b.GetSubscriberById(id)
	if err != nil {
		return fmt.Errorf("subscriber %s does not exist\n %s", id, err)
	}

	for {
		msg, ok := <-sub.receiver
		if ok {
			str.Send(msg)
		}

		if msg.Closed {
			break
		}

		if !ok {
			fmt.Printf("error occured while receiving msg for sub %s\n", id)
			break
		}
	}

	return nil
}
