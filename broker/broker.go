package broker

import (
	"errors"
	"fmt"
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
	return sub
}

func (b *Broker) RemoveSubscriber(id string) error {
	if b.subscriber[id] == nil {
		return fmt.Errorf("subscriber %s not subscribed", id)
	}
	b.subscriber[id].Close()
	return nil
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
	return nil
}

func (b *Broker) Publish(msg *bp.Message) error {
	if b.topics[msg.Topic] == nil {
		return errors.New("no subscriber for topic")
	}
	for _, s := range b.topics[msg.Topic] {
		go func(s *Subscriber) {
			if s.closed {
				return
			}
			s.Send(msg)
		}(s)
	}

	return nil
}

func (b *Broker) Listen(id string, str bp.BrokerService_ListenServer) error {
	if b.subscriber[id] == nil {
		return fmt.Errorf("subscriber %s does not exist", id)
	}

	for {
		msg, ok := <-b.subscriber[id].receiver
		if ok {
			str.Send(msg)
		}

		if !ok {
			fmt.Printf("error occured while receiving msg for sub %s\n", id)
			break
		}
	}

	return nil
}
