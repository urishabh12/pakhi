package broker

import (
	"errors"
	"fmt"
	"log"
	"sync"

	bp "github.com/urishabh12/pakhi/proto"
)

type Subscribers map[string]*Subscriber

type Topic struct {
	name        string
	subscribers Subscribers
	lock        sync.Mutex
}

type Broker struct {
	subscriber Subscribers
	topics     map[string]*Topic
	lock       sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		subscriber: Subscribers{},
		topics:     map[string]*Topic{},
	}
}

func (b *Broker) AddSubscriber() *Subscriber {
	b.lock.Lock()
	defer b.lock.Unlock()
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

	b.lock.Lock()
	defer b.lock.Unlock()
	for _, sub := range b.subscriber {
		if sub.IsClosed() {
			continue
		}
		s = append(s, sub)
	}

	return s, nil
}

func (b *Broker) SetTopic(topic string) {
	b.topics[topic] = &Topic{
		name:        topic,
		subscribers: make(Subscribers),
	}
}

func (b *Broker) SetTopicIfNotExists(topic string) {
	if b.topics[topic] == nil {
		b.lock.Lock()
		if b.topics[topic] == nil {
			b.SetTopic(topic)
		}
		b.lock.Unlock()
	}
}

func (b *Broker) GetTopicsBySubscriberId(id string) ([]string, error) {
	sub, err := b.GetSubscriberById(id)
	if err != nil {
		return nil, err
	}

	topics := sub.GetTopics()
	return topics, nil
}

func (b *Broker) GetSubscribersByTopic(topic string) ([]*Subscriber, error) {
	if b.topics[topic] == nil {
		return nil, errors.New("topic not registered by a subscriber")
	}

	b.SetTopicIfNotExists(topic)

	subs := []*Subscriber{}
	b.topics[topic].lock.Lock()
	defer b.topics[topic].lock.Unlock()
	for _, sub := range b.topics[topic].subscribers {
		if sub.IsClosed() {
			continue
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

func (b *Broker) Subscribe(id string, topic string) error {
	if b.subscriber[id] == nil {
		return fmt.Errorf("subscriber %s does not exist", id)
	}
	b.subscriber[id].AddTopic(topic)

	b.SetTopicIfNotExists(topic)
	b.topics[topic].lock.Lock()
	defer b.topics[topic].lock.Unlock()
	b.topics[topic].subscribers[id] = b.subscriber[id]
	log.Printf("subscriber %s has subscribed to %s", id, topic)
	return nil
}

func (b *Broker) Unsubscribe(id string, topic string) error {
	if b.subscriber[id] == nil {
		return fmt.Errorf("subscriber %s does not exist", id)
	}

	b.subscriber[id].RemoveTopic(topic)

	b.SetTopicIfNotExists(topic)
	b.topics[topic].lock.Lock()
	defer b.topics[topic].lock.Unlock()
	delete(b.topics[topic].subscribers, id)
	log.Printf("subscriber %s has unsubscribed to %s", id, topic)
	return nil
}

func (b *Broker) Publish(msg *bp.Message) error {
	b.SetTopicIfNotExists(msg.Topic)

	//TODO: run it as a go routine
	b.topics[msg.Topic].lock.Lock()
	defer b.topics[msg.Topic].lock.Unlock()
	for _, s := range b.topics[msg.Topic].subscribers {
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
