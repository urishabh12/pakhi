package broker

import (
	"sync"

	"github.com/google/uuid"
	bp "github.com/urishabh12/pakhi/proto"
)

type Subscriber struct {
	id       string
	topics   map[string]bool
	receiver chan *bp.Message
	lock     sync.Mutex
	closed   bool
}

func CreateNewSubscriber() *Subscriber {
	return &Subscriber{
		id:       uuid.New().String(),
		topics:   make(map[string]bool),
		receiver: make(chan *bp.Message, 10),
		closed:   false,
	}
}

func (s *Subscriber) AddTopic(topic string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.topics[topic] = true

	return nil
}

func (s *Subscriber) RemoveTopic(topic string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.topics, topic)

	return nil
}

func (s *Subscriber) GetTopics() []string {
	t := []string{}
	for topic := range s.topics {
		t = append(t, topic)
	}

	return t
}

func (s *Subscriber) Send(msg *bp.Message) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.closed {
		s.receiver <- msg
	}

	return nil
}

func (s *Subscriber) Close() error {
	in := &bp.Message{
		Closed: true,
	}
	s.Send(in)
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closed = true

	return nil
}
