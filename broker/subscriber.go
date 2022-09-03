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
		receiver: make(chan *bp.Message),
		closed:   false,
	}
}

func (s *Subscriber) AddTopic(topic string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.topics[topic] = true
}

func (s *Subscriber) RemoveTopic(topic string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.topics, topic)
}

func (s *Subscriber) Send(msg *bp.Message) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.closed {
		s.receiver <- msg
	}
}

func (s *Subscriber) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closed = true
}
