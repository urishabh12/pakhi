package subscriber

import (
	"context"

	empty "github.com/golang/protobuf/ptypes/empty"
	bp "github.com/urishabh12/pakhi/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ListenCallback func(*bp.Message)

type Subscriber struct {
	client bp.BrokerServiceClient
	ctx    context.Context
	id     string
}

func NewSubscriber(target string) (*Subscriber, error) {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	c := bp.NewBrokerServiceClient(conn)

	sub, err := c.AddSubscriber(context.Background(), &empty.Empty{})
	if err != nil {
		return nil, err
	}

	return &Subscriber{
		client: c,
		ctx:    context.Background(),
		id:     sub.Id,
	}, nil
}

func (s *Subscriber) Subscribe(topic string) error {
	in := &bp.SubscribeRequest{
		Id:    s.id,
		Topic: topic,
	}
	_, err := s.client.Subscribe(s.ctx, in)
	return err
}

func (s *Subscriber) Unsubscribe(topic string) error {
	in := &bp.UnsubscribeRequest{
		Id:    s.id,
		Topic: topic,
	}
	_, err := s.client.Unsubscribe(s.ctx, in)
	return err
}

func (s *Subscriber) Close() error {
	in := &bp.Subscriber{
		Id: s.id,
	}
	_, err := s.client.RemoveSubscriber(s.ctx, in)
	return err
}

func (s *Subscriber) Listen(fn ListenCallback) error {
	in := &bp.Subscriber{
		Id: s.id,
	}
	stream, err := s.client.Listen(s.ctx, in)
	if err != nil {
		return err
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		go func(*bp.Message) {
			fn(resp)
		}(resp)
	}
}
