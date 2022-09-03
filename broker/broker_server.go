package broker

import (
	"context"
	"fmt"
	"log"
	"net"

	empty "github.com/golang/protobuf/ptypes/empty"
	bp "github.com/urishabh12/pakhi/proto"
	"google.golang.org/grpc"
)

type BrokerServer struct {
	port   int
	broker *Broker
}

func NewBrokerServer(port int) (*BrokerServer, error) {
	b := &BrokerServer{
		port:   port,
		broker: NewBroker(),
	}
	return b, nil
}

func (b *BrokerServer) AddSubscriber(ctx context.Context, in *empty.Empty) (*bp.Subscriber, error) {
	sub := b.broker.AddSubscriber()
	out := &bp.Subscriber{
		Id: sub.id,
	}
	return out, nil
}

func (b *BrokerServer) RemoveSubscriber(ctx context.Context, in *bp.Subscriber) (*bp.Status, error) {
	err := b.broker.RemoveSubscriber(in.Id)
	out := &bp.Status{
		Status: err == nil,
	}
	return out, err
}

func (b *BrokerServer) Subscribe(ctx context.Context, in *bp.SubscribeRequest) (*bp.Status, error) {
	err := b.broker.Subscribe(in.Id, in.Topic)
	out := &bp.Status{
		Status: err == nil,
	}
	return out, err
}

func (b *BrokerServer) Unsubscribe(ctx context.Context, in *bp.UnsubscribeRequest) (*bp.Status, error) {
	err := b.broker.Unsubscribe(in.Id, in.Topic)
	out := &bp.Status{
		Status: err == nil,
	}
	return out, err
}

func (b *BrokerServer) Publish(ctx context.Context, in *bp.Message) (*bp.Status, error) {
	err := b.broker.Publish(in)
	out := &bp.Status{
		Status: err == nil,
	}
	return out, err
}

func (b *BrokerServer) Listen(in *bp.Subscriber, str bp.BrokerService_ListenServer) error {
	err := b.broker.Listen(in.Id, str)
	return err
}

func (b *BrokerServer) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", b.port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	bp.RegisterBrokerServiceServer(grpcServer, b)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
	return nil
}
