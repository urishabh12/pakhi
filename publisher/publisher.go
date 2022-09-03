package publisher

import (
	"context"

	bp "github.com/urishabh12/pakhi/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Publisher struct {
	client bp.BrokerServiceClient
	ctx    context.Context
}

func NewPublisher(target string) (*Publisher, error) {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	c := bp.NewBrokerServiceClient(conn)

	return &Publisher{
		client: c,
		ctx:    context.Background(),
	}, nil
}

func (p *Publisher) Publish(topic string, msg string) error {
	in := &bp.Message{
		Topic:   topic,
		Message: msg,
	}
	_, err := p.client.Publish(p.ctx, in)
	return err
}
