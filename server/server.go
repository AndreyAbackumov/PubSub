package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"publish/config"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "publish/proto"
	subpub "publish/pubsub"
)

type pubSubServer struct {
	pb.UnimplementedPubSubServer
	pubSub  subpub.SubPub
	mu      sync.Mutex
	streams map[string]map[pb.PubSub_SubscribeServer]struct{}
}

func NewPubSubServer() *pubSubServer {
	return &pubSubServer{
		pubSub:  subpub.NewSubPub(),
		streams: make(map[string]map[pb.PubSub_SubscribeServer]struct{}),
	}
}

func (s *pubSubServer) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	key := req.GetKey()

	s.mu.Lock()
	if _, ok := s.streams[key]; !ok {
		s.streams[key] = make(map[pb.PubSub_SubscribeServer]struct{})
	}
	s.streams[key][stream] = struct{}{}
	s.mu.Unlock()

	handler := func(msg interface{}) {
		event, ok := msg.(*pb.Event)
		if !ok {
			return
		}

		if err := stream.Send(event); err != nil {
			log.Printf("Failed to send event to subscriber: %v", err)
			s.mu.Lock()
			delete(s.streams[key], stream)
			s.mu.Unlock()
		}
	}

	sub, err := s.pubSub.Subscribe(key, handler)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	<-stream.Context().Done()

	s.mu.Lock()
	delete(s.streams[key], stream)
	if len(s.streams[key]) == 0 {
		delete(s.streams, key)
	}
	s.mu.Unlock()

	return stream.Context().Err()
}

func (s *pubSubServer) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	event := &pb.Event{
		Data: req.GetData(),
	}

	if err := s.pubSub.Publish(req.GetKey(), event); err != nil {
		return nil, fmt.Errorf("failed to publish event: %v", err)
	}

	return &emptypb.Empty{}, nil
}

func main() {
	cfg, err := config.LoadConfig("../")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPC.Port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterPubSubServer(server, NewPubSubServer())

	log.Printf("Server listening at %v", lis.Addr())
	if err := server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
