package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "publish/proto"

	"publish/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	cfg, err := config.LoadConfig("../")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	address := fmt.Sprintf("localhost:%s", cfg.GRPC.Port)
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Не удалось подключиться: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubClient(conn)

	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{Key: "my_key"})
	if err != nil {
		log.Fatalf("Не удалось подписаться: %v", err)
	}

	go func() {
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				log.Fatalf("Ошибка получения события: %v", err)
			}
			fmt.Printf("Получено событие: %s\n", event.Data)
		}
	}()

	_, err = client.Publish(context.Background(), &pb.PublishRequest{Key: "my_key", Data: "Hello, PubSub!"})
	if err != nil {
		log.Fatalf("Не удалось опубликовать событие: %v", err)
	}

	_, err = client.Publish(context.Background(), &pb.PublishRequest{Key: "my_key", Data: "Another event!"})
	if err != nil {
		log.Fatalf("Не удалось опубликовать событие: %v", err)
	}

	time.Sleep(5 * time.Second)
}
