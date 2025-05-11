package main

import (
	"context"
	subpub "publish/pubsub"
	"sync"
	"testing"
	"time"
)

func TestSubscribePublish(t *testing.T) {
	sp := subpub.NewSubPub()
	defer sp.Close(context.Background())

	subject := "test.subject"
	msg := "test message"
	var receivedMsg string
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(m interface{}) {
		defer wg.Done()
		receivedMsg = m.(string)
	}

	_, err := sp.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	err = sp.Publish(subject, msg)
	if err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	wg.Wait()

	if receivedMsg != msg {
		t.Errorf("Expected message '%s', got '%s'", msg, receivedMsg)
	}
}

func TestClose(t *testing.T) {
	sp := subpub.NewSubPub()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := sp.Close(ctx)
	if err != nil {
		t.Fatalf("Close error: %v", err)
	}

	err = sp.Publish("test.subject", "test message")
	if err == nil {
		t.Errorf("Publish after close should return an error")
	}
	_, err = sp.Subscribe("test.subject", func(msg interface{}) {})
	if err == nil {
		t.Errorf("Subscribe after close should return an error")
	}
}

func TestMultipleSubscribers(t *testing.T) {
	sp := subpub.NewSubPub()
	defer sp.Close(context.Background())

	subject := "test.subject"
	msg := "test message"
	var count int
	var wg sync.WaitGroup

	numSubscribers := 3
	wg.Add(numSubscribers)

	handler := func(m interface{}) {
		defer wg.Done()
		count++
	}

	for i := 0; i < numSubscribers; i++ {
		_, err := sp.Subscribe(subject, handler)
		if err != nil {
			t.Fatalf("Subscribe error: %v", err)
		}
	}

	err := sp.Publish(subject, msg)
	if err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	wg.Wait()

	if count != numSubscribers {
		t.Errorf("Expected handler to be called %d times, but was called %d times", numSubscribers, count)
	}
}

func TestContextCancellation(t *testing.T) {
	sp := subpub.NewSubPub()
	subject := "test.subject"
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(m interface{}) {
		defer wg.Done()
		time.Sleep(5 * time.Second)
	}

	_, err := sp.Subscribe(subject, handler)
	if err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	err = sp.Publish(subject, "test message")
	if err != nil {
		t.Fatalf("Publish error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = sp.Close(ctx)
	if err == nil {
		t.Errorf("Expected context cancellation error, got nil")
	}

	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}
