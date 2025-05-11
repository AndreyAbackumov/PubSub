package subpub

import (
	"context"
	"fmt"
	"sync"
)

// MessageHandler is a callback function that processes messages delivered to subscribers.
type MessageHandler func(msg interface{})

// Subscription interface
type Subscription interface {
	// Unsubscribe will remove interest in the current subject subscription is for.
	Unsubscribe()
}

// SubPub interface
type SubPub interface {
	// Subscribe creates an asynchronous queue subscriber on the given subject.
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	// Publish publishes the msg argument to the given subject.
	Publish(subject string, msg interface{}) error
	// Close will shutdown sub-pub system.
	// May be blocked by data delivery until the context is canceled.
	Close(ctx context.Context) error
}

type Bus struct {
	mu          sync.RWMutex
	subscribers map[string][]MessageHandler
	closed      bool
	closeCh     chan struct{}
	wg          sync.WaitGroup
}

type subscription struct {
	subject string
	handler MessageHandler
	sp      *Bus
}

func (s *subscription) Unsubscribe() {

	s.sp.mu.Lock()
	defer s.sp.mu.Unlock()

	if s.sp.closed {
		return
	}

	subscribers, err := s.sp.subscribers[s.subject]
	if !err {
		return
	}

	for i, h := range subscribers {
		if &h == &s.handler {
			subscribers = append(subscribers[:i], subscribers[i+1:]...)
			s.sp.subscribers[s.subject] = subscribers
			break
		}
	}

	if len(s.sp.subscribers[s.subject]) == 0 {
		delete(s.sp.subscribers, s.subject)
	}
}

func (sp *Bus) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if sp.closed {
		return nil, fmt.Errorf("subpub is closed")
	}

	_, err := sp.subscribers[subject]
	if !err {
		sp.subscribers[subject] = make([]MessageHandler, 0)
	}

	sp.subscribers[subject] = append(sp.subscribers[subject], cb)

	sub := &subscription{
		subject: subject,
		handler: cb,
		sp:      sp,
	}
	return sub, nil
}

func (sp *Bus) Publish(subject string, msg interface{}) error {

	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if sp.closed {
		return fmt.Errorf("subpub is closed")
	}

	handlers, ok := sp.subscribers[subject]
	if !ok {
		return nil
	}

	for _, handler := range handlers {
		sp.wg.Add(1)
		go func(h MessageHandler, m interface{}) {
			defer sp.wg.Done()
			h(m)
		}(handler, msg)
	}
	return nil
}

func (sp *Bus) Close(ctx context.Context) error {

	sp.mu.Lock()
	if sp.closed {
		sp.mu.Unlock()
		return fmt.Errorf("subpub is already closed")
	}
	sp.closed = true
	close(sp.closeCh)
	sp.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		sp.wg.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func NewSubPub() SubPub {
	return &Bus{
		subscribers: make(map[string][]MessageHandler),
		closeCh:     make(chan struct{}),
	}
}
