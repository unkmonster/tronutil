package tronblocklistener

import (
	"context"
	"fmt"
	"time"

	"github.com/fbsobreira/gotron-sdk/pkg/client"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/grpc"
)

type Option func(l *Listener)

func WithAddr(addr string) Option {
	return func(l *Listener) {
		l.address = addr
	}
}

func WithTimeout(t time.Duration) Option {
	return func(l *Listener) {
		l.timeout = t
	}
}

func WithDialOptions(options ...grpc.DialOption) Option {
	return func(l *Listener) {
		l.dialOptions = options
	}
}

func WithLogger(logger log.Logger) Option {
	return func(l *Listener) {
		l.logger = logger
	}
}

type Listener struct {
	logger log.Logger
	log    *log.Helper

	address     string
	timeout     time.Duration
	dialOptions []grpc.DialOption
	client      *client.GrpcClient

	done   chan struct{}
	cancel context.CancelFunc
}

func New(opts ...Option) *Listener {
	l := &Listener{
		logger: log.DefaultLogger,
		done:   make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
	}

	l.log = buildHelper(l.logger)
	l.client = newClient(l)

	return l
}

func newClient(l *Listener) *client.GrpcClient {
	client := client.NewGrpcClientWithTimeout(l.address, l.timeout)
	return client
}

// Start starts the listener
func (l *Listener) Start(ctx context.Context) error {
	l.log.WithContext(ctx).Infof("[block_listener] starting")

	err := l.client.Start(l.dialOptions...)
	if err != nil {
		return fmt.Errorf("start tron client: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	go l.worker(ctx)
	return nil
}

// Stop waits the listener to be stopped and returns
func (l *Listener) Stop(ctx context.Context) error {
	l.log.WithContext(ctx).Infof("[block_listener] stopping")

	// stop worker
	l.cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
	}

	l.client.Stop()
	return nil
}

func (l *Listener) worker(ctx context.Context) {
	defer close(l.done)

	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func buildHelper(logger log.Logger) *log.Helper {
	return log.NewHelper(log.With(logger,
		"component", "block_listener",
	))
}
