package listener

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"time"

	"github.com/fbsobreira/gotron-sdk/pkg/client"
	"github.com/fbsobreira/gotron-sdk/pkg/proto/api"
	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/grpc"
)

var (
	blockProducedSpeed = 3 * time.Second
)

type HandleBlockFunc func(ctx context.Context, b *api.BlockExtention)

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

func WithPersister(p Persister) Option {
	return func(l *Listener) {
		l.persister = p
	}
}

func WithClient(client *client.GrpcClient) Option {
	return func(l *Listener) {
		l.client = client
	}
}

type Listener struct {
	logger log.Logger
	log    *log.Helper

	address     string
	timeout     time.Duration
	dialOptions []grpc.DialOption
	client      *client.GrpcClient

	blockHandlers []HandleBlockFunc

	done   chan struct{}
	cancel context.CancelFunc

	persister Persister
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
	if l.client == nil {
		l.client = newClient(l)
	}

	return l
}

func newClient(l *Listener) *client.GrpcClient {
	client := client.NewGrpcClientWithTimeout(l.address, l.timeout)
	return client
}

// Start starts the listener
func (l *Listener) Start(ctx context.Context) error {
	// start tron client
	err := l.client.Start(l.dialOptions...)
	if err != nil {
		return fmt.Errorf("start tron client: %w", err)
	}

	// set context
	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel

	// prepare worker params
	height, err := l.getInitialHeight(ctx)
	if err != nil {
		return fmt.Errorf("retrieve initial height: %w", err)
	}

	l.log.WithContext(ctx).Infof("[block_listener] starting at %d", height)
	go l.worker(ctx, &workerParams{
		height: height,
	})
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

func (l *Listener) getInitialHeight(ctx context.Context) (height int64, err error) {
	if l.persister != nil {
		height, err = l.persister.LoadNextHeight(ctx)
		if err != nil {
			return 0, fmt.Errorf("load persisted height: %w", err)
		}
	}

	if height == 0 {
		height, err = l.getNowHeight(ctx)
		if err != nil {
			return 0, fmt.Errorf("fetch now block height: %w", err)
		}
	}

	return
}

func (l *Listener) getNowHeight(ctx context.Context) (int64, error) {
	b, err := l.client.GetNowBlockCtx(ctx)
	if err != nil {
		return 0, err
	}
	return b.GetBlockHeader().GetRawData().GetNumber(), nil
}

func (l *Listener) handleBlock(ctx context.Context, b *api.BlockExtention) {
	for _, h := range l.blockHandlers {
		h(ctx, b)
	}
	l.log.WithContext(ctx).Infow(
		"event", "block handling completed",
		"block.height", b.BlockHeader.RawData.Number,
		"block.id", hex.EncodeToString(b.GetBlockid()),
	)
}

// processBlocks processes given blocks, returns expected next block height
func (l *Listener) processBlocks(ctx context.Context, blocks ...*api.BlockExtention) (next int64) {
	if len(blocks) == 0 {
		panic("empty blocks")
	}

	// make sure the blocks is in increasing order
	slices.SortFunc(blocks, func(a *api.BlockExtention, b *api.BlockExtention) int {
		return int(a.BlockHeader.RawData.Number - b.BlockHeader.RawData.Number)
	})
	// calculate the next height
	next = blocks[len(blocks)-1].BlockHeader.RawData.Number + 1

	for _, block := range blocks {
		l.handleBlock(ctx, block)
	}
	return
}

type workerParams struct {
	height int64
}

func (l *Listener) worker(ctx context.Context, params *workerParams) {
	defer close(l.done)

	var (
		tick = time.Tick(blockProducedSpeed)
		// nextHeight holds nextHeight of next block to process
		nextHeight = params.height
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			var (
				// now height
				nowHeight int64
			)

			// 获取最新区块
			block, err := l.client.GetNowBlockCtx(ctx)
			if err != nil {
				l.log.WithContext(ctx).Warnw(
					"msg", "failed to fetch now block",
					"reason", err,
				)
				continue
			}

			nowHeight = block.GetBlockHeader().GetRawData().GetNumber()
			if nowHeight == nextHeight {
				nextHeight = l.processBlocks(ctx, block)
			} else {
				// 当前区块高度不等于期待的区块高度，
				// 批量获取并处理 [期待高度, 最新区块高度) 内的所有区块
				blockList, err := l.client.GetBlockByLimitNextCtx(ctx, nextHeight, nowHeight)
				if err != nil {
					l.log.WithContext(ctx).Warnw(
						"msg", "failed to fetch block list",
						"reason", err,
						"next", nextHeight,
						"now", nowHeight,
					)
					continue
				}
				nextHeight = l.processBlocks(ctx, append(blockList.GetBlock(), block)...)
			}

			if l.persister != nil {
				err := l.persister.SaveNextHeight(ctx, nextHeight)
				if err != nil {
					l.log.WithContext(ctx).Errorw(
						"msg", "failed to save next height",
						"reason", err,
						"next_height", nextHeight,
					)
				}
			}
		}
	}
}

func (l *Listener) AddBlockHandler(h HandleBlockFunc) {
	l.blockHandlers = append(l.blockHandlers, h)
}

func (l *Listener) ResetBlockHandlers(handlers []HandleBlockFunc) {
	l.blockHandlers = handlers
}

func buildHelper(logger log.Logger) *log.Helper {
	return log.NewHelper(log.With(logger,
		"component", "block_listener",
	))
}
