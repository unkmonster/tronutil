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
)

const (
	maxBatchGetBlockSize = 100
	blockProducedSpeed   = 3 * time.Second
)

type HandleBlockFunc func(ctx context.Context, b *api.BlockExtention)

type Option func(l *Listener)

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

func WithTimeout(t time.Duration) Option {
	return func(l *Listener) {
		l.timeout = t
	}
}

type Listener struct {
	logger log.Logger
	log    *log.Helper

	client *client.GrpcClient

	blockHandlers []HandleBlockFunc

	done   chan struct{}
	cancel context.CancelFunc

	persister Persister

	// 期待的下一个需要处理的区块的高度
	nextHeight int64
	// 最新区块高度
	nowHeight int64

	timeout time.Duration
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
		panic("client required")
	}

	return l
}

// Start 启动监听器并返回
func (l *Listener) Start(ctx context.Context) error {
	// set context
	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel

	// prepare worker params
	height, local, err := l.getInitialHeight(ctx)
	if err != nil {
		return fmt.Errorf("retrieve initial height: %w", err)
	}
	l.nextHeight = height

	l.log.WithContext(ctx).Infof("[block_listener] started at %d, local: %v", height, local)
	go l.worker(ctx, &workerParams{})
	return nil
}

// Stop waits the listener to be stopped and returns
func (l *Listener) Stop(ctx context.Context) error {
	l.log.WithContext(ctx).Infof("[block_listener] stopping")

	// cancel worker context
	l.cancel()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.done:
		// waiting for worker exited
	}

	return nil
}

// getInitialHeight 从本地加载或获取最新区块高度并返回
func (l *Listener) getInitialHeight(ctx context.Context) (height int64, local bool, err error) {
	local = true
	if l.persister != nil {
		height, err = l.persister.LoadNextHeight(ctx)
		if err != nil {
			return 0, false, fmt.Errorf("load persisted height: %w", err)
		}
	}

	if height == 0 {
		local = false
		height, err = l.getNowHeight(ctx)
		if err != nil {
			return 0, false, fmt.Errorf("fetch now block height: %w", err)
		}
	}

	return
}

// getNowHeight fetches and returns current latest block height
func (l *Listener) getNowHeight(ctx context.Context) (int64, error) {
	b, err := l.client.GetNowBlockCtx(ctx)
	if err != nil {
		return 0, err
	}
	return b.GetBlockHeader().GetRawData().GetNumber(), nil
}

func (l *Listener) handleBlock(ctx context.Context, b *api.BlockExtention) {
	if l.timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.timeout)
		defer cancel()
	}

	for _, h := range l.blockHandlers {
		h(ctx, b)
	}
	l.log.WithContext(ctx).Debugw(
		"event", "block handling completed",
		"block.height", b.BlockHeader.RawData.Number,
		"block.id", hex.EncodeToString(b.GetBlockid()),
		"block.transactions", len(b.Transactions),
	)
}

func (l *Listener) saveNextHeight(ctx context.Context) {
	if l.persister != nil {
		err := l.persister.SaveNextHeight(ctx, l.nextHeight)
		if err != nil {
			l.log.WithContext(ctx).Errorw(
				"msg", "failed to save next height",
				"reason", err,
				"next_height", l.nextHeight,
			)
		}
	}
}

// processBlocks processes given blocks, returns expected next block height
// input blocks must be non-empty
func (l *Listener) processBlocks(ctx context.Context, blocks ...*api.BlockExtention) {
	// make sure the blocks is in increasing order
	sortBlocks(blocks)

	for _, block := range blocks {
		curHeight := blockHeight(block)
		if curHeight != l.nextHeight {
			l.log.WithContext(ctx).Warnw(
				"msg", "current height is mismatch with expected next height",
				"current", curHeight,
				"next", l.nextHeight,
			)
			break
		}

		// FIXME: 如果区块处理完成后，机器断电，saveHeight 失败，
		// 将导致这个区块在重启后被重复处理
		l.handleBlock(ctx, block)
		l.nextHeight++
		l.saveNextHeight(ctx)
	}

}

type workerParams struct {
}

func (l *Listener) worker(ctx context.Context, params *workerParams) {
	defer close(l.done)

	var (
		tick = time.Tick(blockProducedSpeed)
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			l.tick(ctx)
		}
	}
}

func (l *Listener) AddBlockHandler(h HandleBlockFunc) {
	l.blockHandlers = append(l.blockHandlers, h)
}

func (l *Listener) ResetBlockHandlers(handlers []HandleBlockFunc) {
	l.blockHandlers = handlers
}

func (l *Listener) NextHeight() int64 {
	return l.nextHeight
}

func (l *Listener) NowHeight() int64 {
	return l.nowHeight
}

func (l *Listener) tick(ctx context.Context) {
	var (
		nowHeight int64
		nowBlock  *api.BlockExtention
		err       error
	)

	// 获取最新区块高度
	nowBlock, err = l.client.GetNowBlockCtx(ctx)
	if err != nil {
		l.log.WithContext(ctx).Warnw(
			"msg", "failed to fetch now block",
			"reason", err,
		)
		return
	}
	nowHeight = blockHeight(nowBlock)
	if nowHeight <= 0 {
		l.log.WithContext(ctx).Warnw(
			"msg", "now block with invalid height",
			"raw", fmt.Sprintf("%+v", nowBlock),
		)
		return
	}
	l.nowHeight = nowHeight

	if nowHeight == l.nextHeight {
		// 如果当前区块高度是期待的区块高度，处理这个区块,
		// 否则开始恢复模式
		l.processBlocks(ctx, nowBlock)
		return
	}

	l.log.WithContext(ctx).Warnw(
		"event", "scanned block behind of now block",
		"now", nowHeight,
		"next_height", l.nextHeight,
	)

	// 当前区块高度与期待的区块高度不匹配 (落后于)，
	// 批量获取并处理 [期待高度, 最新区块高度) 内的所有区块
	var (
		begin = l.nextHeight
		end   = nowHeight
	)

	for begin < end {
		left := begin
		right := min(end, left+maxBatchGetBlockSize)
		blockList, err := l.client.GetBlockByLimitNextCtx(ctx, left, right)
		if err != nil || len(blockList.Block) == 0 {
			l.log.WithContext(ctx).Warnw(
				"msg", "failed to fetch block list",
				"reason", err,
				"next", l.nextHeight,
				"now", nowHeight,
				"begin", begin,
				"batch_size", maxBatchGetBlockSize,
			)
			break
		}

		blocks := blockList.GetBlock()
		sortBlocks(blocks)

		if blockHeight(blocks[len(blocks)-1])+1 == blockHeight(nowBlock) {
			// 性能优化: 如果当前区块高度为 blocks 中最新区块的高度 + 1,
			// 将当前区块添加到待处理的 blocks
			blocks = append(blocks, nowBlock)
		}

		if blockHeight(blocks[0]) != l.nextHeight {
			l.log.WithContext(ctx).Warnw(
				"msg", "fist block height of GetBlockByLimitNextCtx result is mismatch with next_height",
				"block_list.first", blockHeight(blocks[0]),
				"block_list.last", blockHeight(blocks[len(blocks)-1]),
				"next", l.nextHeight,
				"now", nowHeight,
				"left", left,
				"right", right,
			)
			break
		}
		l.processBlocks(ctx, blocks...)
		begin = right
	}
}

func buildHelper(logger log.Logger) *log.Helper {
	return log.NewHelper(log.With(logger,
		"component", "block_listener",
	))
}

// blockHeight returns height of input block
// if unable to extract height returns 0
func blockHeight(b *api.BlockExtention) int64 {
	if b == nil || b.BlockHeader == nil || b.BlockHeader.RawData == nil {
		return 0
	}
	return b.BlockHeader.RawData.Number
}

// sortBlocks 将输入的 blocks 按照区块高度升序排序
func sortBlocks(blocks []*api.BlockExtention) {
	slices.SortFunc(blocks, func(a, b *api.BlockExtention) int {
		return int(blockHeight(a) - blockHeight(b))
	})
}
