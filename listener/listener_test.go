package listener

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/fbsobreira/gotron-sdk/pkg/client"
	"github.com/fbsobreira/gotron-sdk/pkg/proto/api"
	"github.com/fbsobreira/gotron-sdk/pkg/proto/core"
	"github.com/stretchr/testify/require"
)

func TestStartAndStop(t *testing.T) {
	c := client.NewGrpcClientWithTimeout("grpc.trongrid.io:50051", 5*time.Second)
	err := c.Start(client.GRPCInsecure())
	require.NoError(t, err)
	defer c.Stop()

	l := New(
		WithClient(c),
	)
	ctx := context.Background()

	err = l.Start(ctx)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	err = l.Stop(ctx)
	require.NoError(t, err)
}

func TestSortBlocks(t *testing.T) {
	blocks := make([]*api.BlockExtention, 0, 100)

	for range 100 {
		blocks = append(blocks, &api.BlockExtention{
			BlockHeader: &core.BlockHeader{
				RawData: &core.BlockHeaderRaw{
					Number: rand.Int64(),
				},
			},
		})
	}

	sortBlocks(blocks)
	prevHeight := int64(-1)
	for _, b := range blocks {
		h := blockHeight(b)
		require.GreaterOrEqual(t, h, int64(0))
		require.GreaterOrEqual(t, h, prevHeight)
		prevHeight = h
	}
}
