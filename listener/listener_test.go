package listener

import (
	"context"
	"testing"
	"time"

	"github.com/fbsobreira/gotron-sdk/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestStartAndStop(t *testing.T) {
	l := New(
		WithAddr("grpc.trongrid.io:50051"),
		WithTimeout(5*time.Second),
		WithDialOptions(
			client.GRPCInsecure(),
		),
	)
	ctx := context.Background()

	err := l.Start(ctx)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	err = l.Stop(ctx)
	require.NoError(t, err)
}
