package listener

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestStartAndStop(t *testing.T) {
	l := New(
		WithAddr("grpc.trongrid.io:50051"),
		WithTimeout(5*time.Second),
		WithDialOptions(
			grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		),
	)
	ctx := context.Background()

	err := l.Start(ctx)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	err = l.Stop(ctx)
	require.NoError(t, err)
}
