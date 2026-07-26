package main

import (
	"context"
	"log"
	"time"

	"github.com/fbsobreira/gotron-sdk/pkg/client"
	tronblocklistener "github.com/unkmonster/tronutil/listener"
)

func main() {
	c := client.NewGrpcClientWithTimeout("grpc.trongrid.io:50051", 5*time.Second)
	err := c.Start(client.GRPCInsecure())
	if err != nil {
		log.Fatal(err)
	}

	listener := tronblocklistener.New(
		tronblocklistener.WithClient(c),
		tronblocklistener.WithPersister(tronblocklistener.NewFilePersister("height.txt")),
	)

	ctx := context.Background()
	if err := listener.Start(ctx); err != nil {
		log.Fatal(err)
	}

	time.Sleep(10000 * time.Second)

	log.Fatal(listener.Stop(ctx))
}
