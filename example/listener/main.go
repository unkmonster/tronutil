package main

import (
	"context"
	"log"
	"time"

	"github.com/fbsobreira/gotron-sdk/pkg/client"
	tronblocklistener "github.com/unkmonster/tronutil/listener"
)

func main() {
	listener := tronblocklistener.New(
		tronblocklistener.WithAddr("grpc.shasta.trongrid.io:50051"),
		tronblocklistener.WithTimeout(5*time.Second),
		tronblocklistener.WithDialOptions(
			client.GRPCInsecure(),
		),
		tronblocklistener.WithPersister(tronblocklistener.NewFilePersister("height.txt")),
	)

	ctx := context.Background()
	if err := listener.Start(ctx); err != nil {
		log.Fatal(err)
	}

	time.Sleep(10000 * time.Second)

	log.Fatal(listener.Stop(ctx))
}
