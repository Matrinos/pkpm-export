package main

import (
	"fmt"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"golang.org/x/net/context"
)

func main() {
	// Create Liftbridge client.
	addrs := []string{"localhost:9292", "localhost:9293", "localhost:9294"}
	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create a stream attached to the NATS subject "foo".
	var (
        	subject = "channel.*"
        	name    = "mainflux-stream"
	)
	if err := client.CreateStream(context.Background(), subject, name); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}
	
	// // Publish a message to "foo".
	// if _, err := client.Publish(context.Background(), name, []byte("hello")); err != nil {
	// 	panic(err)
	// }

	// Subscribe to the stream starting from the beginning.
	ctx := context.Background()
	if err := client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Offset(), string(msg.Value()))
	}, lift.StartAtEarliestReceived()); err != nil {
		panic(err)
	}

	<-ctx.Done()
}
