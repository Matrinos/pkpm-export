package main

import (
	"fmt"
	"time"

	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"golang.org/x/net/context"
)

const (
	subject  = "channels/17668d20-510f-4818-a179-cf9949ec651a/messages"
	name     = "mainflux-stream"
	cursorID = "mycursor-5"
)

func main() {
	// Create Liftbridge client.
	addrs := []string{"localhost:9292", "localhost:9293", "localhost:9294"}
	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	defer client.Close()

	// Create a stream attached to the NATS subject
	if err := client.CreateStream(ctx, subject, name); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}

	// Publish a message to "foo". (Optional)
	// if _, err := client.Publish(context.Background(), name, []byte("hello")); err != nil {
	// 	panic(err)
	// }

	// Reset cursor (Optinal)
	// if err := client.SetCursor(ctx, cursorID, name, 0, 0); err != nil {
	// 	fmt.Printf("Reset cursor error %v", err)
	// 	panic(err)
	// }

	startSubsciption(ctx, client)

}

func startSubsciption(ctx context.Context, client lift.Client) {
	// Retrieve offset to resume processing from.
	offset, err := client.FetchCursor(ctx, cursorID, name, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Start offset is %d\n", offset)

	if err := client.Subscribe(ctx, name, func(msg *lift.Message, err error) {
		if err != nil {
			fmt.Printf("Message error %v", err)
			panic(err)
		}
		fmt.Printf("offset: %d ", offset)
		// percent := rand.Intn(100); percent > 90
		if (msg.Offset()+1)%2 == 0 {
			fmt.Println("Partition:", msg.Partition(), "Index:", msg.Offset(), "Failed!")
			// Reset offset to failed one
			if err := client.SetCursor(ctx, cursorID, name, msg.Partition(), msg.Offset()-1); err != nil {
				fmt.Printf("SetCursor error %v", err)
			}
			time.Sleep(3 * time.Second)
			startSubsciption(ctx, client)

			<-ctx.Done()
		} else {
			// fmt.Println(msg.Timestamp(), msg.Offset(), string(msg.Key()), string(msg.Value()))
			fmt.Println("Partition:", msg.Partition(), "Index:", msg.Offset(), "Data Received...")
			// Checkpoint current position.
			if err := client.SetCursor(ctx, cursorID, name, msg.Partition(), msg.Offset()); err != nil {
				fmt.Printf("SetCursor error %v", err)
				panic(err)
			}
		}

	}, lift.StartAtOffset(offset+1)); err != nil {
		fmt.Printf("Subscribe error %v", err)
		panic(err)
	}
	fmt.Println("Subscription started...")
	<-ctx.Done()
}
