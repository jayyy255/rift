// Command quickstart publishes a few records and consumes them with a
// consumer group. Run a broker first (make run), then: go run ./examples/quickstart
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jayyy255/rift/pkg/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := client.Dial("127.0.0.1:7070")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	_, err = c.CreateTopic(ctx, "orders", client.TopicConfig{Partitions: 6})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatal(err)
	}

	// The producer batches records; Send returns once the broker has
	// fsynced the record (the default durability).
	p := c.NewProducer("orders", client.ProducerConfig{Durability: client.DurabilityFsynced})
	for i := range 3 {
		md, err := p.Send(ctx, client.Record{Key: []byte("customer-17"), Value: []byte(fmt.Sprintf(`{"order_id":%d}`, 100+i))})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("published partition=%d offset=%d\n", md.Partition, md.Offset)
	}
	p.Close()

	// The consumer resumes from the group's committed offsets and commits
	// its progress on Close.
	cons, err := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "orders", Group: "analytics", AutoCommit: true})
	if err != nil {
		log.Fatal(err)
	}
	defer cons.Close(ctx)
	msgs, err := cons.Poll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range msgs {
		fmt.Printf("consumed partition=%d offset=%d key=%s value=%s\n", m.Partition, m.Offset, m.Key, m.Value)
	}
}
