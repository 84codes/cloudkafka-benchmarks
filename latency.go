package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Latency struct {
	c Client
}

func LatencyBench(c Client) Latency {
	return Latency{c: c}
}
func (l Latency) Config() string {
	return "Latency"
}
func (l Latency) Stat() *Stats {
	return &Stats{}

}
func (l Latency) Run() error {
	consumer := ConsumerBenchmark{
		Client: l.c,
		Batch:  *size,
		Offset: kafka.OffsetEnd,
	}
	producer := ProducerBenchmark{
		Client:      l.c,
		MessageSize: *size,
		Ack:         Acknowledge(*ack),
	}
	done := make(chan *Stats)
	go func() {
		err := consumer.Run()
		if err != nil {
			fmt.Println("[ERROR]", err)
			os.Exit(1)
		}
		done <- consumer.Stat()
	}()
	if err := producer.Run(); err != nil {
		return err
	}
	return nil
}
