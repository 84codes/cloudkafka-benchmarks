package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Latency struct {
	ConnDetails ConnectionDetails
}

func LatencyBench(d ConnectionDetails) Latency {
	return Latency{ConnDetails: d}
}
func (l Latency) Config() string {
	return "Latency"
}
func (l Latency) Run() (Stats, error) {
	consumer := ConsumerBenchmark{
		ConnectionDetails: l.ConnDetails,
		NumMessages:       *messages,
		Batch:             *size,
		Offset:            kafka.OffsetEnd,
	}
	producer := ProducerBenchmark{
		ConnectionDetails: l.ConnDetails,
		NumMessages:       *messages,
		MessageSize:       *size,
		Ack:               Acknowledge(*ack),
	}
	done := make(chan Stats)
	go func() {
		s, err := consumer.Run()
		if err != nil {
			fmt.Println("[ERROR]", err)
			os.Exit(1)
		}
		done <- s
	}()
	if _, err := producer.Run(); err != nil {
		return Stats{}, err
	}
	return <-done, nil
}
