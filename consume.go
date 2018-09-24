package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConsumerBenchmark struct {
	Client
	Batch  int64
	Offset kafka.Offset
}

func (b ConsumerBenchmark) consume(consumer *kafka.Consumer) error {
	metadata, err := consumer.GetMetadata(&b.Topic, false, 10000)
	if err != nil {
		return fmt.Errorf("Could not read metadata from Kafka cluster: %s", err)
	}
	parts := metadata.Topics[b.Topic].Partitions
	toppar := make([]kafka.TopicPartition, len(parts))
	for i, p := range parts {
		toppar[i] = kafka.TopicPartition{Topic: &b.Topic, Partition: p.ID, Offset: b.Offset}
	}
	fmt.Fprintf(os.Stderr, "%% Consuming from %s\n", b.Topic)
	err = consumer.Assign(toppar)
	if err != nil {
		return fmt.Errorf("Could not assign partitions to consumer: %s", err)
	}
	return nil
}

func (b ConsumerBenchmark) Run() error {
	config := &kafka.ConfigMap{
		"metadata.broker.list":     b.Brokers,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          "SCRAM-SHA-256",
		"sasl.username":            b.Username,
		"sasl.password":            b.Password,
		"group.id":                 "kafka-bench",
		"enable.auto.commit":       false,
		"go.events.channel.enable": true,
	}
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return fmt.Errorf("Failed to create consumer: %s\n", err)
	}
	go b.handleDelivery(consumer.Events())
	if err := b.consume(consumer); err != nil {
		return nil
	}
	_ = <-b.doneChan
	consumer.Close()
	return nil
}

func (b ConsumerBenchmark) Config() string {
	return fmt.Sprintf(
		"Producer Config\n Messages:\t%d\n Batch:\t\t%d",
		b.NumMessages, b.Batch)
}
