package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Acknowledge int

const AckLeader = Acknowledge(1)
const AckAll = Acknowledge(-1)
const AckNone = Acknowledge(0)

type ProducerBenchmark struct {
	Client
	MessageSize int64
	Ack         Acknowledge
}

func (b ProducerBenchmark) generateMessage() *kafka.Message {
	// 39 is the size of the surrounding json struct including timestamp
	// Subsctracting this ensure the message is exactly b.MessageSize
	var extra int64 = 39
	bytes := make([]byte, b.MessageSize-extra)
	for i := range bytes {
		bytes[i] = '!'
	}
	message := Message{
		Timestamp: time.Now().UnixNano(),
		Payload:   string(bytes),
	}
	value, _ := json.Marshal(message)
	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &b.Topic,
			Partition: kafka.PartitionAny},
		Value: value,
	}
}

func (b ProducerBenchmark) Config() string {
	return fmt.Sprintf("Producer Config: messages=%d size=%d ack=%d",
		b.NumMessages, b.MessageSize, int(b.Ack))
}

func (b ProducerBenchmark) Run() error {
	config := &kafka.ConfigMap{
		"metadata.broker.list":         b.Brokers,
		"security.protocol":            "SASL_SSL",
		"sasl.mechanisms":              "SCRAM-SHA-256",
		"sasl.username":                b.Username,
		"sasl.password":                b.Password,
		"group.id":                     "kafka-bench",
		"acks":                         int(b.Ack),
		"go.batch.producer":            true,
		"go.delivery.reports":          true,
		"queue.buffering.max.messages": int(b.NumMessages),
		"compression.type":             "lz4",
		"queue.buffering.max.ms":       200,
		// "statistics.interval.ms":       3000,
	}
	p, err := kafka.NewProducer(config)
	if err != nil {
		return fmt.Errorf("Failed to create producer: %s\n", err)
	}
	go b.handleDelivery(p.Events())
	var i int64
	for i = 0; i < b.NumMessages; i++ {
		p.ProduceChannel() <- b.generateMessage()
	}
	fmt.Fprintf(os.Stderr, "%% Flushing %d message(s)\n", p.Len())
	for p.Flush(10000) > 0 {
		fmt.Fprintf(os.Stderr, "%% Flushing queue...\n")
	}
	_ = <-b.doneChan
	p.Close()
	return nil
}
