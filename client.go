package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Message struct {
	Timestamp int64  `json:"ts"`
	Payload   string `json:"payload"`
}

type Stats struct {
	Start        int64
	Last         int64
	LatencyHigh  int64
	LatencyLow   int64
	LatencyTotal int64
	TotalBytes   int64
	Count        int64
}

func (s Stats) calc() map[string]float64 {
	total_s := float64(s.Last-s.Start) / 1000000000
	return map[string]float64{
		"total_time":         total_s,
		"records":            float64(s.Count),
		"latency_high":       float64(s.LatencyHigh) / 1000000000,
		"latency_low":        float64(s.LatencyLow) / 1000000000,
		"latency_avg":        float64(s.LatencyTotal / s.Count),
		"bytes_per_second":   float64(s.TotalBytes) / 1024 / total_s,
		"records_per_second": float64(s.Count) / total_s,
	}
}

func (s Stats) MarshalJSON() ([]byte, error) {
	if s.Count == 0 {
		return nil, errors.New("No stats data")
	}
	return json.Marshal(s.calc())
}

func (s Stats) String() string {
	if s.Count == 0 {
		return ""
	}
	data := s.calc()
	var keys []string
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buffer bytes.Buffer
	buffer.WriteString("Stats:\n")
	for _, k := range keys {
		buffer.WriteString(fmt.Sprintf(" %s = %.2f\n", k, data[k]))
	}
	return buffer.String()
}

func newStats() *Stats {
	return &Stats{
		Start:      time.Now().UnixNano(),
		TotalBytes: 0,
		Count:      0,
		LatencyLow: -1}
}

type MessageStats struct {
	Size    int
	Latency int64
}

type ConnectionDetails struct {
	Brokers  string
	Username string
	Password string
	Topic    string
}

type Client struct {
	ConnectionDetails
	NumMessages int64
	Stats       *Stats
	doneChan    chan bool
}

func NewClient(brokers, username, password, topic string, numMessages int64) Client {
	d := ConnectionDetails{
		Brokers:  brokers,
		Username: username,
		Password: password,
		Topic:    topic,
	}
	return Client{
		ConnectionDetails: d,
		NumMessages:       *messages,
		Stats:             newStats(),
		doneChan:          make(chan bool),
	}
}

func (c Client) updateStats(iter, bytes, latency int64) {
	if latency < c.Stats.LatencyLow || c.Stats.LatencyLow == -1 {
		c.Stats.LatencyLow = latency
	}
	if latency > c.Stats.LatencyHigh {
		c.Stats.LatencyHigh = latency
	}
	c.Stats.Count = iter
	c.Stats.TotalBytes += bytes
	c.Stats.LatencyTotal += latency
	c.Stats.Last = time.Now().UnixNano()
}

func (c Client) handleDelivery(eventCh chan kafka.Event) {
	var msgCounter int64 = 0
	defer close(c.doneChan)
	for e := range eventCh {
		switch ev := e.(type) {
		case kafka.PartitionEOF:
			fmt.Fprintf(os.Stderr, "%% Consumer: Reached EOF before consuming %d messages\n", c.NumMessages)
		case *kafka.Stats:
			// fmt.Fprintf(os.Stderr, "%% Kafka stats: %s\n", ev)
		case *kafka.Message:
			msgCounter += 1
			m := ev
			if m.TopicPartition.Error != nil {
				fmt.Fprintf(os.Stderr, "%% Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				var dat Message
				if err := json.Unmarshal(m.Value, &dat); err != nil {
					fmt.Fprintf(os.Stderr, "%% Parsing message failed: %s\n", err)
				}
				c.updateStats(msgCounter, int64(len(m.Value)), time.Now().UnixNano()-dat.Timestamp)
			}
			if msgCounter >= c.NumMessages {
				return
			}
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "[ERROR] Kafka error: %s\n", ev)
		default:
			fmt.Fprintf(os.Stderr, "%% Kafka event: %s\n", ev)
		}
	}
}

func (c Client) Stat() *Stats {
	return c.Stats
}

type Benchmark interface {
	Run() error
	Config() string
	Stat() *Stats
}
