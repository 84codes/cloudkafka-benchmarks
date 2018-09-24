package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	confFile  = flag.String("f", "", "config")
	brokers   = flag.String("b", "", "brokerlist")
	username  = flag.String("u", "", "username")
	password  = flag.String("p", "", "password")
	topic     = flag.String("t", "", "topic")
	benchType = flag.String("c", "", "consume, produce or latency")
	messages  = flag.Int64("m", -1, "Number of messages to send")
	size      = flag.Int64("s", -1, "Producer: Message size in bytes, Consumer: Batch size")
	ack       = flag.Int("a", 1, "Ack")
	stats     = flag.Int("stats", 0, "Stats print interval")
)

func getClient(benchType string) Benchmark {
	c := NewClient(*brokers, *username, *password, *topic, *messages)
	switch benchType {
	case "consume":
		return ConsumerBenchmark{
			Client: c,
			Batch:  *size,
			Offset: kafka.OffsetBeginning,
		}
	case "produce":
		return ProducerBenchmark{
			Client:      c,
			MessageSize: *size,
			Ack:         Acknowledge(*ack),
		}
	//case "latency":
	//		return LatencyBench(d)
	default:
		fmt.Fprintf(os.Stderr, "Invalid benchmark type %s", benchType)
		os.Exit(1)
	}
	return nil
}

func parseConfigFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		parts := strings.Split(line, "=")
		switch parts[0] {
		case "brokers":
			*brokers = parts[1]
		case "username":
			*username = parts[1]
		case "password":
			*password = parts[1]
		case "topic":
			*topic = parts[1]
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func statsInterval(b Benchmark) chan bool {
	quit := make(chan bool)
	if *stats == 0 {
		return quit
	}
	ticker := time.NewTicker(time.Duration(*stats) * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				s := b.Stat().String()
				if s != "" {
					fmt.Fprintf(os.Stderr, "%s\n", s)
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
	return quit
}

func main() {
	flag.Parse()
	if *confFile != "" {
		if err := parseConfigFile(*confFile); err != nil {
			fmt.Fprintf(os.Stderr, err.Error())
			os.Exit(1)
		}
	}
	b := getClient(*benchType)
	fmt.Fprintf(os.Stderr, "\n%s\n", b.Config())
	q := statsInterval(b)
	err := b.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	q <- true
	json, err := b.Stat().MarshalJSON()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	fmt.Print(json)
}
