package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Package Level Variables
var _producer *kafka.Producer

type RecordValue struct {
	Count int
}

// Functions
func MessagePongHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "pong"})
}


func SetupKafkaProducer() {
	var err error
	_producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "pkc-1dkx6.ap-southeast-1.aws.confluent.cloud:9092",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms": "PLAIN",
		"sasl.username": "NQGAHCSBBVIBTRDV",
		"sasl.password": "1gACJscJKWdN+OR1rdZLd78uQEEL0fLjQl/GV6iUTCoNsTyN4QglmNeS9buj6USG",
		"ssl.ca.location": "/etc/ssl/certs",
		"enable.ssl.certificate.verification": "false",
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}
}


func ProduceMessage(c *gin.Context) {
	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range _producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	topic := "topic_0"

	for n := 0; n < 10; n++ {
		data := &RecordValue{
			Count: n}
		recordValue, _ := json.Marshal(&data)
		fmt.Printf("Preparing to produce record: %s\n", recordValue)
		_producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(recordValue),
		}, nil)
	}

	// Wait for all messages to be delivered
	var unflushed = _producer.Flush(15 * 1000)
	if unflushed > 0 {
		fmt.Printf("%d messages were NOT produced to topic %s!", unflushed, topic)
	} else {
		fmt.Printf("10 messages were produced to topic %s!", topic)
	}
}


func SetupKafkaConsumer() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "pkc-1dkx6.ap-southeast-1.aws.confluent.cloud:9092",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms": "PLAIN",
		"sasl.username": "NQGAHCSBBVIBTRDV",
		"sasl.password": "1gACJscJKWdN+OR1rdZLd78uQEEL0fLjQl/GV6iUTCoNsTyN4QglmNeS9buj6USG",
		"group.id":          "consumer_group_1",
		"auto.offset.reset": "earliest",
		"ssl.ca.location": "/etc/ssl/certs",
		"enable.ssl.certificate.verification": "false",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	// Subscribe to topic
	
	subscribeTo := [1]string{"topic_0"}

	err = c.SubscribeTopics(subscribeTo[:], nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	totalCount := 0
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := c.ReadMessage(100 * time.Millisecond)
			time.Sleep(10000 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			//recordKey := string(msg.Key)
			recordValue := msg.Value
			data := RecordValue{}
			err = json.Unmarshal(recordValue, &data)
			if err != nil {
				fmt.Printf("Failed to decode JSON at offset %d: %v", msg.TopicPartition.Offset, err)
				continue
			}
			count := data.Count
			totalCount += count
			fmt.Printf("Consumed record from topic %s partition %s at offset %s", msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
		}
	}

}

