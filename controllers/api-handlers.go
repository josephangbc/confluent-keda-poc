package controllers

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Package Level Variables
var _producer *kafka.Producer
var _consumer *kafka.Consumer

type RecordValue struct {
	Count int
	Stuck bool
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
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(n % 5)},
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

func ProduceMessageTo(c *gin.Context) {
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
	
	var messageTarget target

	err := c.BindJSON(&messageTarget)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{})
		return
	}

	for n := 0; n < int(messageTarget.Num); n++ {
		data := &RecordValue{
			Count: n}
		data.Stuck = checkIfExist(n, messageTarget.Stuck)
		recordValue, _ := json.Marshal(&data)
		fmt.Printf("Preparing to produce record: %s\n", recordValue)
		_producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &messageTarget.Topic, Partition: int32(n % 5)},
			Value:          []byte(recordValue),
		}, nil)
	}

	// Wait for all messages to be delivered
	var unflushed = _producer.Flush(15 * 1000)
	if unflushed > 0 {
		fmt.Printf("%d messages were NOT produced to topic %s!", unflushed, messageTarget.Topic)
	} else {
		fmt.Printf("%d messages were produced to topic %s!",messageTarget.Num , messageTarget.Topic)
	}

	c.JSON(http.StatusOK, gin.H{"Topic": messageTarget.Topic, "Num": messageTarget.Num})
	return
}

type target struct {
    Topic	string `json:"topic"`
    Num		int32  `json:"num"`
	Stuck	[]int  `json:"stuck"`
}

func checkIfExist(n int, arr []int) bool {
	for i:= range arr {
		if i == n {
			return true
		}
	}
	return false
}

func GenerateHighCPU(c *gin.Context) {

	var taskTarget taskTarget

	err := c.BindJSON(&taskTarget)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{})
		return
	}

	for i:=0; i < taskTarget.NTasks; i++ {
		go oneTask(taskTarget.Iteration)
	}
	
	c.JSON(http.StatusOK, gin.H{})
}

func oneTask(n int) {
	s := 2.0
	for i:= 0; i < n; i++ {
		s = math.Pow(s, 2)
		fmt.Println(s)
	}
	return
}

type taskTarget struct {
	NTasks		int  `json:"nTasks"`
    Iteration	int  `json:"iteration"`
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
	
	subscribeTo := [4]string{"topic_0", "topic_1", "topic_2", "topic_3"}

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
			os.Exit(0)
		default:
			msg, err := c.ReadMessage(100 * time.Millisecond)
			time.Sleep(2000 * time.Millisecond)
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
			if data.Stuck {
				time.Sleep(12000 * time.Millisecond)
			}
			count := data.Count
			totalCount += count
			fmt.Printf("Consumed record from topic %s partition %d at offset %s\n", *msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset)
		}
	}

}

func SetupKafkaConsumerParallel() {
	_consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "pkc-1dkx6.ap-southeast-1.aws.confluent.cloud:9092",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms": "PLAIN",
		"sasl.username": "NQGAHCSBBVIBTRDV",
		"sasl.password": "1gACJscJKWdN+OR1rdZLd78uQEEL0fLjQl/GV6iUTCoNsTyN4QglmNeS9buj6USG",
		"group.id":          "consumer_group_1",
		"auto.offset.reset": "earliest",
		"ssl.ca.location": "/etc/ssl/certs",
		"enable.ssl.certificate.verification": "false",
		"go.events.channel.size": 1,
		"go.events.channel.enable": false,
		// "enable.auto.offset.store": "false",
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	// Subscribe to topic
	
	subscribeTo := [4]string{"topic_0", "topic_1", "topic_2", "topic_3"}

	err = _consumer.SubscribeTopics(subscribeTo[:], nil)
	// Set up a channel for handling Ctrl-C, etc
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Process messages
	// totalCount := 0
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
			os.Exit(0)
		default:
			event := _consumer.Poll(100)
			switch e := event.(type) {
			case *kafka.Message:
				fmt.Printf("Initiate at %s: %d at offset %d \n", *e.TopicPartition.Topic, int(e.TopicPartition.Partition), int(e.TopicPartition.Offset))
				fmt.Printf("Pause at %s: %d at offset %d \n", *e.TopicPartition.Topic, int(e.TopicPartition.Partition), int(e.TopicPartition.Offset))
				_consumer.Pause([]kafka.TopicPartition{e.TopicPartition})
				
				go func() {
					fmt.Printf("Process at %s: %d at offset %d \n", *e.TopicPartition.Topic, int(e.TopicPartition.Partition), int(e.TopicPartition.Offset))
					
					// base sleep time
					time.Sleep(2000 * time.Millisecond)

					// Read Message details
					recordValue := e.Value
					data := RecordValue{}
					err := json.Unmarshal(recordValue, &data)
					fmt.Printf("Trying to consume %s: %d at offset %d \n", *e.TopicPartition.Topic, int(e.TopicPartition.Partition), int(e.TopicPartition.Offset))
					if err != nil {
						fmt.Printf("Failed to decode JSON at offset %d: %v", e.TopicPartition.Offset, err)
					}

					// Check if message is supposed to be stuck
					if data.Stuck {
						for i := 30; i > 0; i-- {
							fmt.Printf("Message at TopicPartition: %s:%d, offset: %d is stuck for another %d sec\n", *e.TopicPartition.Topic,int(e.TopicPartition.Partition), int(e.TopicPartition.Offset), i*10)
							time.Sleep(10000 * time.Millisecond)
						}
					}

					fmt.Printf("Committing at %s: %d at offset %d \n", *e.TopicPartition.Topic, int(e.TopicPartition.Partition), int(e.TopicPartition.Offset))
					_consumer.Commit()
					
					fmt.Printf("Resume at %s: %d at offset %d \n", *e.TopicPartition.Topic, int(e.TopicPartition.Partition), int(e.TopicPartition.Offset))
					_consumer.Resume([]kafka.TopicPartition{e.TopicPartition})
				}()

			case kafka.PartitionEOF:
				// fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				// run = false
			default:
				// fmt.Printf("Ignored %v\n", e)
			}
		}
	}
}
