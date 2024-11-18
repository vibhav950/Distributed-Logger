package main

import (
	"fmt"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"} // Replace "localhost" with "<laptop1-IP>" if needed
	topic := "logs"

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer partitionConsumer.Close()

	fmt.Println("Listening for logs...")

	for message := range partitionConsumer.Messages() {
		fmt.Printf("Message received: %s\n", string(message.Value))
	}
}
