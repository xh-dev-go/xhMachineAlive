package main

import (
	"flag"
	"github.com/segmentio/kafka-go"
	"github.com/xh-dev-go/xhUtils/xhKafka/header"
	"github.com/xh-dev-go/xhUtils/xhKafka/producer"
	"log"
	"os"
	"time"
)

func send(w *producer.XhKafkaProducer, kafkaTopic,key,name string){
	var headers = header.FromKafkaHeader([]kafka.Header{})
	headers = headers.Add("Date", time.Now().String())
	headers = headers.Add("KEY", key)
	headers = headers.Add("NAME", name)

	err := w.SimpleSend(
		// NOTE: Each Message has TopicOut defined, otherwise an error is returned.
		kafka.Message{
			Topic:   kafkaTopic,
			Value:   []byte{},
			Headers: headers,
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}
func main() {
	cmdKafkaHost, cmdKafkaTopic, cmdKey, cmdName := "kafka-host", "kafka-topic", "key", "name"
	var kafkaHost, kafkaTopic, key, name string
	flag.StringVar(&kafkaHost, cmdKafkaHost, "", "kafka host")
	flag.StringVar(&kafkaTopic, cmdKafkaTopic, "", "kafka topic")
	flag.StringVar(&key, cmdKey, "", "key of device")
	flag.StringVar(&name, cmdName, "", "name of device")
	flag.Parse()

	if name == "" {
		println("Please enter the name of device")
		flag.Usage()
		os.Exit(1)
	}

	if key == "" {
		println("Please enter the key of device")
		flag.Usage()
		os.Exit(1)
	}

	if kafkaHost == "" {
		println("Please enter the kafka server")
		flag.Usage()
		os.Exit(1)
	}
	if kafkaTopic == "" {
		println("Please enter the kafka topic")
		flag.Usage()
		os.Exit(1)
	}

	w := producer.New(kafkaHost)
	for{
		send(&w, kafkaTopic, key, name)
		time.Sleep(time.Minute)
	}
}
