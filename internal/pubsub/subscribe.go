package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}

func SubscribeGOB[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var target T
		buffer := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buffer)
		err := decoder.Decode(&target)
		return target, err
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failed to declare and bind queue: %v", err)
	}

	msgs, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %v", err)
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				log.Printf("failed to unmarshal message: %v\n", err)
				continue
			}
			rAck := handler(target)
			switch rAck {
			case Ack:
				log.Println("acknowledging message...")
				if err = msg.Ack(false); err != nil {
					log.Printf("failed to acknowledge message: %v\n", err)
				}
			case NackRequeue:
				log.Println("requeuing message...")
				if err = msg.Nack(false, true); err != nil {
					log.Printf("failed to requeue message: %v\n", err)
				}
			case NackDiscard:
				log.Println("discarding message...")
				if err = msg.Nack(false, false); err != nil {
					log.Printf("failed to discard message: %v\n", err)
				}
			}
		}
	}()
	return nil
}
