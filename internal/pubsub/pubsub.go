package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	connChan, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}
	durable := false
	autoDelete := true
	exclusive := true
	if queueType == Durable {
		durable = true
		autoDelete = false
		exclusive = false
	}

	amqpTable := make(amqp.Table)
	amqpTable["x-dead-letter-exchange"] = "peril_dlx"
	queue, err := connChan.QueueDeclare(queueName, durable, autoDelete, exclusive, false, amqpTable)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare queue: %v", err)
	}

	if err = connChan.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}

	return connChan, queue, nil
}
