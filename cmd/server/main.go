package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril Game Server....")
	const connectionURL = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connectionURL)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	connChan, queue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.Durable)
	if err != nil {
		log.Fatalf("could not subscribe to game logs: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) < 1 {
			continue
		}
		switch words[0] {
		case "pause":
			log.Println("Sending pause message")
			err = pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("could not publish to RabbitMQ: %v", err)
			}
		case "resume":
			log.Println("Sending resume message")
			err = pubsub.PublishJSON(connChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("could not publish to RabbitMQ: %v", err)
			}
		case "quit":
			log.Println("Exiting")
			return
		default:
			fmt.Println("Unknown Command")
		}
	}
}
