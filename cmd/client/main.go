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
	fmt.Println("Starting Peril client...")
	const connectionURL = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connectionURL)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("failed to get username: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("failed to declare and subscribe to queue: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) < 1 {
			continue
		}
		switch words[0] {
		case "spawn":
			if err = gameState.CommandSpawn(words); err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			_, err = gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("Move successful!")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}
