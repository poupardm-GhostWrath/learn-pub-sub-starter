package main

import (
	"fmt"
	"log"
	"strconv"

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

	connChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to create channel: %v", err)
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("failed to get username: %v", err)
	}

	gs := gamelogic.NewGameState(username)

	// Subscribe to Pause Queue
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, gs.GetUsername()),
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gs))
	if err != nil {
		log.Fatalf("failed to subscribe to pause: %v", err)
	}

	// Subscribe to Army Move Queue
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, gs.GetUsername()),
		fmt.Sprintf("%s.*", routing.ArmyMovesPrefix),
		pubsub.Transient,
		handlerMove(gs, connChan))
	if err != nil {
		log.Fatalf("failed to subscribe to army moves: %v", err)
	}

	// Subscribe to War Queue
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix),
		pubsub.Durable,
		handlerWar(gs, connChan))
	if err != nil {
		log.Fatalf("failed to subscribe to war: %v", err)
	}

	for {
		words := gamelogic.GetInput()
		if len(words) < 1 {
			continue
		}
		switch words[0] {
		case "spawn":
			if err = gs.CommandSpawn(words); err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gs.CommandMove(words)
			if err != nil {
				fmt.Printf("failed to move: %v", err)
				continue
			}
			err = pubsub.PublishJSON(
				connChan,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, move.Player.Username),
				move)
			if err != nil {
				fmt.Printf("failed to publish move: %v", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Println("usage: spam <int>")
				continue
			}
			num, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println("invalid integer")
				continue
			}
			for range num {
				msg := gamelogic.GetMaliciousLog()
				err = publishGameLog(connChan, msg, username)
				if err != nil {
					fmt.Printf("failed to publish message: %v\n", err)
				}
			}
			fmt.Printf("Published %v malicious logs\n", num)
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command")
		}
	}
}
