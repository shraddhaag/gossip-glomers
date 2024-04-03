package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
Received message format:

	{
	  "src": "c1",
	  "dest": "n1",
	  "body": {
	    "type": "generate",
	    "msg_id": 1,
	  }
	}

Response message format:

	{
	  "src": "n1",
	  "dest": "c1",
	  "body": {
	    "type": "generate_ok",
		"id": 123
	    "msg_id": 1,
	    "in_reply_to": 1,
	  }
	}
*/
func main() {
	n := maelstrom.NewNode()

	s := serverConfig{
		node: n,
	}

	n.Handle("generate", s.handleSequentialIDGeneration)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type serverConfig struct {
	node    *maelstrom.Node
	counter int64
}

func (s *serverConfig) handleSequentialIDGeneration(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "generate_ok"
	body["id"] = s.node.ID() + "-" + fmt.Sprint(s.counter)

	// using sync/atomic package here to protect against data race
	atomic.AddInt64(&s.counter, 1)

	return s.node.Reply(msg, body)
}

func (s *serverConfig) handleUUIDGeneration(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "generate_ok"
	body["id"] = uuid.New()

	return s.node.Reply(msg, body)
}
