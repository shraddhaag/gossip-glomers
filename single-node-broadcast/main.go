package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	s := serverConfig{
		node: n,
	}

	n.Handle("broadcast", s.handleBroadcastWorkload)
	n.Handle("read", s.handleBroadcastWorkload)
	n.Handle("topology", s.handleBroadcastWorkload)
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type serverConfig struct {
	node     *maelstrom.Node
	messages []float64
	sync.Mutex
}

func (s *serverConfig) handleBroadcastWorkload(msg maelstrom.Message) error {
	var body, returnBody map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	switch body["type"].(string) {
	case "broadcast":
		returnBody = s.handleBroadcastMessageType(body)
	case "read":
		returnBody = s.handleReadMessageType(body)
	case "topology":
		returnBody = s.handleTopologyMessageType()
	default:
		return fmt.Errorf("unknown message type")
	}

	return s.node.Reply(msg, returnBody)
}

/*
Received message format:

	{
		"type": "broadcast",
		"message": 1000
	}

Response message format:

	{
	  "type": "broadcast_ok"
	}
*/
func (s *serverConfig) handleBroadcastMessageType(body map[string]any) map[string]any {
	message := body["message"].(float64)

	s.Lock()
	s.messages = append(s.messages, message)
	s.Unlock()

	return map[string]interface{}{
		"type": "broadcast_ok",
	}
}

/*
Received message format:

	{
	  "type": "read"
	}

Response message format:

	{
	  "type": "read_ok",
	  "messages": [1, 8, 72, 25]
	}
*/
func (s *serverConfig) handleReadMessageType(body map[string]any) map[string]any {
	s.Lock()
	messages := s.messages
	s.Unlock()

	return map[string]interface{}{
		"type":     "read_ok",
		"messages": messages,
	}
}

/*
Received message format:

	{
	  "type": "topology",
	  "topology": {
	    "n1": ["n2", "n3"],
	    "n2": ["n1"],
	    "n3": ["n1"]
	  }
	}

Response message format:

	{
	  "type": "topology_ok"
	}
*/
func (s *serverConfig) handleTopologyMessageType() map[string]any {
	return map[string]interface{}{
		"type": "topology_ok",
	}
}
