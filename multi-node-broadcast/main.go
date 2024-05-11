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
	topology []string
	sync.Mutex
}

func (s *serverConfig) handleBroadcastWorkload(msg maelstrom.Message) error {
	var body, returnBody map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	switch body["type"].(string) {
	case "broadcast":
		returnBody = s.handleBroadcastMessageType(body, msg.Src)
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
func (s *serverConfig) handleBroadcastMessageType(body map[string]any, source string) map[string]any {
	message := body["message"].(float64)

	s.Lock()
	// NOTE: collision is not handled
	s.messages = append(s.messages, message)
	s.Unlock()

	// NOTE: this is only applicable when we are sending messages to all nodes.
	// if message is an original, ie not send by other nodes in the cluster,
	// then we need to propagate it to all other nodes.
	if source[0] != 'n' {
		go s.broadcastMessageAsynchronously(body)
	}

	return map[string]interface{}{
		"type": "broadcast_ok",
	}
}

func (s *serverConfig) broadcastMessageAsynchronously(body map[string]any) {
	var wg sync.WaitGroup

	for _, nodeID := range s.node.NodeIDs() {
		if nodeID == s.node.ID() {
			continue
		}

		wg.Add(1)
		go s.broadcastMessageToNode(body, nodeID, &wg)
	}

	wg.Wait()
}

func (s *serverConfig) broadcastMessageToNode(body map[string]any, nodeID string, wg *sync.WaitGroup) {
	err := s.node.Send(nodeID, body)
	if err != nil {
		panic(err)
	}
	wg.Done()
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
