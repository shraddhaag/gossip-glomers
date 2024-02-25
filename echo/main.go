package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
Received message format:

	{
	  "src": "c1",
	  "dest": "n1",
	  "body": {
	    "type": "echo",
	    "msg_id": 1,
	    "echo": "Please echo 35"
	  }
	}

Response message format:

	{
	  "src": "n1",
	  "dest": "c1",
	  "body": {
	    "type": "echo_ok",
	    "msg_id": 1,
	    "in_reply_to": 1,
	    "echo": "Please echo 35"
	  }
	}
*/
func main() {
	n := maelstrom.NewNode()
	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Reply updates the src, dest and body.in_reply_to fields internally.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
