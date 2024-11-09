package main

import (
	"encoding/json"
	"github.com/meoying/local-msg-go/internal/msg"
	"testing"
)

func TestJSON(t *testing.T) {
	val, err := json.Marshal(msg.Msg{
		Topic:   "order_created",
		Content: "这是消息体",
	})
	t.Log(err)
	t.Log(string(val))
}
