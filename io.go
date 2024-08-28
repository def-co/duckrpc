package main

import (
	"encoding/json"
	"fmt"
)

var (
	CommandExecute        = "e"
	CommandEnd            = "x"
	CommandQueryImmediate = "qq"
)

type RpcMsg struct {
	Command string
	Args    map[string]any
}

func (msg *RpcMsg) UnmarshalJSON(buf []byte) error {
	var data map[string]any
	if err := json.Unmarshal(buf, &data); err != nil {
		return err
	}

	cmd, ok := data["@"].(string)
	if !ok {
		return fmt.Errorf("no method key")
	}

	delete(data, "@")
	msg.Command = cmd
	msg.Args = data

	return nil
}

type Query struct {
	Sql  string
	Args []any
}

func parseQuery(data map[string]any) (Query, error) {
	var q Query
	if sql, ok := data["q"].(string); !ok {
		return q, fmt.Errorf("invalid query: q key missing")
	} else {
		q.Sql = sql
	}

	if args, ok := data["p"].([]any); ok {
		q.Args = args
	}

	return q, nil
}
