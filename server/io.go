package main

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	CommandExecute         = "e"
	CommandEnd             = "x"
	CommandQueryImmediate  = "qq"
	CommandQuery           = "q"
	CommandQueryFetch      = "qf"
	CommandQueryRelease    = "qx"
	CommandAppender        = "a"
	CommandAppenderInsert  = "ai"
	CommandAppenderRelease = "ax"
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

	if sql, err := jsonGet[string](data, "q"); err != nil {
		return q, err
	} else {
		q.Sql = sql
	}

	if args, err := jsonGet[[]any](data, "p"); err != nil {
		if !errors.Is(err, errMissingKey) {
			return q, err
		}
	} else {
		q.Args = args
	}

	return q, nil
}
