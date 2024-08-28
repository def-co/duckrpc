package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type Server struct {
	db    *sql.DB
	stdin *bufio.Reader
}

func NewServer(db *sql.DB) *Server {
	return &Server{
		db:    db,
		stdin: bufio.NewReader(os.Stdin),
	}
}

func (s *Server) Loop() error {
	s.respondOk()

	for {
		cont, err := s.ProcessOne()
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
}

func (s *Server) ProcessOne() (bool, error) {
	line, err := s.stdin.ReadBytes('\n')
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		return false, fmt.Errorf("stdin read: %w", err)
	}

	var msg RpcMsg
	if err := json.Unmarshal(line, &msg); err != nil {
		return false, fmt.Errorf("stdin json: %w", err)
	}

	switch msg.Command {
	case CommandEnd:
		s.respondOk()
		return false, nil

	case CommandExecute:
		s.cmdExecute(msg.Args)
		return true, nil

	case CommandQueryImmediate:
		s.cmdQueryImmediate(msg.Args)
		return true, nil

	default:
		s.respondErr(fmt.Errorf("unknown command: %s", msg.Command))
		return true, nil
	}
}

func (s *Server) respond(resp map[string]any) {
	buf, err := json.Marshal(resp)
	if err != nil {
		panic(fmt.Errorf("response marshal: %w", err))
	}
	buf = append(buf, '\n')

	os.Stdout.Write(buf)
}

func (s *Server) respondOk() {
	s.respond(map[string]any{
		"ok": true,
	})
}

func (s *Server) respondErr(err error) {
	s.respond(map[string]any{
		"ok":  false,
		"err": err.Error(),
	})
}

func (s *Server) cmdExecute(args map[string]any) {
	query, err := parseQuery(args)
	if err != nil {
		s.respondErr(err)
		return
	}

	res, err := s.db.Exec(query.Sql, query.Args...)
	if err != nil {
		s.respondErr(err)
		return
	}

	resp := map[string]any{
		"ok": true,
	}

	if rows, err := res.RowsAffected(); err != nil {
		resp["aff"] = rows
	}
	if id, err := res.LastInsertId(); err != nil {
		resp["id"] = id
	}

	s.respond(resp)
}

func fetchRow(rows *sql.Rows, cols int) ([]any, error) {
	row := make([]any, cols)
	ptrs := make([]any, cols)
	for i, _ := range row {
		ptrs[i] = &row[i]
	}

	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}

	return row, nil
}

func (s *Server) cmdQueryImmediate(args map[string]any) {
	query, err := parseQuery(args)
	if err != nil {
		s.respondErr(err)
		return
	}

	rows, err := s.db.Query(query.Sql, query.Args...)
	if err != nil {
		s.respondErr(fmt.Errorf("query: %w", err))
		return
	}

	cols, err := rows.Columns()
	if err != nil {
		s.respondErr(fmt.Errorf("get columns: %w", err))
		return
	}

	rowsArr := []any{}
	for rows.Next() {
		if row, err := fetchRow(rows, len(cols)); err != nil {
			s.respondErr(fmt.Errorf("row scan: %w", err))
			return
		} else {
			rowsArr = append(rowsArr, row)
		}
	}

	resp := map[string]any{
		"ok": true,
		"r":  rowsArr,
		"c":  cols,
	}

	if err := rows.Err(); err != nil {
		resp["ok"] = false
		resp["err"] = err.Error()
	}

	s.respond(resp)
}
