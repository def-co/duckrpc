package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

type hdl func(map[string]any) error

type Server struct {
	db *sql.DB

	// c is a shared connection reused for the message handler.
	// It is not reused for appenders which run in separate threads.
	c *sql.Conn

	stdin *bufio.Reader
	hdls  map[string]hdl
	qs    prober[*sql.Rows]
	as    prober[*appender]
}

func NewServer(db *sql.DB) (*Server, error) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	s := &Server{
		db:    db,
		c:     conn,
		stdin: bufio.NewReader(os.Stdin),
		qs:    newProber[*sql.Rows](),
		as:    newProber[*appender](),
	}
	s.hdls = map[string]hdl{
		CommandExecute:         s.cmdExecute,
		CommandQueryImmediate:  s.cmdQueryImmediate,
		CommandQuery:           s.cmdQueryHandle,
		CommandQueryFetch:      s.cmdQueryFetch,
		CommandQueryRelease:    s.cmdQueryHandle,
		CommandAppender:        s.cmdAppender,
		CommandAppenderInsert:  s.cmdAppenderInsert,
		CommandAppenderRelease: s.cmdAppenderRelease,
	}
	return s, nil
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

	if msg.Command == CommandEnd {
		s.respondOk()
		return false, nil
	} else if hdl, ok := s.hdls[msg.Command]; ok {
		if err := hdl(msg.Args); err != nil {
			s.respondErr(err)
		}
		return true, nil
	} else {
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

func (s *Server) cmdExecute(args map[string]any) error {
	query, err := parseQuery(args)
	if err != nil {
		return err
	}

	res, err := s.c.ExecContext(context.Background(), query.Sql, query.Args...)
	if err != nil {
		return err
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
	return nil
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

func (s *Server) cmdQueryHandle(args map[string]any) error {
	query, err := parseQuery(args)
	if err != nil {
		return err
	}

	rows, err := s.c.QueryContext(context.Background(), query.Sql, query.Args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
	}

	k := s.qs.Insert(rows)

	s.respond(map[string]any{
		"ok": true,
		"h":  k,
		"c":  cols,
	})
	return nil
}

func (s *Server) cmdQueryFetch(args map[string]any) error {
	numRows, err := jsonGet[int](args, "n")
	if err != nil {
		return err
	}

	handle, err := jsonGet[int](args, "h")
	if err != nil {
		return err
	}

	query, ok := s.qs.Get(handle)
	if !ok {
		return fmt.Errorf("no such handle")
	}
	cols, err := query.Columns()
	if err != nil {
		panic(fmt.Errorf("consistency error: could not fetch cols anymore: %w", err))
	}
	colCount := len(cols)

	eof := false
	rows := []any{}
	for i := 0; i < numRows; i += 1 {
		if !query.Next() {
			eof = true
			break
		}

		if row, err := fetchRow(query, colCount); err != nil {
			return fmt.Errorf("row scan: %w", err)
		} else {
			rows = append(rows, row)
		}
	}

	resp := map[string]any{
		"ok":  true,
		"r":   rows,
		"eof": eof,
	}

	if err := query.Err(); err != nil {
		resp["ok"] = false
		resp["err"] = err.Error()
	}

	s.respond(resp)
	return nil
}

func (s *Server) cmdQueryRelease(args map[string]any) error {
	handle, err := jsonGet[int](args, "h")
	if err != nil {
		return err
	}

	query, ok := s.qs.Get(handle)
	if !ok {
		return fmt.Errorf("no such query")
	}

	query.Close()
	s.qs.Release(handle)

	s.respondOk()
	return nil
}

func (s *Server) cmdQueryImmediate(args map[string]any) error {
	query, err := parseQuery(args)
	if err != nil {
		return err
	}

	rows, err := s.c.QueryContext(context.Background(), query.Sql, query.Args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
	}

	rowsArr := []any{}
	for rows.Next() {
		if row, err := fetchRow(rows, len(cols)); err != nil {
			return fmt.Errorf("row scan: %w", err)
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
	return nil
}

func (s *Server) cmdAppender(args map[string]any) error {
	table, err := jsonGet[string](args, "t")
	if err != nil {
		return err
	}

	conn, err := s.db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	app, err := startAppender(conn, "main", table)
	if err != nil {
		return err
	}

	handle := s.as.Insert(app)
	s.respond(map[string]any{
		"ok": true,
		"h":  handle,
	})
	return nil
}

func (s *Server) cmdAppenderInsert(args map[string]any) error {
	handle, err := jsonGet[int](args, "h")
	if err != nil {
		return err
	}

	rows, err := jsonGet[[]any](args, "r")
	if err != nil {
		return err
	}

	app, ok := s.as.Get(handle)
	if !ok {
		return fmt.Errorf("no such handle")
	}

	for i, row := range rows {
		rowArr, ok := row.([]any)
		if !ok {
			return fmt.Errorf("incorrect row type: expected array (at %d)", i)
		}
		if err := app.Insert(rowArr); err != nil {
			s.as.Release(handle)
			return err
		}
	}

	s.respondOk()
	return nil
}

func (s *Server) cmdAppenderRelease(args map[string]any) error {
	handle, err := jsonGet[int](args, "h")
	if err != nil {
		return err
	}

	app, ok := s.as.Get(handle)
	if !ok {
		return fmt.Errorf("no such handle")
	}
	defer s.as.Release(handle)

	if err := app.Close(); err != nil {
		return err
	}

	s.respondOk()
	return nil
}
