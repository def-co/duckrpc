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

type dbc struct {
	db *sql.DB

	// conn is a shared connection used for the message handler for any queries
	// run via Execute or Query commands.
	// Appenders run in separate threads and use their own conns.
	conn *sql.Conn
}

type q struct {
	conn *sql.Conn
	rows *sql.Rows
}

type Server struct {
	stdin *bufio.Reader
	hdls  map[string]hdl

	ds prober[*dbc]
	qs prober[q]
	as prober[*appender]
}

func NewServer() (*Server, error) {
	s := &Server{
		stdin: bufio.NewReader(os.Stdin),
		ds:    newProber[*dbc](),
		qs:    newProber[q](),
		as:    newProber[*appender](),
	}
	s.hdls = map[string]hdl{
		CommandConnect:         s.cmdConnect,
		CommandDisconnect:      s.cmdDisconnect,
		CommandExecute:         s.cmdExecute,
		CommandQueryImmediate:  s.cmdQueryImmediate,
		CommandQuery:           s.cmdQueryHandle,
		CommandQueryFetch:      s.cmdQueryFetch,
		CommandQueryRelease:    s.cmdQueryRelease,
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

func (s *Server) cmdConnect(args map[string]any) error {
	path, err := jsonGet[string](args, "p")
	if err != nil {
		return err
	}

	db, err := sql.Open("duckdb", path)
	if err != nil {
		return err
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		db.Close()
		return err
	}

	k := s.ds.Insert(&dbc{db, conn})

	s.respond(map[string]any{
		"ok": true,
		"d":  k,
	})
	return nil
}

func (s *Server) cmdDisconnect(args map[string]any) error {
	handle, err := jsonGet[int](args, "d")
	if err != nil {
		return err
	}

	dbc, ok := s.ds.Get(handle)
	if !ok {
		return fmt.Errorf("no such connection")
	}

	for k, q := range s.qs.Vals {
		if q.conn == dbc.conn {
			q.rows.Close()
			s.qs.Release(k)
		}
	}
	for k, a := range s.as.Vals {
		if a.db == dbc.db {
			a.Close()
			s.as.Release(k)
		}
	}

	dbc.conn.Close()
	dbc.db.Close()

	s.ds.Release(handle)

	s.respondOk()
	return nil
}

func (s *Server) cmdExecute(args map[string]any) error {
	dbHandle, err := jsonGet[int](args, "d")
	if err != nil {
		return err
	}
	dbc, ok := s.ds.Get(dbHandle)
	if !ok {
		return fmt.Errorf("invalid connection handle")
	}

	query, err := parseQuery(args)
	if err != nil {
		return err
	}

	res, err := dbc.conn.ExecContext(context.Background(), query.Sql, query.Args...)
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
	dbHandle, err := jsonGet[int](args, "d")
	if err != nil {
		return err
	}
	dbc, ok := s.ds.Get(dbHandle)
	if !ok {
		return fmt.Errorf("invalid connection handle")
	}

	query, err := parseQuery(args)
	if err != nil {
		return err
	}

	rows, err := dbc.conn.QueryContext(context.Background(), query.Sql, query.Args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
	}

	k := s.qs.Insert(q{dbc.conn, rows})

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
	cols, err := query.rows.Columns()
	if err != nil {
		panic(fmt.Errorf("consistency error: could not fetch cols anymore: %w", err))
	}
	colCount := len(cols)

	eof := false
	rows := []any{}
	for i := 0; i < numRows; i += 1 {
		if !query.rows.Next() {
			eof = true
			break
		}

		if row, err := fetchRow(query.rows, colCount); err != nil {
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

	if err := query.rows.Err(); err != nil {
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

	query.rows.Close()
	s.qs.Release(handle)

	s.respondOk()
	return nil
}

func (s *Server) cmdQueryImmediate(args map[string]any) error {
	dbHandle, err := jsonGet[int](args, "d")
	if err != nil {
		return err
	}
	dbc, ok := s.ds.Get(dbHandle)
	if !ok {
		return fmt.Errorf("invalid connection handle")
	}

	query, err := parseQuery(args)
	if err != nil {
		return err
	}

	rows, err := dbc.conn.QueryContext(context.Background(), query.Sql, query.Args...)
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
	dbHandle, err := jsonGet[int](args, "d")
	if err != nil {
		return err
	}
	dbc, ok := s.ds.Get(dbHandle)
	if !ok {
		return fmt.Errorf("invalid connection handle")
	}

	table, err := jsonGet[string](args, "t")
	if err != nil {
		return err
	}

	conn, err := dbc.db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	app, err := startAppender(dbc.db, conn, "main", table)
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
