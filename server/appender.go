package main

import (
	"database/sql"
	"database/sql/driver"
	"fmt"

	duckdb "github.com/marcboeker/go-duckdb"
)

type appender struct {
	open bool
	comm chan any
}

func startAppender(conn *sql.Conn, schema, table string) (*appender, error) {
	app := &appender{
		open: true,
		comm: make(chan any),
	}
	go app.loop(conn, schema, table)
	err := <-app.comm
	if err != nil {
		return nil, err.(error)
	}
	return app, nil
}

func (app *appender) Close() error {
	if !app.open {
		panic("appender not open")
	}
	app.comm <- ([]any)(nil)

	if err := <-app.comm; err != nil {
		return err.(error)
	}
	return nil
}

func (app *appender) Insert(row []any) error {
	if !app.open {
		panic("appender not open")
	}
	app.comm <- row

	if err := <-app.comm; err != nil {
		return err.(error)
	}
	return nil
}

func (app *appender) loop(conn *sql.Conn, schema, table string) {
	defer func() {
		app.open = false
		conn.Close()
		close(app.comm)
	}()

	err := conn.Raw(func(driverConn any) error {
		rawConn, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("raw conn cast failed")
		}

		a, err := duckdb.NewAppenderFromConn(rawConn, schema, table)
		if err != nil {
			return fmt.Errorf("open appender: %w", err)
		}

		app.comm <- error(nil)

		for {
			row := (<-app.comm).([]any)
			if row == nil {
				if err := a.Flush(); err != nil {
					return fmt.Errorf("flush appender: %w", err)
				}
				if err := a.Close(); err != nil {
					return fmt.Errorf("close appender: %w", err)
				}
				app.comm <- error(nil)
				break
			}

			vals := make([]driver.Value, len(row))
			for i, val := range row {
				vals[i] = val
			}
			if err := a.AppendRow(vals...); err != nil {
				return fmt.Errorf("append: %w", err)
			}
			app.comm <- error(nil)
		}

		return nil
	})
	app.comm <- err
}
