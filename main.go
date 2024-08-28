package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

func mainDo() int {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s dbname\n", os.Args[0])
		return 64
	}

	dbname := os.Args[1]

	db, err := sql.Open("duckdb", dbname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not open database: %s\n", err)
		return 1
	}

	s, err := NewServer(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 1
	}

	if err := s.Loop(); err != nil {
		fmt.Fprintf(os.Stderr, "processing failed: %s\n", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(mainDo())
}
