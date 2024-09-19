package main

import (
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

func mainDo() int {
	s, err := NewServer()
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
