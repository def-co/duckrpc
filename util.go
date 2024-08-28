package main

import (
	"errors"
	"fmt"
)

var (
	errMissingKey  = errors.New("missing key")
	errInvalidType = errors.New("invalid type for key")
)

type prober[T any] struct {
	Vals map[int]T
	Next int
}

func newProber[T any]() prober[T] {
	return prober[T]{
		Vals: make(map[int]T, 0),
		Next: 0,
	}
}

func (p *prober[T]) Insert(val T) int {
	k := p.Next
	p.Next += 1
	p.Vals[k] = val
	return k
}

func (p *prober[T]) Release(k int) {
	delete(p.Vals, k)
}

func (p *prober[T]) Get(k int) (T, bool) {
	val, ok := p.Vals[k]
	return val, ok
}

func jsonGet[T any](m map[string]any, k string) (T, error) {
	var t T

	val, ok := m[k]
	if !ok {
		return t, fmt.Errorf("%w: %s", errMissingKey, k)
	}

	switch any(t).(type) {
	case int:
		tf, ok := val.(float64)
		if !ok {
			return t, fmt.Errorf("%w: %s", errInvalidType, k)
		}
		t = any(int(tf)).(T)
		return t, nil
	}

	t, ok = val.(T)
	if !ok {
		return t, fmt.Errorf("%w: %s", errInvalidType, k)
	}

	return t, nil
}
