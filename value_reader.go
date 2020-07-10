package rdb

import (
	"bufio"
)

type valueReader interface {
	ReadValue(r *bufio.Reader) (interface{}, error)
}

type stringValueReader struct{}

func (stringValueReader) ReadValue(r *bufio.Reader) (interface{}, error) {
	return readString(r)
}
