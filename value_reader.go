package rdb

import "io"

type valueReader interface {
	ReadValue(r io.Reader) (interface{}, error)
}

var _ valueReader = stringValueReader{}

type stringValueReader struct{}

func (stringValueReader) ReadValue(r io.Reader) (interface{}, error) {
	return readString(r)
}
