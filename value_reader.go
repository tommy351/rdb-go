package rdb

import (
	"github.com/tommy351/rdb-go/internal/reader"
)

type valueReader interface {
	ReadValue(r reader.BytesReader) (interface{}, error)
}

type stringValueReader struct{}

func (stringValueReader) ReadValue(r reader.BytesReader) (interface{}, error) {
	return readString(r)
}
