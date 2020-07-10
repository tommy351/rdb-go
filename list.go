package rdb

import (
	"fmt"

	"github.com/tommy351/rdb-go/internal/convert"
)

// ListHead contains the key and the length of a list. It is returned when a list
// is read first time. The length may be incorrect when the list is backed by a
// quicklist data structure.
type ListHead struct {
	DataKey
	Length int
}

// ListEntry is returned when a new list entry is read.
type ListEntry struct {
	DataKey
	Index  int
	Length int
	Value  string
}

// ListData is returned when all entries in a list are all read.
type ListData struct {
	DataKey
	Value []string
}

type listMapper struct{}

func (listMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &ListHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (listMapper) MapEntry(element *collectionEntry) (interface{}, error) {
	value, err := convert.String(element.Value)

	if err != nil {
		return nil, fmt.Errorf("failed to convert list value to string: %w", err)
	}

	return &ListEntry{
		DataKey: element.DataKey,
		Index:   element.Index,
		Length:  element.Length,
		Value:   value,
	}, nil
}

func (listMapper) MapSlice(slice *collectionSlice) (interface{}, error) {
	data := &ListData{
		DataKey: slice.DataKey,
		Value:   make([]string, len(slice.Value)),
	}

	for i, v := range slice.Value {
		value, err := convert.String(v)

		if err != nil {
			return nil, fmt.Errorf("failed to convert list value to string: %w", err)
		}

		data.Value[i] = value
	}

	return data, nil
}

type listZipListValueReader struct{}

func (listZipListValueReader) ReadValue(r byteReader) (interface{}, error) {
	value, err := readZipListEntry(r)

	if err != nil {
		return nil, fmt.Errorf("failed to read list value from ziplist: %w", err)
	}

	return value, nil
}
