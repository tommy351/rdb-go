package rdb

import (
	"fmt"

	"github.com/tommy351/rdb-go/internal/convert"
)

// SetHead contains the key and the length of a set. It is returned when a set
// is read first time.
type SetHead struct {
	DataKey
	Length int
}

// SetEntry is returned when a new set entry is read.
type SetEntry struct {
	DataKey
	Index  int
	Length int
	Value  string
}

// SetData is returned when all entries in a set are all read.
type SetData struct {
	DataKey
	Value []string
}

type setMapper struct{}

func (setMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &SetHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (setMapper) MapEntry(element *collectionEntry) (interface{}, error) {
	value, err := convert.String(element.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to convert set value to string: %w", err)
	}

	return &SetEntry{
		DataKey: element.DataKey,
		Index:   element.Index,
		Length:  element.Length,
		Value:   value,
	}, nil
}

func (setMapper) MapSlice(slice *collectionSlice) (interface{}, error) {
	data := &SetData{
		DataKey: slice.DataKey,
		Value:   make([]string, len(slice.Value)),
	}

	for i, v := range slice.Value {
		value, err := convert.String(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert set value to string: %w", err)
		}

		data.Value[i] = value
	}

	return data, nil
}
