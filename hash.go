package rdb

import (
	"fmt"
	"io"

	"github.com/tommy351/rdb-go/internal/convert"
)

// HashValue contains a key-value pair of a hash entry.
type HashValue struct {
	Index string
	Value string
}

// HashHead contains the key and the length of a hash. It is returned when a hash
// is read first time.
type HashHead struct {
	DataKey
	Length int64
}

// HashEntry is returned when a new hash entry is read.
type HashEntry struct {
	DataKey
	HashValue
	Length int64
}

// HashData is returned when all entries in a hash are all read.
type HashData struct {
	DataKey
	Value map[string]string
}

type hashValueReader struct{}

func (hashValueReader) ReadValue(r io.Reader) (interface{}, error) {
	key, err := readString(r)

	if err != nil {
		return nil, fmt.Errorf("failed to read hash key: %w", err)
	}

	value, err := readString(r)

	if err != nil {
		return nil, fmt.Errorf("failed to read hash value: %w", err)
	}

	return HashValue{
		Index: key,
		Value: value,
	}, nil
}

type hashMapper struct{}

func (hashMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &HashHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (hashMapper) MapEntry(element *collectionEntry) (interface{}, error) {
	return &HashEntry{
		DataKey:   element.DataKey,
		HashValue: element.Value.(HashValue),
		Length:    element.Length,
	}, nil
}

func (hashMapper) MapSlice(slice *collectionSlice) (interface{}, error) {
	data := &HashData{
		DataKey: slice.DataKey,
		Value:   make(map[string]string, len(slice.Value)),
	}

	for _, v := range slice.Value {
		v := v.(HashValue)
		data.Value[v.Index] = v.Value
	}

	return data, nil
}

type hashZipListValueReader struct{}

func (hashZipListValueReader) ReadValue(r io.Reader) (interface{}, error) {
	key, err := readZipListEntry(r)

	if err != nil {
		return nil, fmt.Errorf("failed to read hash key from ziplist: %w", err)
	}

	value, err := readZipListEntry(r)

	if err != nil {
		return nil, fmt.Errorf("failed to read hash value from ziplist: %w", err)
	}

	keyString, err := convert.String(key)

	if err != nil {
		return nil, fmt.Errorf("failed to convert hash key to string: %w", err)
	}

	valueString, err := convert.String(value)

	if err != nil {
		return nil, fmt.Errorf("failed to convert hash value to string: %w", err)
	}

	return HashValue{
		Index: keyString,
		Value: valueString,
	}, nil
}
