package rdb

import (
	"fmt"

	"github.com/tommy351/rdb-go/internal/convert"
)

// SortedSetValue contains the value and its score of a sorted set entry.
type SortedSetValue struct {
	Value string
	Score float64
}

// SortedSetHead contains the key and the length of a sorted set. It is returned
// when a sorted set is read first time.
type SortedSetHead struct {
	DataKey
	Length int
}

// SortedSetEntry is returned when a new sorted set entry is read.
type SortedSetEntry struct {
	DataKey
	SortedSetValue
	Index  int
	Length int
}

// SortedSetData is returned when all entries in a sorted set are all read.
type SortedSetData struct {
	DataKey
	Value []SortedSetValue
}

type sortedSetValueReader struct {
	Type byte
}

func (z sortedSetValueReader) ReadValue(r byteReader) (interface{}, error) {
	value, err := readString(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read zset value: %w", err)
	}

	score, err := z.readScore(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read zset score: %w", err)
	}

	return SortedSetValue{
		Value: value,
		Score: score,
	}, nil
}

func (z sortedSetValueReader) readScore(r byteReader) (float64, error) {
	if z.Type == typeZSet2 {
		return readBinaryDouble(r)
	}

	return readFloat(r)
}

type sortedSetMapper struct{}

func (sortedSetMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &SortedSetHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (sortedSetMapper) MapEntry(element *collectionEntry) (interface{}, error) {
	return &SortedSetEntry{
		DataKey:        element.DataKey,
		SortedSetValue: element.Value.(SortedSetValue),
		Index:          element.Index,
		Length:         element.Length,
	}, nil
}

func (sortedSetMapper) MapSlice(slice *collectionSlice) (interface{}, error) {
	data := &SortedSetData{
		DataKey: slice.DataKey,
		Value:   make([]SortedSetValue, len(slice.Value)),
	}

	for i, v := range slice.Value {
		data.Value[i] = v.(SortedSetValue)
	}

	return data, nil
}

type sortedSetZipListValueReader struct{}

func (s sortedSetZipListValueReader) ReadValue(r byteReader) (interface{}, error) {
	value, err := readZipListEntry(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read zset value from ziplist: %w", err)
	}

	score, err := readZipListEntry(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read zset score from ziplist: %w", err)
	}

	valueString, err := convert.String(value)
	if err != nil {
		return nil, fmt.Errorf("failed to convert zset value to string: %w", err)
	}

	scoreFloat, err := convert.Float64(score)
	if err != nil {
		return nil, fmt.Errorf("failed to convert zset score to float64: %w", err)
	}

	return SortedSetValue{
		Value: valueString,
		Score: scoreFloat,
	}, nil
}
