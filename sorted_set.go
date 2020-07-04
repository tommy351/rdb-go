package rdb

import (
	"io"
)

type SortedSetValue struct {
	Value interface{}
	Score float64
}

type SortedSetHead struct {
	DataKey
	Length int64
}

type SortedSetEntry struct {
	DataKey
	SortedSetValue
	Index  int64
	Length int64
}

type SortedSetData struct {
	DataKey
	Value []SortedSetValue
}

var _ valueReader = sortedSetValueReader{}

type sortedSetValueReader struct {
	Type byte
}

func (z sortedSetValueReader) ReadValue(r io.Reader) (interface{}, error) {
	value, err := readString(r)

	if err != nil {
		return nil, err
	}

	score, err := z.readScore(r)

	if err != nil {
		return nil, err
	}

	return SortedSetValue{
		Value: value,
		Score: score,
	}, nil
}

func (z sortedSetValueReader) readScore(r io.Reader) (float64, error) {
	if z.Type == typeZSet2 {
		return readBinaryDouble(r)
	}

	return readFloat(r)
}

var _ collectionMapper = (*sortedSetMapper)(nil)

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
	}

	for _, v := range slice.Value {
		data.Value = append(data.Value, v.(SortedSetValue))
	}

	return data, nil
}

var _ valueReader = sortedSetZipListValueReader{}

type sortedSetZipListValueReader struct{}

func (s sortedSetZipListValueReader) ReadValue(r io.Reader) (interface{}, error) {
	value, err := readZipListEntry(r)

	if err != nil {
		return nil, err
	}

	score, err := readZipListEntry(r)

	if err != nil {
		return nil, err
	}

	scoreFloat, err := convertToFloat64(score)

	if err != nil {
		return nil, err
	}

	return SortedSetValue{
		Value: value,
		Score: scoreFloat,
	}, nil
}
