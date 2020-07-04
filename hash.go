package rdb

import "io"

type HashValue struct {
	Index string
	Value interface{}
}

type HashHead struct {
	DataKey
	Length int64
}

type HashElement struct {
	DataKey
	HashValue
	Length int64
}

type HashData struct {
	DataKey
	Value map[string]interface{}
}

var _ valueReader = hashValueReader{}

type hashValueReader struct{}

func (hashValueReader) ReadValue(r io.Reader) (interface{}, error) {
	key, err := readString(r)

	if err != nil {
		return nil, err
	}

	value, err := readString(r)

	if err != nil {
		return nil, err
	}

	return HashValue{
		Index: key,
		Value: value,
	}, nil
}

var _ collectionMapper = (*hashMapper)(nil)

type hashMapper struct{}

func (hashMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &HashHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (hashMapper) MapElement(element *collectionElement) (interface{}, error) {
	return &HashElement{
		DataKey:   element.DataKey,
		HashValue: element.Value.(HashValue),
		Length:    element.Length,
	}, nil
}

func (hashMapper) MapSlice(slice *collectionSlice) (interface{}, error) {
	data := &HashData{
		DataKey: slice.DataKey,
		Value:   map[string]interface{}{},
	}

	for _, v := range slice.Value {
		v := v.(HashValue)
		data.Value[v.Index] = v.Value
	}

	return data, nil
}
