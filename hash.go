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

type HashEntry struct {
	DataKey
	HashValue
	Length int64
}

type HashData struct {
	DataKey
	Value map[string]interface{}
}

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
		Value:   map[string]interface{}{},
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
		return nil, err
	}

	value, err := readZipListEntry(r)

	if err != nil {
		return nil, err
	}

	keyString, err := convertToString(key)

	if err != nil {
		return nil, err
	}

	return HashValue{
		Index: keyString,
		Value: value,
	}, nil
}
