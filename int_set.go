package rdb

import (
	"fmt"
	"io"
)

type intSetIterator struct {
	DataKey DataKey
	Reader  bufReader
	Mapper  collectionMapper

	r        bufReader
	done     bool
	encoding uint32
	index    int
	length   int
	values   []interface{}
}

func (i *intSetIterator) Next() (interface{}, error) {
	if i.done {
		return nil, io.EOF
	}

	if i.r == nil {
		sr, err := newStringReader(i.Reader)

		if err != nil {
			return nil, fmt.Errorf("failed to read intset buffer: %w", err)
		}

		i.r = sr

		if i.encoding, err = readUint32(i.r); err != nil {
			return nil, fmt.Errorf("failed to read intset encoding: %w", err)
		}

		length, err := readUint32(i.r)

		if err != nil {
			return nil, fmt.Errorf("failed to read intset length: %w", err)
		}

		i.length = int(length)

		head, err := i.Mapper.MapHead(&collectionHead{
			DataKey: i.DataKey,
			Length:  i.length,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to map head in intset: %w", err)
		}

		return head, nil
	}

	if i.index == i.length {
		i.done = true
		i.r = nil

		slice, err := i.Mapper.MapSlice(&collectionSlice{
			DataKey: i.DataKey,
			Value:   i.values,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to map slice in intset: %w", err)
		}

		return slice, nil
	}

	value, err := i.readValue()

	if err != nil {
		return nil, err
	}

	entry, err := i.Mapper.MapEntry(&collectionEntry{
		DataKey: i.DataKey,
		Index:   i.index,
		Length:  i.length,
		Value:   value,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to map entry in intset: %w", err)
	}

	i.index++
	i.values = append(i.values, value)

	return entry, nil
}

func (i *intSetIterator) readValue() (interface{}, error) {
	switch i.encoding {
	case 8:
		return readInt64(i.r)
	case 4:
		return readInt32(i.r)
	case 2:
		return readInt16(i.r)
	}

	return nil, IntSetEncodingError{Encoding: i.encoding}
}
