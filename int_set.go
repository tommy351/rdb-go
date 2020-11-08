package rdb

import (
	"fmt"
	"io"
)

type intSetIterator struct {
	DataKey DataKey
	Reader  byteReader
	Mapper  collectionMapper

	buf      byteReader
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

	if i.buf == nil {
		buf, err := readStringEncoding(i.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read intset buffer: %w", err)
		}

		i.buf = newSliceReader(buf)
		if i.encoding, err = readUint32(i.buf); err != nil {
			return nil, fmt.Errorf("failed to read intset encoding: %w", err)
		}

		length, err := readUint32(i.buf)
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
		i.buf = nil

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
		return readInt64(i.buf)
	case 4:
		return readInt32(i.buf)
	case 2:
		return readInt16(i.buf)
	}

	return nil, IntSetEncodingError{Encoding: i.encoding}
}
