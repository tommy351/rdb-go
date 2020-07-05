package rdb

import (
	"bytes"
	"io"
)

type intSetIterator struct {
	DataKey DataKey
	Reader  io.Reader
	Mapper  collectionMapper

	buf      *bytes.Buffer
	encoding uint32
	index    int64
	length   int64
	done     bool
	values   []interface{}
}

func (i *intSetIterator) Next() (interface{}, error) {
	if i.done {
		return nil, io.EOF
	}

	if i.buf == nil {
		s, err := readString(i.Reader)

		if err != nil {
			return nil, err
		}

		i.buf = bytes.NewBufferString(s)

		if i.encoding, err = readUint32(i.buf); err != nil {
			return nil, err
		}

		length, err := readUint32(i.buf)

		if err != nil {
			return nil, err
		}

		i.length = int64(length)

		return i.Mapper.MapHead(&collectionHead{
			DataKey: i.DataKey,
			Length:  i.length,
		})
	}

	if i.index == i.length {
		i.buf.Reset()

		i.done = true
		i.buf = nil

		return i.Mapper.MapSlice(&collectionSlice{
			DataKey: i.DataKey,
			Value:   i.values,
		})
	}

	value, err := i.readValue()

	if err != nil {
		return nil, err
	}

	element, err := i.Mapper.MapEntry(&collectionEntry{
		DataKey: i.DataKey,
		Index:   i.index,
		Length:  i.length,
		Value:   value,
	})

	if err != nil {
		return nil, err
	}

	i.index++
	i.values = append(i.values, value)

	return element, nil
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
