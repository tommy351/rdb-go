package rdb

import (
	"bytes"
	"fmt"
	"io"
)

var _ iterator = (*zipMapIterator)(nil)

type zipMapIterator struct {
	DataKey DataKey
	Reader  io.Reader
	Mapper  collectionMapper

	buf    *bytes.Buffer
	index  int64
	length int64
	done   bool
	values []interface{}
}

func (z *zipMapIterator) Next() (interface{}, error) {
	if z.done {
		return nil, io.EOF
	}

	if z.buf == nil {
		s, err := readString(z.Reader)

		if err != nil {
			return nil, err
		}

		z.buf = bytes.NewBufferString(s)

		length, err := readByte(z.buf)

		if err != nil {
			return nil, err
		}

		z.length = int64(length)

		return z.Mapper.MapHead(&collectionHead{
			DataKey: z.DataKey,
			Length:  z.length,
		})
	}

	keyLength, err := z.readLength()

	if err == io.EOF {
		z.buf.Reset()

		z.done = true
		z.buf = nil

		return z.Mapper.MapSlice(&collectionSlice{
			DataKey: z.DataKey,
			Value:   z.values,
		})
	}

	if err != nil {
		return nil, err
	}

	var value HashValue

	if value.Index, err = readStringByLength(z.buf, keyLength); err != nil {
		return nil, err
	}

	valueLength, err := z.readLength()

	if err == io.EOF {
		return nil, fmt.Errorf("unexpected end of zip map for key %q", value.Index)
	}

	if err != nil {
		return nil, err
	}

	// Read the free byte
	if _, err := readByte(z.buf); err != nil {
		return nil, err
	}

	if value.Value, err = readStringByLength(z.buf, valueLength); err != nil {
		return nil, err
	}

	element, err := z.Mapper.MapElement(&collectionElement{
		DataKey: z.DataKey,
		Index:   z.index,
		Length:  z.length,
		Value:   value,
	})

	if err != nil {
		return nil, err
	}

	z.index++
	z.values = append(z.values, value)

	return element, nil
}

func (z *zipMapIterator) readLength() (int64, error) {
	first, err := readByte(z.buf)

	if err != nil {
		return 0, err
	}

	if first < 254 {
		return int64(first), nil
	}

	if first == 254 {
		length, err := readUint32(z.buf)

		if err != nil {
			return 0, err
		}

		return int64(length), nil
	}

	return 0, io.EOF
}
