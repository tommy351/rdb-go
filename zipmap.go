package rdb

import (
	"errors"
	"fmt"
	"io"
)

type zipMapIterator struct {
	DataKey DataKey
	Reader  byteReader
	Mapper  collectionMapper

	buf    byteReader
	index  int
	length int
	done   bool
	values []interface{}
}

func (z *zipMapIterator) Next() (interface{}, error) {
	if z.done {
		return nil, io.EOF
	}

	if z.buf == nil {
		buf, err := readStringEncoding(z.Reader)
		if err != nil {
			return nil, fmt.Errorf("zipmap string read error: %w", err)
		}

		z.buf = newSliceReader(buf)

		length, err := readByte(z.buf)
		if err != nil {
			return nil, fmt.Errorf("zipmap length read error: %w", err)
		}

		z.length = int(length)

		return z.Mapper.MapHead(&collectionHead{
			DataKey: z.DataKey,
			Length:  z.length,
		})
	}

	keyLength, err := z.readLength()
	if errors.Is(err, io.EOF) {
		z.done = true
		z.buf = nil

		return z.Mapper.MapSlice(&collectionSlice{
			DataKey: z.DataKey,
			Value:   z.values,
		})
	}

	if err != nil {
		return nil, fmt.Errorf("zipmap key length read error: %w", err)
	}

	var value HashValue

	if value.Index, err = readStringByLength(z.buf, keyLength); err != nil {
		return nil, fmt.Errorf("zipmap key read error: %w", err)
	}

	valueLength, err := z.readLength()

	if errors.Is(err, io.EOF) {
		return nil, UnexpectedZipMapEndError{Key: value.Index}
	}

	if err != nil {
		return nil, fmt.Errorf("zipmap value length read error: %w", err)
	}

	// Read the free byte
	if _, err := readByte(z.buf); err != nil {
		return nil, fmt.Errorf("zipmap free byte read error: %w", err)
	}

	if value.Value, err = readStringByLength(z.buf, valueLength); err != nil {
		return nil, fmt.Errorf("zipmap value read error: %w", err)
	}

	element, err := z.Mapper.MapEntry(&collectionEntry{
		DataKey: z.DataKey,
		Index:   z.index,
		Length:  z.length,
		Value:   value,
	})
	if err != nil {
		return nil, fmt.Errorf("zipmap map entry error: %w", err)
	}

	z.index++
	z.values = append(z.values, value)

	return element, nil
}

func (z *zipMapIterator) readLength() (int, error) {
	first, err := readByte(z.buf)
	if err != nil {
		return 0, err
	}

	if first < 254 {
		return int(first), nil
	}

	if first == 254 {
		length, err := readUint32(z.buf)
		if err != nil {
			return 0, err
		}

		return int(length), nil
	}

	return 0, io.EOF
}
