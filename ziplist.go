package rdb

import (
	"bytes"
	"fmt"
	"io"
)

type zipListIterator struct {
	DataKey     DataKey
	Reader      io.Reader
	ValueReader valueReader
	Mapper      collectionMapper

	buf        *bytes.Buffer
	zlBytes    uint32
	tailOffset uint32
	index      int64
	length     int64
	done       bool
	values     []interface{}
}

func (z *zipListIterator) Next() (interface{}, error) {
	if z.done {
		return nil, io.EOF
	}

	if z.buf == nil {
		s, err := readString(z.Reader)

		if err != nil {
			return nil, err
		}

		z.buf = bytes.NewBufferString(s)

		if z.zlBytes, err = readUint32(z.buf); err != nil {
			return nil, err
		}

		if z.tailOffset, err = readUint32(z.buf); err != nil {
			return nil, err
		}

		length, err := readUint16(z.buf)

		if err != nil {
			return nil, err
		}

		z.length = int64(length)

		return z.Mapper.MapHead(&collectionHead{
			DataKey: z.DataKey,
			Length:  z.length,
		})
	}

	if z.index == z.length {
		z.buf.Reset()

		z.done = true
		z.buf = nil

		return z.Mapper.MapSlice(&collectionSlice{
			DataKey: z.DataKey,
			Value:   z.values,
		})
	}

	value, err := z.ValueReader.ReadValue(z.buf)

	if err != nil {
		return nil, err
	}

	element, err := z.Mapper.MapEntry(&collectionEntry{
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

func readZipListEntry(r io.Reader) (interface{}, error) {
	var prevLen uint32

	b, err := readByte(r)

	if err != nil {
		return nil, err
	}

	if b == 254 {
		i, err := readUint32(r)

		if err != nil {
			return nil, err
		}

		prevLen = i
	} else {
		prevLen = uint32(b)
	}

	_ = prevLen

	header, err := readByte(r)

	if err != nil {
		return nil, err
	}

	switch {
	case (header >> 6) == 0:
		return readStringByLength(r, int64(header&0x3f))

	case (header >> 6) == 1:
		next, err := readByte(r)

		if err != nil {
			return nil, err
		}

		return readStringByLength(r, int64(header&0x3f)<<8|int64(next))

	case (header >> 6) == 2:
		length, err := readUint32BE(r)

		if err != nil {
			return nil, err
		}

		return readStringByLength(r, int64(length))

	case (header >> 4) == 12:
		return readInt16(r)

	case (header >> 4) == 13:
		return readInt32(r)

	case (header >> 4) == 14:
		return readInt64(r)

	case header == 240:
		return read24BitSignedNumber(r)

	case header == 254:
		return readInt8(r)

	case header >= 241 && header <= 253:
		return header - 241, nil
	}

	return nil, fmt.Errorf("invalid ziplist entry header %d", header)
}
