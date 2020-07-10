package rdb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

type zipListIterator struct {
	DataKey     DataKey
	Reader      *bufio.Reader
	ValueReader valueReader
	Mapper      collectionMapper
	ValueLength int

	buf    *bufio.Reader
	index  int
	length int
	done   bool
	values []interface{}
}

func (z *zipListIterator) Next() (interface{}, error) {
	if z.done {
		return nil, io.EOF
	}

	if z.buf == nil {
		buf, err := readStringEncoding(z.Reader)

		if err != nil {
			return nil, fmt.Errorf("failed to read ziplist buffer: %w", err)
		}

		z.buf = bufio.NewReader(bytes.NewReader(buf))

		if _, err := readUint32(z.buf); err != nil {
			return nil, fmt.Errorf("failed to read ziplist zlbytes: %w", err)
		}

		if _, err := readUint32(z.buf); err != nil {
			return nil, fmt.Errorf("failed to ziplist tail offset: %w", err)
		}

		if z.length, err = z.readLength(); err != nil {
			return nil, fmt.Errorf("failed to read ziplist length: %w", err)
		}

		return z.Mapper.MapHead(&collectionHead{
			DataKey: z.DataKey,
			Length:  z.length,
		})
	}

	if z.index == z.length {
		end, err := z.buf.ReadByte()

		if err != nil {
			return nil, fmt.Errorf("failed to read ziplist end: %w", err)
		}

		if end != 255 {
			return nil, ZipListEndError{Value: end}
		}

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

func (z *zipListIterator) readLength() (int, error) {
	value, err := readUint16(z.buf)

	if err != nil {
		return 0, err
	}

	length := int(value)

	if length%z.ValueLength != 0 {
		return 0, ZipListLengthError{
			Length:      length,
			ValueLength: z.ValueLength,
		}
	}

	return length / z.ValueLength, nil
}

func readZipListEntry(r *bufio.Reader) (interface{}, error) {
	var prevLen uint32

	b, err := r.ReadByte()

	if err != nil {
		return nil, fmt.Errorf("failed to read first byte of ziplist entry: %w", err)
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

	header, err := r.ReadByte()

	if err != nil {
		return nil, err
	}

	switch header >> 6 {
	case 0:
		return readStringByLength(r, int(header&0x3f))
	case 1:
		next, err := r.ReadByte()

		if err != nil {
			return nil, err
		}

		return readStringByLength(r, int(header&0x3f)<<8|int(next))
	case 2:
		length, err := readUint32BE(r)

		if err != nil {
			return nil, err
		}

		return readStringByLength(r, int(length))
	}

	switch header >> 4 {
	case 12:
		return readInt16(r)
	case 13:
		return readInt32(r)
	case 14:
		return readInt64(r)
	}

	switch header {
	case 240:
		return read24BitSignedNumber(r)
	case 254:
		return readInt8(r)
	}

	if header >= 241 && header <= 253 {
		return header - 241, nil
	}

	return nil, ZipListHeaderError{Header: header}
}
