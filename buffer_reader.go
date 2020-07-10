package rdb

import (
	"io"
)

const defaultBufferSize = 4096

type byteReader interface {
	ReadBytes(n int) ([]byte, error)
}

type byteSliceReader struct {
	data   []byte
	offset int
}

func newByteSliceReader(data []byte) *byteSliceReader {
	return &byteSliceReader{
		data: data,
	}
}

func (b *byteSliceReader) ReadBytes(n int) ([]byte, error) {
	offset := b.offset
	b.offset += n
	return b.data[offset : offset+n], nil
}

type bufferReader struct {
	r      io.Reader
	offset int
	length int
	buf    []byte
}

func newBufferReader(r io.Reader) *bufferReader {
	return &bufferReader{
		r:   r,
		buf: make([]byte, defaultBufferSize),
	}
}

func (b *bufferReader) ReadBytes(n int) ([]byte, error) {
	remaining := b.length - b.offset

	if n > cap(b.buf) {
		buf := make([]byte, n)
		copy(buf, b.buf[b.offset:b.length])

		b.length = 0
		b.offset = 0

		if _, err := io.ReadFull(b.r, buf[remaining:cap(buf)]); err != nil {
			return nil, err
		}

		return buf, nil
	}

	if remaining < n {
		// Move the remaining data to the front
		copy(b.buf, b.buf[b.offset:b.length])
		b.length -= b.offset
		b.offset = 0

		read, err := io.ReadAtLeast(b.r, b.buf[remaining:cap(b.buf)], n-remaining)

		if err != nil {
			return nil, err
		}

		b.length += read
	}

	offset := b.offset
	b.offset += n

	return b.buf[offset : offset+n], nil
}
