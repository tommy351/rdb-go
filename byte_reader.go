package rdb

import (
	"io"
)

const defaultBufferSize = 4096

type byteReader interface {
	ReadBytes(n int) ([]byte, error)
}

type sliceReader struct {
	data   []byte
	offset int
}

func newSliceReader(data []byte) *sliceReader {
	return &sliceReader{
		data: data,
	}
}

func (b *sliceReader) ReadBytes(n int) ([]byte, error) {
	offset := b.offset
	remaining := len(b.data) - offset

	if remaining <= 0 {
		return nil, io.EOF
	}

	if remaining < n {
		n = remaining
	}

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
		// Allocate a new []byte for the result
		buf := make([]byte, n)

		// Copy the remaining data to the result
		copy(buf, b.buf[b.offset:b.length])

		// Reset the length and the offset because the buffer are copied to the result
		b.length = 0
		b.offset = 0

		// Read the data into the result
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

		// Read the buffer to its capacity
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
