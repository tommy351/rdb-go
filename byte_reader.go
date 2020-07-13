package rdb

import (
	"io"
)

const (
	maxBufferSize = 4096
	minReadSize   = 512
)

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
		r: r,
	}
}

func (b *bufferReader) ReadBytes(n int) ([]byte, error) {
	if n > maxBufferSize {
		return b.readIntoNewBuffer(n)
	}

	if b.remaining() < n {
		if err := b.fill(n); err != nil {
			return nil, err
		}
	}

	offset := b.offset
	b.offset += n

	return b.buf[offset : offset+n], nil
}

func (b *bufferReader) remaining() int {
	return b.length - b.offset
}

func (b *bufferReader) readIntoNewBuffer(n int) ([]byte, error) {
	// Allocate a new buffer for the result
	buf := make([]byte, n)

	// Copy the remaining data to the result
	copied := copy(buf, b.buf[b.offset:b.length])

	// Reset the length and the offset
	b.offset = 0
	b.length = 0

	// Read the data into the result
	if _, err := io.ReadFull(b.r, buf[copied:]); err != nil {
		return nil, err
	}

	return buf, nil
}

func (b *bufferReader) fill(n int) error {
	remaining := b.remaining()
	minRead := n - remaining
	readSize := max(minRead, minReadSize)
	minCap := remaining + readSize

	// If the buffer capacity is not enough for reading
	if b.length+readSize > cap(b.buf) {
		if minCap <= cap(b.buf) {
			// Move the remaining data to the front if the buffer is enough
			copy(b.buf, b.buf[b.offset:b.length])
		} else {
			// Otherwise, allocate a bigger buffer
			bufSize := max(cap(b.buf), minReadSize)

			for bufSize < minCap && bufSize < maxBufferSize {
				bufSize *= 2
			}

			buf := make([]byte, bufSize)
			copy(buf, b.buf[b.offset:b.length])
			b.buf = buf
		}

		b.length -= b.offset
		b.offset = 0
	}

	// Read the buffer to its capacity
	read, err := io.ReadAtLeast(b.r, b.buf[b.length:], minRead)

	if err != nil {
		return err
	}

	b.length += read
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
