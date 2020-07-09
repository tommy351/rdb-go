package reader

import "io"

type Buffer struct {
	offset int
	data   []byte
}

func NewBuffer(data []byte) *Buffer {
	return &Buffer{
		data: data,
	}
}

func (b *Buffer) ReadBytes(n int) ([]byte, error) {
	offset := b.offset
	remaining := len(b.data) - offset

	if remaining < n {
		return nil, io.ErrUnexpectedEOF
	}

	b.offset = offset + n

	return b.data[offset : offset+n], nil
}
