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

	if err := b.SkipBytes(n); err != nil {
		return nil, err
	}

	return b.data[offset : offset+n], nil
}

func (b *Buffer) SkipBytes(n int) error {
	remaining := len(b.data) - b.offset

	if remaining < n {
		return io.ErrUnexpectedEOF
	}

	b.offset += n

	return nil
}
