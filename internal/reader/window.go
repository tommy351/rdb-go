package reader

import (
	"io"
	"io/ioutil"
)

const (
	newBufSize  = 4096
	minReadSize = newBufSize >> 2
)

// Window reads data into a reusable byte slice.
//
// Based on: https://github.com/pkg/json/blob/319c2b1/reader.go
type Window struct {
	r      io.Reader
	offset int
	buf    []byte
}

// NewWindow returns a new Window to read from r.
func NewWindow(r io.Reader) *Window {
	return &Window{
		r: r,
	}
}

func (w *Window) remaining() int {
	return len(w.buf) - w.offset
}

func (w *Window) peek(n int) ([]byte, error) {
	if w.remaining() < n {
		if err := w.fill(n); err != nil {
			return nil, err
		}
	}

	return w.buf[w.offset : w.offset+n], nil
}

func (w *Window) fill(n int) error {
	remaining := w.remaining()
	min := n - remaining
	readSize := max(minReadSize, min)

	switch {
	case cap(w.buf)-len(w.buf) >= readSize:
		// Do nothing, the buffer is enough for reading more data
	case cap(w.buf)-remaining >= readSize:
		// The buffer is enough if we move the buffer to the front
		w.compact()
	default:
		// Otherwise, extend the buffer
		w.extend(readSize + remaining)
	}

	remaining += w.offset

	// Fill the buffer to its capacity
	_, err := io.ReadAtLeast(w.r, w.buf[remaining:cap(w.buf)], min)

	return err
}

func (w *Window) discard(n int) {
	w.offset += n
}

func (w *Window) compact() {
	copy(w.buf, w.buf[w.offset:])
	w.offset = 0
}

func (w *Window) extend(n int) {
	buf := make([]byte, max(newBufSize, n))
	copy(buf, w.buf[w.offset:])
	w.buf = buf
	w.offset = 0
}

func (w *Window) ReadBytes(n int) ([]byte, error) {
	buf, err := w.peek(n)

	if err != nil {
		return nil, err
	}

	w.discard(len(buf))

	return buf, nil
}

func (w *Window) SkipBytes(n int) error {
	remaining := w.remaining()

	if n < remaining {
		w.discard(n)
		return nil
	}

	w.discard(remaining)

	if _, err := io.CopyN(ioutil.Discard, w.r, int64(n-remaining)); err != nil {
		return err
	}

	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
