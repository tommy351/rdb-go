package rdb

import (
	"bufio"
	"bytes"
	"io"
)

const defaultBufSize = 4096

type bufReader interface {
	io.Reader
	Peek(n int) ([]byte, error)
	Discard(n int) (int, error)
	ReadByte() (byte, error)
}

func newBufReader(r io.Reader) bufReader {
	return bufio.NewReaderSize(r, defaultBufSize)
}

func newBufReaderFromString(s string) bufReader {
	return newBufReader(bytes.NewReader([]byte(s)))
}

type limitedBufReader struct {
	r bufReader
	n int
}

func newLimitedBufReader(r bufReader, n int) *limitedBufReader {
	return &limitedBufReader{
		r: r,
		n: n,
	}
}

func (l *limitedBufReader) Read(buf []byte) (int, error) {
	if l.n <= 0 {
		return 0, io.EOF
	}

	if len(buf) > l.n {
		buf = buf[0:l.n]
	}

	n, err := l.r.Read(buf)
	l.n -= n
	return n, err
}

func (l *limitedBufReader) Peek(n int) ([]byte, error) {
	return l.r.Peek(n)
}

func (l *limitedBufReader) Discard(n int) (int, error) {
	discarded, err := l.r.Discard(n)
	l.n -= discarded
	return discarded, err
}

func (l *limitedBufReader) ReadByte() (byte, error) {
	b, err := l.r.ReadByte()
	l.n--
	return b, err
}
