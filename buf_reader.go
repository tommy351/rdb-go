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
	r := bytes.NewReader([]byte(s))

	if r.Len() > defaultBufSize {
		return newBufReader(r)
	}

	return bufio.NewReaderSize(r, r.Len())
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
	if l.n <= 0 {
		return nil, io.EOF
	}

	if n > l.n {
		return l.r.Peek(l.n)
	}

	return l.r.Peek(n)
}

func (l *limitedBufReader) Discard(n int) (discarded int, err error) {
	if l.n <= 0 {
		return 0, io.EOF
	}

	if n > l.n {
		discarded, err = l.r.Discard(l.n)
	} else {
		discarded, err = l.r.Discard(n)
	}

	l.n -= discarded
	return
}

func (l *limitedBufReader) ReadByte() (byte, error) {
	if l.n <= 0 {
		return 0, io.EOF
	}

	b, err := l.r.ReadByte()
	l.n--
	return b, err
}
