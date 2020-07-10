package rdb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

	lzf "github.com/zhuyie/golzf"
)

func newBufReaderFromString(s string) *bufio.Reader {
	return newBufReader(bytes.NewReader([]byte(s)))
}

func newStringReader(r *bufio.Reader) (*bufio.Reader, error) {
	length, encoded, err := readLengthWithEncoding(r)

	if err != nil {
		return nil, err
	}

	if !encoded {
		return newBufReader(io.LimitReader(r, int64(length))), nil
	}

	switch length {
	case encInt8:
		value, err := readInt8(r)

		if err != nil {
			return nil, err
		}

		return newBufReaderFromString(strconv.FormatInt(int64(value), 10)), nil

	case encInt16:
		value, err := readInt16(r)

		if err != nil {
			return nil, err
		}

		return newBufReaderFromString(strconv.FormatInt(int64(value), 10)), nil

	case encInt32:
		value, err := readInt32(r)

		if err != nil {
			return nil, err
		}

		return newBufReaderFromString(strconv.FormatInt(int64(value), 10)), nil

	case encLZF:
		buf, err := readLZF(r)

		if err != nil {
			return nil, err
		}

		return newBufReader(bytes.NewReader(buf)), nil
	}

	return nil, StringEncodingError{Encoding: length}
}

func readLZF(r *bufio.Reader) ([]byte, error) {
	compressedLen, err := readLength(r)

	if err != nil {
		return nil, err
	}

	decompressedLen, err := readLength(r)

	if err != nil {
		return nil, err
	}

	compressedBuf := make([]byte, compressedLen)

	if _, err := io.ReadFull(r, compressedBuf); err != nil {
		return nil, err
	}

	decompressedBuf := make([]byte, decompressedLen)

	if _, err := lzf.Decompress(compressedBuf, decompressedBuf); err != nil {
		return nil, fmt.Errorf("failed to decompress LZF: %w", err)
	}

	return decompressedBuf, nil
}
