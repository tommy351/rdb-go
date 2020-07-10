package rdb

import (
	"bytes"
	"fmt"
	"io"
	"strconv"

	lzf "github.com/zhuyie/golzf"
)

func newStringReader(r bufReader) (bufReader, int, error) {
	length, encoded, err := readLengthWithEncoding(r)

	if err != nil {
		return nil, 0, err
	}

	if !encoded {
		return newLimitedBufReader(r, length), length, nil
	}

	switch length {
	case encInt8:
		value, err := readInt8(r)

		if err != nil {
			return nil, 0, err
		}

		return newStringReaderFromInt(int64(value))

	case encInt16:
		value, err := readInt16(r)

		if err != nil {
			return nil, 0, err
		}

		return newStringReaderFromInt(int64(value))

	case encInt32:
		value, err := readInt32(r)

		if err != nil {
			return nil, 0, err
		}

		return newStringReaderFromInt(int64(value))

	case encLZF:
		buf, err := readLZF(r)

		if err != nil {
			return nil, 0, err
		}

		return newBufReader(bytes.NewReader(buf)), len(buf), nil
	}

	return nil, 0, StringEncodingError{Encoding: length}
}

func newStringReaderFromInt(value int64) (bufReader, int, error) {
	s := strconv.FormatInt(value, 10)
	return newBufReaderFromString(s), len(s), nil
}

func readLZF(r bufReader) ([]byte, error) {
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
