package rdb

import (
	"bytes"
	"fmt"
	"io"
	"strconv"

	lzf "github.com/zhuyie/golzf"
)

func newStringReader(r bufReader) (bufReader, error) {
	length, encoded, err := readLengthWithEncoding(r)

	if err != nil {
		return nil, err
	}

	if !encoded {
		return newLimitedBufReader(r, length), nil
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
