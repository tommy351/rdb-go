package rdb

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/tommy351/rdb-go/internal/reader"
	lzf "github.com/zhuyie/golzf"
)

func readMillisecondsTime(r reader.BytesReader) (*time.Time, error) {
	value, err := reader.ReadUint64(r)

	if err != nil {
		return nil, err
	}

	return timePtr(time.Unix(0, int64(value)*int64(time.Millisecond)).UTC()), nil
}

func readSecondsTime(r reader.BytesReader) (*time.Time, error) {
	value, err := reader.ReadUint32(r)

	if err != nil {
		return nil, err
	}

	return timePtr(time.Unix(int64(value), 0).UTC()), nil
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func readLengthWithEncoding(r reader.BytesReader) (int, bool, error) {
	first, err := reader.ReadUint8(r)

	if err != nil {
		return 0, false, err
	}

	enc := (first & 0xc0) >> 6
	data := int(first & 0x3f)

	switch enc {
	case len6Bit:
		return data, false, nil

	case len14Bit:
		next, err := reader.ReadUint8(r)

		if err != nil {
			return 0, false, nil
		}

		return (data << 8) | int(next), false, nil

	case lenEncVal:
		return data, true, nil
	}

	switch first {
	case len32Bit:
		value, err := reader.ReadUint32BE(r)

		if err != nil {
			return 0, false, err
		}

		return int(value), false, nil

	case len64Bit:
		value, err := reader.ReadUint64BE(r)

		if err != nil {
			return 0, false, err
		}

		return int(value), false, nil
	}

	return 0, false, LengthEncodingError{Encoding: enc}
}

func readLength(r reader.BytesReader) (int, error) {
	length, _, err := readLengthWithEncoding(r)
	return length, err
}

func readStringEncoding(r reader.BytesReader) ([]byte, error) {
	length, encoded, err := readLengthWithEncoding(r)

	if err != nil {
		return nil, err
	}

	if !encoded {
		return r.ReadBytes(length)
	}

	switch length {
	case encInt8:
		value, err := reader.ReadInt8(r)

		if err != nil {
			return nil, err
		}

		return []byte(strconv.FormatInt(int64(value), 10)), nil

	case encInt16:
		value, err := reader.ReadInt16(r)

		if err != nil {
			return nil, err
		}

		return []byte(strconv.FormatInt(int64(value), 10)), nil

	case encInt32:
		value, err := reader.ReadInt32(r)

		if err != nil {
			return nil, err
		}

		return []byte(strconv.FormatInt(int64(value), 10)), nil

	case encLZF:
		return readLZF(r)
	}

	return nil, StringEncodingError{Encoding: length}
}

func readLZF(r reader.BytesReader) ([]byte, error) {
	compressedLen, err := readLength(r)

	if err != nil {
		return nil, err
	}

	decompressedLen, err := readLength(r)

	if err != nil {
		return nil, err
	}

	compressedBuf, err := r.ReadBytes(compressedLen)

	if err != nil {
		return nil, err
	}

	decompressedBuf := make([]byte, decompressedLen)

	if _, err := lzf.Decompress(compressedBuf, decompressedBuf); err != nil {
		return nil, fmt.Errorf("failed to decompress LZF: %w", err)
	}

	return decompressedBuf, nil
}

func readString(r reader.BytesReader) (string, error) {
	buf, err := readStringEncoding(r)

	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func readFloat(r reader.BytesReader) (float64, error) {
	length, err := reader.ReadUint8(r)

	if err != nil {
		return 0, err
	}

	switch length {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(1), nil
	case 255:
		return math.Inf(-1), nil
	}

	s, err := reader.ReadString(r, int(length))

	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(s, 64)
}

func read24BitSignedNumber(r reader.BytesReader) (int, error) {
	buf, err := r.ReadBytes(3)

	if err != nil {
		return 0, err
	}

	return int(int32(buf[2])<<24|int32(buf[1])<<16|int32(buf[0])<<8) >> 8, nil
}

func skipString(r reader.BytesReader) error {
	length, encoded, err := readLengthWithEncoding(r)

	if err != nil {
		return fmt.Errorf("failed to read length: %w", err)
	}

	if !encoded {
		return r.SkipBytes(length)
	}

	switch length {
	case encInt8:
		return r.SkipBytes(1)
	case encInt16:
		return r.SkipBytes(2)
	case encInt32:
		return r.SkipBytes(4)
	case encLZF:
		// Read compressed length
		cLength, err := readLength(r)

		if err != nil {
			return err
		}

		// Read decompressed length
		if _, err := readLength(r); err != nil {
			return err
		}

		return r.SkipBytes(cLength)
	}

	return StringEncodingError{Encoding: length}
}

func skipBinaryDouble(r reader.BytesReader) error {
	return r.SkipBytes(8)
}

func skipFloat(r reader.BytesReader) error {
	length, err := reader.ReadUint8(r)

	if err != nil {
		return err
	}

	if length < 253 {
		return r.SkipBytes(int(length))
	}

	return nil
}
