package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/tommy351/rdb-go/internal/convert"
	lzf "github.com/zhuyie/golzf"
)

func readByte(r io.Reader) (byte, error) {
	buf := make([]byte, 1)

	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}

	return buf[0], nil
}

func readUint16(r io.Reader) (value uint16, err error) {
	err = binary.Read(r, binary.LittleEndian, &value)
	return
}

func readUint32(r io.Reader) (value uint32, err error) {
	err = binary.Read(r, binary.LittleEndian, &value)
	return
}

func readUint64(r io.Reader) (value uint64, err error) {
	err = binary.Read(r, binary.LittleEndian, &value)
	return
}

func readUint32BE(r io.Reader) (value uint32, err error) {
	err = binary.Read(r, binary.BigEndian, &value)
	return
}

func readUint64BE(r io.Reader) (value uint64, err error) {
	err = binary.Read(r, binary.BigEndian, &value)
	return
}

func readInt8(r io.Reader) (value int8, err error) {
	err = binary.Read(r, binary.LittleEndian, &value)
	return
}

func readInt16(r io.Reader) (value int16, err error) {
	err = binary.Read(r, binary.LittleEndian, &value)
	return
}

func readInt32(r io.Reader) (value int32, err error) {
	err = binary.Read(r, binary.LittleEndian, &value)
	return
}

func readInt64(r io.Reader) (value int64, err error) {
	err = binary.Read(r, binary.LittleEndian, &value)
	return
}

func readMillisecondsTime(r io.Reader) (*time.Time, error) {
	value, err := readUint64(r)

	if err != nil {
		return nil, err
	}

	return timePtr(time.Unix(0, int64(value)*int64(time.Millisecond)).UTC()), nil
}

func readSecondsTime(r io.Reader) (*time.Time, error) {
	value, err := readUint32(r)

	if err != nil {
		return nil, err
	}

	return timePtr(time.Unix(int64(value), 0).UTC()), nil
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func readBytes(r io.Reader, length int64) ([]byte, error) {
	buf := make([]byte, length)

	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("failed to read bytes by length %d: %w", length, err)
	}

	return buf, nil
}

func readStringByLength(r io.Reader, length int64) (string, error) {
	buf, err := readBytes(r, length)

	if err != nil {
		return "", err
	}

	return convert.BytesToString(buf), nil
}

func readLengthWithEncoding(r io.Reader) (int64, bool, error) {
	first, err := readByte(r)

	if err != nil {
		return 0, false, err
	}

	enc := (first & 0xc0) >> 6
	data := int64(first & 0x3f)

	switch enc {
	case len6Bit:
		return data, false, nil

	case len14Bit:
		next, err := readByte(r)

		if err != nil {
			return 0, false, nil
		}

		return (data << 8) | int64(next), false, nil

	case lenEncVal:
		return data, true, nil
	}

	switch first {
	case len32Bit:
		value, err := readUint32BE(r)

		if err != nil {
			return 0, false, err
		}

		return int64(value), false, nil

	case len64Bit:
		value, err := readUint64BE(r)

		if err != nil {
			return 0, false, err
		}

		return int64(value), false, nil
	}

	return 0, false, LengthEncodingError{Encoding: enc}
}

func readLength(r io.Reader) (int64, error) {
	length, _, err := readLengthWithEncoding(r)
	return length, err
}

func readStringEncoding(r io.Reader) ([]byte, error) {
	length, encoded, err := readLengthWithEncoding(r)

	if err != nil {
		return nil, err
	}

	if !encoded {
		return readBytes(r, length)
	}

	switch length {
	case encInt8:
		value, err := readInt8(r)

		if err != nil {
			return nil, err
		}

		return []byte(strconv.FormatInt(int64(value), 10)), nil

	case encInt16:
		value, err := readInt16(r)

		if err != nil {
			return nil, err
		}

		return []byte(strconv.FormatInt(int64(value), 10)), nil

	case encInt32:
		value, err := readInt32(r)

		if err != nil {
			return nil, err
		}

		return []byte(strconv.FormatInt(int64(value), 10)), nil

	case encLZF:
		return readLZF(r)
	}

	return nil, StringEncodingError{Encoding: length}
}

func readLZF(r io.Reader) ([]byte, error) {
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

func readString(r io.Reader) (string, error) {
	buf, err := readStringEncoding(r)

	if err != nil {
		return "", err
	}

	return convert.BytesToString(buf), nil
}

func readBinaryDouble(r io.Reader) (value float64, err error) {
	err = binary.Read(r, binary.LittleEndian, &value)
	return
}

func readFloat(r io.Reader) (float64, error) {
	length, err := readByte(r)

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

	s, err := readStringByLength(r, int64(length))

	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(s, 64)
}

func read24BitSignedNumber(r io.Reader) (int, error) {
	buf := make([]byte, 3)

	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}

	return int(int32(buf[2])<<24|int32(buf[1])<<16|int32(buf[0])<<8) >> 8, nil
}
