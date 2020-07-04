package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
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

	return timePtr(time.Unix(0, int64(value)*int64(time.Millisecond))), nil
}

func readSecondsTime(r io.Reader) (*time.Time, error) {
	value, err := readUint32(r)

	if err != nil {
		return nil, err
	}

	return timePtr(time.Unix(int64(value), 0)), nil
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func readStringByLength(r io.Reader, length int64) (string, error) {
	buf := make([]byte, length)

	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	return string(buf), nil
}

func readLengthWithEncoding(r io.Reader) (int64, bool, error) {
	first, err := readByte(r)

	if err != nil {
		return 0, false, err
	}

	enc := (first & 0xc0) >> 6
	data := int64(first & 0x3f)

	switch enc {
	case lenEncVal:
		return data, true, nil

	case len6Bit:
		return data, false, nil

	case len14Bit:
		next, err := readByte(r)

		if err != nil {
			return 0, false, nil
		}

		return (data << 8) | int64(next), false, nil
	}

	return 0, false, fmt.Errorf("invalid length encoding %d", enc)
}

func readLength(r io.Reader) (int64, error) {
	length, _, err := readLengthWithEncoding(r)
	return length, err
}

func readString(r io.Reader) (string, error) {
	length, encoded, err := readLengthWithEncoding(r)

	if err != nil {
		return "", err
	}

	if !encoded {
		return readStringByLength(r, length)
	}

	switch length {
	case encInt8:
		value, err := readByte(r)

		if err != nil {
			return "", err
		}

		return strconv.FormatInt(int64(value), 10), nil

	case encInt16:
		value, err := readInt16(r)

		if err != nil {
			return "", err
		}

		return strconv.FormatInt(int64(value), 10), nil

	case encInt32:
		value, err := readInt32(r)

		if err != nil {
			return "", err
		}

		return strconv.FormatInt(int64(value), 10), nil

	case encLZF:
		// TODO
	}

	return "", fmt.Errorf("invalid string encoding %d", length)
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

	return strconv.ParseFloat(s, 64)
}

func read24BitSignedNumber(r io.Reader) (int, error) {
	buf := make([]byte, 3)

	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}

	return int(binary.LittleEndian.Uint32(append([]byte{0}, buf...))) >> 8, nil
}
