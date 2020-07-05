package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"time"

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
		compressedLen, err := readLength(r)

		if err != nil {
			return "", err
		}

		decompressedLen, err := readLength(r)

		if err != nil {
			return "", err
		}

		compressedBuf := make([]byte, compressedLen)

		if _, err := io.ReadFull(r, compressedBuf); err != nil {
			return "", err
		}

		decompressedBuf := make([]byte, decompressedLen)

		if _, err := lzf.Decompress(compressedBuf, decompressedBuf); err != nil {
			return "", err
		}

		return string(decompressedBuf), nil
	}

	return "", StringEncodingError{Encoding: length}
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

	return int(binary.LittleEndian.Uint32(append([]byte{0}, buf...))) >> 8, nil
}

func convertToFloat64(value interface{}) (float64, error) {
	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	case reflect.String:
		return strconv.ParseFloat(v.String(), 64)
	}

	return 0, ConvertError{Value: value, Type: "float64"}
}

func convertToString(value interface{}) (string, error) {
	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.String:
		return v.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64), nil
	}

	return "", ConvertError{Value: value, Type: "string"}
}
