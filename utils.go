package rdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"strconv"
	"time"
)

const defaultBufSize = 4096

func newBufReader(r io.Reader) *bufio.Reader {
	return bufio.NewReaderSize(r, defaultBufSize)
}

func readUint16(r *bufio.Reader) (uint16, error) {
	buf, err := readBytes(r, 2)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint16(buf), nil
}

func readUint32(r *bufio.Reader) (uint32, error) {
	buf, err := readBytes(r, 4)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(buf), nil
}

func readUint64(r *bufio.Reader) (uint64, error) {
	buf, err := readBytes(r, 8)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(buf), nil
}

func readUint32BE(r *bufio.Reader) (uint32, error) {
	buf, err := readBytes(r, 4)

	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf), nil
}

func readUint64BE(r *bufio.Reader) (uint64, error) {
	buf, err := readBytes(r, 8)

	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buf), nil
}

func readInt8(r *bufio.Reader) (int8, error) {
	b, err := r.ReadByte()

	if err != nil {
		return 0, err
	}

	return int8(b), nil
}

func readInt16(r *bufio.Reader) (int16, error) {
	v, err := readUint16(r)

	if err != nil {
		return 0, err
	}

	return int16(v), nil
}

func readInt32(r *bufio.Reader) (int32, error) {
	v, err := readUint32(r)

	if err != nil {
		return 0, err
	}

	return int32(v), nil
}

func readInt64(r *bufio.Reader) (int64, error) {
	v, err := readUint64(r)

	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func readMillisecondsTime(r *bufio.Reader) (*time.Time, error) {
	value, err := readUint64(r)

	if err != nil {
		return nil, err
	}

	return timePtr(time.Unix(0, int64(value)*int64(time.Millisecond)).UTC()), nil
}

func readSecondsTime(r *bufio.Reader) (*time.Time, error) {
	value, err := readUint32(r)

	if err != nil {
		return nil, err
	}

	return timePtr(time.Unix(int64(value), 0).UTC()), nil
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func readBytes(r *bufio.Reader, n int) ([]byte, error) {
	if n > defaultBufSize {
		buf := make([]byte, n)

		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}

		return buf, nil
	}

	buf, err := r.Peek(n)

	if err != nil {
		return nil, fmt.Errorf("failed to read bytes by length %d: %w", n, err)
	}

	if _, err := r.Discard(len(buf)); err != nil {
		return nil, fmt.Errorf("failed to discard bytes by length %d: %w", len(buf), err)
	}

	return buf, nil
}

func readStringByLength(r *bufio.Reader, n int) (string, error) {
	buf, err := readBytes(r, n)

	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func readLengthWithEncoding(r *bufio.Reader) (int, bool, error) {
	first, err := r.ReadByte()

	if err != nil {
		return 0, false, err
	}

	enc := (first & 0xc0) >> 6
	data := int(first & 0x3f)

	switch enc {
	case len6Bit:
		return data, false, nil

	case len14Bit:
		next, err := r.ReadByte()

		if err != nil {
			return 0, false, nil
		}

		return (data << 8) | int(next), false, nil

	case lenEncVal:
		return data, true, nil
	}

	switch first {
	case len32Bit:
		value, err := readUint32BE(r)

		if err != nil {
			return 0, false, err
		}

		return int(value), false, nil

	case len64Bit:
		value, err := readUint64BE(r)

		if err != nil {
			return 0, false, err
		}

		return int(value), false, nil
	}

	return 0, false, LengthEncodingError{Encoding: enc}
}

func readLength(r *bufio.Reader) (int, error) {
	length, _, err := readLengthWithEncoding(r)
	return length, err
}

func readString(r *bufio.Reader) (string, error) {
	r, err := newStringReader(r)

	if err != nil {
		return "", err
	}

	buf, err := ioutil.ReadAll(r)

	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func readBinaryDouble(r *bufio.Reader) (float64, error) {
	v, err := readUint64(r)

	if err != nil {
		return 0, err
	}

	return math.Float64frombits(v), nil
}

func readFloat(r *bufio.Reader) (float64, error) {
	length, err := r.ReadByte()

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

	s, err := readStringByLength(r, int(length))

	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(s, 64)
}

func read24BitSignedNumber(r *bufio.Reader) (int, error) {
	buf, err := readBytes(r, 3)

	if err != nil {
		return 0, err
	}

	return int(int32(buf[2])<<24|int32(buf[1])<<16|int32(buf[0])<<8) >> 8, nil
}

func skipString(r *bufio.Reader) error {
	length, encoded, err := readLengthWithEncoding(r)

	if err != nil {
		return fmt.Errorf("failed to read length: %w", err)
	}

	if !encoded {
		_, err := r.Discard(length)
		return err
	}

	switch length {
	case encInt8:
		_, err := r.Discard(1)
		return err
	case encInt16:
		_, err := r.Discard(2)
		return err
	case encInt32:
		_, err := r.Discard(4)
		return err
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

		_, err = r.Discard(cLength)
		return err
	}

	return StringEncodingError{Encoding: length}
}

func skipBinaryDouble(r *bufio.Reader) error {
	_, err := r.Discard(8)
	return err
}

func skipFloat(r *bufio.Reader) error {
	length, err := r.ReadByte()

	if err != nil {
		return err
	}

	if length < 253 {
		if _, err := r.Discard(int(length)); err != nil {
			return err
		}
	}

	return nil
}
