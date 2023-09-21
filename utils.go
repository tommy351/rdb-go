package rdb

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"time"

	lzf "github.com/zhuyie/golzf"
)

func readByte(r byteReader) (byte, error) {
	buf, err := r.ReadBytes(1)
	if err != nil {
		return 0, fmt.Errorf("readByte error: %w", err)
	}

	return buf[0], nil
}

func readUint16(r byteReader) (uint16, error) {
	buf, err := r.ReadBytes(2)
	if err != nil {
		return 0, fmt.Errorf("readUint16 error: %w", err)
	}

	return binary.LittleEndian.Uint16(buf), nil
}

func readUint32(r byteReader) (uint32, error) {
	buf, err := r.ReadBytes(4)
	if err != nil {
		return 0, fmt.Errorf("readUint32 error: %w", err)
	}

	return binary.LittleEndian.Uint32(buf), nil
}

func readUint64(r byteReader) (uint64, error) {
	buf, err := r.ReadBytes(8)
	if err != nil {
		return 0, fmt.Errorf("readUint64 error: %w", err)
	}

	return binary.LittleEndian.Uint64(buf), nil
}

func readUint32BE(r byteReader) (uint32, error) {
	buf, err := r.ReadBytes(4)
	if err != nil {
		return 0, fmt.Errorf("readUint32BE error: %w", err)
	}

	return binary.BigEndian.Uint32(buf), nil
}

func readUint64BE(r byteReader) (uint64, error) {
	buf, err := r.ReadBytes(8)
	if err != nil {
		return 0, fmt.Errorf("readUint64BE error: %w", err)
	}

	return binary.BigEndian.Uint64(buf), nil
}

func readInt8(r byteReader) (int8, error) {
	v, err := readByte(r)
	if err != nil {
		return 0, fmt.Errorf("readInt8 error: %w", err)
	}

	return int8(v), nil
}

func readInt16(r byteReader) (int16, error) {
	v, err := readUint16(r)
	if err != nil {
		return 0, fmt.Errorf("readInt16 error: %w", err)
	}

	return int16(v), nil
}

func readInt32(r byteReader) (int32, error) {
	v, err := readUint32(r)
	if err != nil {
		return 0, fmt.Errorf("readInt32 error: %w", err)
	}

	return int32(v), nil
}

func readInt64(r byteReader) (int64, error) {
	v, err := readUint64(r)
	if err != nil {
		return 0, fmt.Errorf("readInt64 error: %w", err)
	}

	return int64(v), nil
}

func readMillisecondsTime(r byteReader) (*time.Time, error) {
	value, err := readUint64(r)
	if err != nil {
		return nil, fmt.Errorf("readMillisecondsTime error: %w", err)
	}

	return timePtr(time.Unix(0, int64(value)*int64(time.Millisecond)).UTC()), nil
}

func readSecondsTime(r byteReader) (*time.Time, error) {
	value, err := readUint32(r)
	if err != nil {
		return nil, fmt.Errorf("readSecondsTime error: %w", err)
	}

	return timePtr(time.Unix(int64(value), 0).UTC()), nil
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func readStringByLength(r byteReader, length int) (string, error) {
	buf, err := r.ReadBytes(length)
	if err != nil {
		return "", fmt.Errorf("readStringByLength error (length=%d): %w", length, err)
	}

	return string(buf), nil
}

func readLengthWithEncoding(r byteReader) (int, bool, error) {
	first, err := readByte(r)
	if err != nil {
		return 0, false, fmt.Errorf("readLengthWithEncoding error: %w", err)
	}

	enc := (first & 0xc0) >> 6
	data := int(first & 0x3f)

	switch enc {
	case len6Bit:
		return data, false, nil

	case len14Bit:
		next, err := readByte(r)
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

func readLength(r byteReader) (int, error) {
	length, _, err := readLengthWithEncoding(r)

	return length, err
}

func readStringEncoding(r byteReader) ([]byte, error) {
	length, encoded, err := readLengthWithEncoding(r)
	if err != nil {
		return nil, err
	}

	if !encoded {
		return r.ReadBytes(length)
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

func readLZF(r byteReader) ([]byte, error) {
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
		return nil, fmt.Errorf("failed to read compressed bytes: %w", err)
	}

	decompressedBuf := r.MakeByteSlice(decompressedLen)

	if _, err := lzf.Decompress(compressedBuf, decompressedBuf); err != nil {
		return nil, fmt.Errorf("failed to decompress LZF: %w", err)
	}

	return decompressedBuf, nil
}

func readString(r byteReader) (string, error) {
	buf, err := readStringEncoding(r)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func readBinaryDouble(r byteReader) (float64, error) {
	v, err := readUint64(r)
	if err != nil {
		return 0, err
	}

	return math.Float64frombits(v), err
}

func readFloat(r byteReader) (float64, error) {
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

	s, err := readStringByLength(r, int(length))
	if err != nil {
		return 0, err
	}

	return strconv.ParseFloat(s, 64)
}

func read24BitSignedNumber(r byteReader) (int, error) {
	buf, err := r.ReadBytes(3)
	if err != nil {
		return 0, fmt.Errorf("read24BitSignedNumber error: %w", err)
	}

	return int(int32(buf[2])<<24|int32(buf[1])<<16|int32(buf[0])<<8) >> 8, nil
}

func skipBytes(r byteReader, length int) error {
	// TODO
	if _, err := r.ReadBytes(length); err != nil {
		return fmt.Errorf("failed to skip %d bytes: %w", length, err)
	}

	return nil
}

func skipString(r byteReader) error {
	length, encoded, err := readLengthWithEncoding(r)
	if err != nil {
		return fmt.Errorf("failed to read length: %w", err)
	}

	if !encoded {
		return skipBytes(r, length)
	}

	switch length {
	case encInt8:
		return skipBytes(r, 1)
	case encInt16:
		return skipBytes(r, 2)
	case encInt32:
		return skipBytes(r, 4)
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

		return skipBytes(r, cLength)
	}

	return StringEncodingError{Encoding: length}
}

func skipBinaryDouble(r byteReader) error {
	return skipBytes(r, 8)
}

func skipFloat(r byteReader) error {
	length, err := readByte(r)
	if err != nil {
		return err
	}

	if length < 253 {
		return skipBytes(r, int(length))
	}

	return nil
}

func checkRdbModuleOpCode(r byteReader, expected int) error {
	val, err := readLength(r)
	if err != nil {
		return err
	}

	if val != expected {
		return fmt.Errorf("illegal rdbModuleOpcode %d, expect:%d", val, expected)
	}

	return nil
}

func redisModuleReadUnsigned(r byteReader) (int, error) {
	if err := checkRdbModuleOpCode(r, rdbModuleOpcodeUInt); err != nil {
		return 0, err
	}

	val, err := readLength(r)
	if err != nil {
		return 0, err
	}

	return val, nil
}

func redisModuleReadDouble(r byteReader) (uint64, error) {
	if err := checkRdbModuleOpCode(r, rdbModuleOpcodeDouble); err != nil {
		return 0, err
	}

	scoreBytes, err := readUint64(r)
	if err != nil {
		return 0, err
	}

	return scoreBytes, nil
}

func redisModuleReadStringBuffer(r byteReader) (string, error) {
	if err := checkRdbModuleOpCode(r, rdbModuleOpcodeString); err != nil {
		return "", err
	}

	value, err := readString(r)
	if err != nil {
		return "", err
	}

	return value, nil
}
