package reader

import (
	"encoding/binary"
	"math"
)

type BytesReader interface {
	ReadBytes(n int) ([]byte, error)
}

func ReadUint8(r BytesReader) (uint8, error) {
	buf, err := r.ReadBytes(1)

	if err != nil {
		return 0, err
	}

	return buf[0], nil
}

func ReadUint16(r BytesReader) (uint16, error) {
	buf, err := r.ReadBytes(2)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint16(buf), nil
}

func ReadUint32(r BytesReader) (uint32, error) {
	buf, err := r.ReadBytes(4)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(buf), nil
}

func ReadUint64(r BytesReader) (uint64, error) {
	buf, err := r.ReadBytes(8)

	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(buf), nil
}

func ReadUint32BE(r BytesReader) (uint32, error) {
	buf, err := r.ReadBytes(4)

	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf), nil
}

func ReadUint64BE(r BytesReader) (uint64, error) {
	buf, err := r.ReadBytes(8)

	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buf), nil
}

func ReadInt8(r BytesReader) (int8, error) {
	v, err := ReadUint8(r)

	if err != nil {
		return 0, err
	}

	return int8(v), nil
}

func ReadInt16(r BytesReader) (int16, error) {
	v, err := ReadUint16(r)

	if err != nil {
		return 0, err
	}

	return int16(v), nil
}

func ReadInt32(r BytesReader) (int32, error) {
	v, err := ReadUint32(r)

	if err != nil {
		return 0, err
	}

	return int32(v), nil
}

func ReadInt64(r BytesReader) (int64, error) {
	v, err := ReadUint64(r)

	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func ReadFloat64(r BytesReader) (float64, error) {
	v, err := ReadUint64(r)

	if err != nil {
		return 0, err
	}

	return math.Float64frombits(v), nil
}

func ReadString(r BytesReader, n int) (string, error) {
	buf, err := r.ReadBytes(n)

	if err != nil {
		return "", err
	}

	return string(buf), nil
}
