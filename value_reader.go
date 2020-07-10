package rdb

type valueReader interface {
	ReadValue(r byteReader) (interface{}, error)
}

type stringValueReader struct{}

func (stringValueReader) ReadValue(r byteReader) (interface{}, error) {
	return readString(r)
}
