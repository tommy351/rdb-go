package rdb

type valueReader interface {
	ReadValue(r bufReader) (interface{}, error)
}

type stringValueReader struct{}

func (stringValueReader) ReadValue(r bufReader) (interface{}, error) {
	return readString(r)
}
