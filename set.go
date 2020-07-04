package rdb

type SetHead struct {
	DataKey
	Length int64
}

type SetElement struct {
	DataKey
	Index  int64
	Length int64
	Value  interface{}
}

type SetData struct {
	DataKey
	Value []interface{}
}

var _ collectionMapper = setMapper{}

type setMapper struct{}

func (setMapper) MapHead(head *collectionHead) (interface{}, error) {
	return &SetHead{
		DataKey: head.DataKey,
		Length:  head.Length,
	}, nil
}

func (setMapper) MapElement(element *collectionElement) (interface{}, error) {
	return &SetElement{
		DataKey: element.DataKey,
		Index:   element.Index,
		Length:  element.Length,
		Value:   element.Value,
	}, nil
}

func (setMapper) MapSlice(slice *collectionSlice) (interface{}, error) {
	return &SetData{
		DataKey: slice.DataKey,
		Value:   slice.Value,
	}, nil
}
