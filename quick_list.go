package rdb

import (
	"io"
)

type quickListIterator struct {
	DataKey     DataKey
	Reader      io.Reader
	ValueReader valueReader
	Mapper      collectionMapper

	index       int64
	length      int64
	initialized bool
	done        bool
	values      []interface{}
	iterator    iterator
}

func (q *quickListIterator) Next() (interface{}, error) {
	if q.done {
		return nil, io.EOF
	}

	if !q.initialized {
		length, err := readLength(q.Reader)

		if err != nil {
			return nil, err
		}

		q.initialized = true
		q.length = length

		return q.Mapper.MapHead(&collectionHead{
			DataKey: q.DataKey,
			Length:  length,
		})
	}

	if q.index == q.length {
		q.done = true

		return q.Mapper.MapSlice(&collectionSlice{
			DataKey: q.DataKey,
			Value:   q.values,
		})
	}

	if q.iterator == nil {
		q.iterator = &zipListIterator{
			DataKey:     q.DataKey,
			Reader:      q.Reader,
			ValueReader: q.ValueReader,
			Mapper:      q,
		}
	}

	return q.iterator.Next()
}

func (q *quickListIterator) MapHead(head *collectionHead) (interface{}, error) {
	return nil, nil
}

func (q *quickListIterator) MapEntry(entry *collectionEntry) (interface{}, error) {
	q.values = append(q.values, entry.Value)

	return entry.Value, nil
}

func (q *quickListIterator) MapSlice(slice *collectionSlice) (interface{}, error) {
	q.index++
	q.iterator = nil

	return nil, nil
}