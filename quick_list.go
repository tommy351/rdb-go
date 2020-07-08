package rdb

import (
	"fmt"
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
			return nil, fmt.Errorf("failed to read quicklist buffer: %w", err)
		}

		q.initialized = true
		q.length = length

		head, err := q.Mapper.MapHead(&collectionHead{
			DataKey: q.DataKey,
			Length:  length,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to map head in quicklist: %w", err)
		}

		return head, nil
	}

	if q.index == q.length {
		q.done = true

		slice, err := q.Mapper.MapSlice(&collectionSlice{
			DataKey: q.DataKey,
			Value:   q.values,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to map slice in quicklist: %w", err)
		}

		return slice, nil
	}

	if q.iterator == nil {
		q.iterator = &zipListIterator{
			DataKey:     q.DataKey,
			Reader:      q.Reader,
			ValueReader: q.ValueReader,
			Mapper:      q,
			ValueLength: 1,
		}
	}

	return q.iterator.Next()
}

func (q *quickListIterator) MapHead(head *collectionHead) (interface{}, error) {
	return nil, errContinueLoop
}

func (q *quickListIterator) MapEntry(entry *collectionEntry) (interface{}, error) {
	mappedEntry, err := q.Mapper.MapEntry(entry)

	if err != nil {
		return nil, fmt.Errorf("failed to map entry in quicklist: %w", err)
	}

	q.values = append(q.values, entry.Value)

	return mappedEntry, nil
}

func (q *quickListIterator) MapSlice(slice *collectionSlice) (interface{}, error) {
	q.index++
	q.iterator = nil

	return nil, errContinueLoop
}
