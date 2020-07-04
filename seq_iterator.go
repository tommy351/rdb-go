package rdb

import "io"

type seqIterator struct {
	DataKey     DataKey
	Reader      io.Reader
	ValueReader valueReader
	Mapper      collectionMapper

	index       int64
	length      int64
	values      []interface{}
	initialized bool
	done        bool
}

func (s *seqIterator) Next() (interface{}, error) {
	if s.done {
		return nil, io.EOF
	}

	if !s.initialized {
		length, err := readLength(s.Reader)

		if err != nil {
			return nil, err
		}

		s.initialized = true
		s.length = length

		return s.Mapper.MapHead(&collectionHead{
			DataKey: s.DataKey,
			Length:  length,
		})
	}

	if s.length == s.index {
		s.done = true
		return s.Mapper.MapSlice(&collectionSlice{
			DataKey: s.DataKey,
			Value:   s.values,
		})
	}

	value, err := s.ValueReader.ReadValue(s.Reader)

	if err != nil {
		return nil, err
	}

	element, err := s.Mapper.MapEntry(&collectionEntry{
		DataKey: s.DataKey,
		Index:   s.index,
		Length:  s.length,
		Value:   value,
	})

	if err != nil {
		return nil, err
	}

	s.index++
	s.values = append(s.values, value)

	return element, nil
}
