package rdb

import (
	"bufio"
	"fmt"
	"io"
)

type seqIterator struct {
	DataKey     DataKey
	Reader      *bufio.Reader
	ValueReader valueReader
	Mapper      collectionMapper

	index       int
	length      int
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
			return nil, fmt.Errorf("failed to read seq length: %w", err)
		}

		s.initialized = true
		s.length = length
		s.values = make([]interface{}, length)

		head, err := s.Mapper.MapHead(&collectionHead{
			DataKey: s.DataKey,
			Length:  length,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to map head in seq: %w", err)
		}

		return head, nil
	}

	if s.length == s.index {
		s.done = true

		slice, err := s.Mapper.MapSlice(&collectionSlice{
			DataKey: s.DataKey,
			Value:   s.values,
		})

		if err != nil {
			return nil, fmt.Errorf("failed to map slice in seq: %w", err)
		}

		return slice, nil
	}

	value, err := s.ValueReader.ReadValue(s.Reader)

	if err != nil {
		return nil, fmt.Errorf("failed to read value from seq: %w", err)
	}

	entry, err := s.Mapper.MapEntry(&collectionEntry{
		DataKey: s.DataKey,
		Index:   s.index,
		Length:  s.length,
		Value:   value,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to map entry in seq: %w", err)
	}

	s.values[s.index] = value
	s.index++

	return entry, nil
}
