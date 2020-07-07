package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/tommy351/rdb-go"
	"github.com/tommy351/rdb-go/internal/convert"
)

type JSONPrinter struct {
	db         int64
	writer     io.Writer
	keyIndex   int
	entryIndex int
}

func NewJSONPrinter(w io.Writer) *JSONPrinter {
	return &JSONPrinter{
		db:     -1,
		writer: w,
	}
}

func (j *JSONPrinter) print(args ...interface{}) error {
	_, err := fmt.Fprint(j.writer, args...)
	return err
}

func (j *JSONPrinter) printValue(value interface{}) error {
	buf, err := json.Marshal(value)

	if err != nil {
		return err
	}

	return j.print(convert.BytesToString(buf))
}

func (j *JSONPrinter) printKey(key *rdb.DataKey) error {
	if key.Database != j.db {
		if j.db >= 0 {
			if err := j.print("},"); err != nil {
				return err
			}
		}

		if err := j.print("{"); err != nil {
			return err
		}

		j.db = key.Database
		j.keyIndex = 0
	}

	if j.keyIndex > 0 {
		if err := j.print(","); err != nil {
			return err
		}
	}

	if err := j.printValue(key.Key); err != nil {
		return err
	}

	if err := j.print(":"); err != nil {
		return err
	}

	j.keyIndex++
	return nil
}

func (j *JSONPrinter) Start() error {
	return j.print("[")
}

func (j *JSONPrinter) End() error {
	if j.db >= 0 {
		if err := j.print("}"); err != nil {
			return err
		}
	}

	return j.print("]")
}

func (j *JSONPrinter) String(data *rdb.StringData) error {
	if err := j.printKey(&data.DataKey); err != nil {
		return err
	}

	return j.printValue(data.Value)
}

func (j *JSONPrinter) printArrayHead(key *rdb.DataKey) error {
	j.entryIndex = 0

	if err := j.printKey(key); err != nil {
		return err
	}

	return j.print("[")
}

func (j *JSONPrinter) printArrayEntry(value interface{}) error {
	if j.entryIndex > 0 {
		if err := j.print(","); err != nil {
			return err
		}
	}

	if err := j.printValue(value); err != nil {
		return err
	}

	j.entryIndex++
	return nil
}

func (j *JSONPrinter) printArrayEnd() error {
	return j.print("]")
}

func (j *JSONPrinter) printObjectHead(key *rdb.DataKey) error {
	j.entryIndex = 0

	if err := j.printKey(key); err != nil {
		return err
	}

	return j.print("{")
}

func (j *JSONPrinter) printObjectEntry(key string, value interface{}) error {
	if j.entryIndex > 0 {
		if err := j.print(","); err != nil {
			return err
		}
	}

	if err := j.printValue(key); err != nil {
		return err
	}

	if err := j.print(":"); err != nil {
		return err
	}

	if err := j.printValue(value); err != nil {
		return err
	}

	j.entryIndex++
	return nil
}

func (j *JSONPrinter) printObjectEnd() error {
	return j.print("}")
}

func (j *JSONPrinter) ListHead(head *rdb.ListHead) error {
	return j.printArrayHead(&head.DataKey)
}

func (j *JSONPrinter) ListEntry(entry *rdb.ListEntry) error {
	return j.printArrayEntry(entry.Value)
}

func (j *JSONPrinter) ListData(data *rdb.ListData) error {
	return j.print("]")
}

func (j *JSONPrinter) SetHead(head *rdb.SetHead) error {
	return j.printArrayHead(&head.DataKey)
}

func (j *JSONPrinter) SetEntry(entry *rdb.SetEntry) error {
	return j.printArrayEntry(entry.Value)
}

func (j *JSONPrinter) SetData(data *rdb.SetData) error {
	return j.printArrayEnd()
}

func (j *JSONPrinter) SortedSetHead(head *rdb.SortedSetHead) error {
	return j.printObjectHead(&head.DataKey)
}

func (j *JSONPrinter) SortedSetEntry(entry *rdb.SortedSetEntry) error {
	return j.printObjectEntry(entry.Value, entry.Score)
}

func (j *JSONPrinter) SortedSetData(data *rdb.SortedSetData) error {
	return j.printObjectEnd()
}

func (j *JSONPrinter) HashHead(head *rdb.HashHead) error {
	return j.printObjectHead(&head.DataKey)
}

func (j *JSONPrinter) HashEntry(entry *rdb.HashEntry) error {
	return j.printObjectEntry(entry.Index, entry.Value)
}

func (j *JSONPrinter) HashData(data *rdb.HashData) error {
	return j.printObjectEnd()
}
