package main

import "github.com/tommy351/rdb-go"

type Printer interface {
	Start() error
	End() error

	String(data *rdb.StringData) error

	ListHead(head *rdb.ListHead) error
	ListEntry(entry *rdb.ListEntry) error
	ListData(data *rdb.ListData) error

	SetHead(head *rdb.SetHead) error
	SetEntry(entry *rdb.SetEntry) error
	SetData(data *rdb.SetData) error

	SortedSetHead(head *rdb.SortedSetHead) error
	SortedSetEntry(entry *rdb.SortedSetEntry) error
	SortedSetData(data *rdb.SortedSetData) error

	HashHead(head *rdb.HashHead) error
	HashEntry(head *rdb.HashEntry) error
	HashData(data *rdb.HashData) error
}
