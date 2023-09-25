package rdb

import "time"

type iterator interface {
	Next() (interface{}, error)
}

type collectionHead struct {
	DataKey DataKey
	Length  int
}

type collectionEntry struct {
	DataKey DataKey
	Index   int
	Length  int
	Value   interface{}
}

type collectionSlice struct {
	DataKey DataKey
	Value   []interface{}
}

type collectionMapper interface {
	MapHead(*collectionHead) (interface{}, error)
	MapEntry(*collectionEntry) (interface{}, error)
	MapSlice(*collectionSlice) (interface{}, error)
}

type Aux struct {
	Key   string
	Value string
}

type DatabaseSize struct {
	Size   int
	Expire int
}

// DataKey contains the database, the key and the expiry of data.
type DataKey struct {
	Database int
	Key      string
	Expiry   *time.Time
}

// Expired returns true if the key is expired.
func (d DataKey) Expired() bool {
	if d.Expiry == nil {
		return false
	}

	return time.Now().After(*d.Expiry)
}

// StringData contains the key and the value of string data.
type StringData struct {
	DataKey
	Value string
}

// BloomFilter represents a bloom filter data structure implemented by RedisBloom.
// At present, It only contains data key, but does not store the actual data values.
type BloomFilter struct {
	DataKey
}

// CuckooFilter represents a cuckoo filter data structure implemented by RedisBloom.
// At present, It only contains data key, but does not store the actual data values.
type CuckooFilter struct {
	DataKey
}
