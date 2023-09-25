package rdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

const (
	len6Bit   = 0
	len14Bit  = 1
	len32Bit  = 0x80
	len64Bit  = 0x81
	lenEncVal = 3

	opCodeModuleAux    = 247
	opCodeIdle         = 248
	opCodeFreq         = 249
	opCodeAux          = 250
	opCodeResizeDB     = 251
	opCodeExpireTimeMS = 252
	opCodeExpireTime   = 253
	opCodeSelectDB     = 254
	opCodeEOF          = 255

	typeString          = 0
	typeList            = 1
	typeSet             = 2
	typeZSet            = 3
	typeHash            = 4
	typeZSet2           = 5
	typeModule          = 6
	typeModule2         = 7
	typeHashZipMap      = 9
	typeListZipList     = 10
	typeSetIntSet       = 11
	typeZSetZipList     = 12
	typeHashZipList     = 13
	typeListQuickList   = 14
	typeStreamListPacks = 15

	encInt8  = 0
	encInt16 = 1
	encInt32 = 2
	encLZF   = 3

	minVersion = 1
	maxVersion = 9

	rdbModuleOpcodeEOF    = 0
	rdbModuleOpcodeSInt   = 1
	rdbModuleOpcodeUInt   = 2
	rdbModuleOpcodeFloat  = 3
	rdbModuleOpcodeDouble = 4
	rdbModuleOpcodeString = 5

	// module ID is encoded using given module name
	// https://github.com/redis/redis/blob/24187ed8e396625cc44a6bbeeb87e01aec55c27d/src/module.c#L6599-L6615
	redisBloomBloomFilter    = 3465209449566631940 // MBbloom--
	redisBloomCuckooFilter   = 3465209449562641412 // MBbloomCF
	redisBloomTopK           = 5659418315958718464 // TopK-TYPE
	redisBloomTDigest        = 5490471757281169408 // TDIS-TYPE
	redisBloomCountMinSketch = 631811237999480832  // CMSk-TYPE
)

// nolint: gochecknoglobals
var (
	magicString     = []byte("REDIS")
	errContinueLoop = errors.New("continue loop")
)

// Parser parses a RDB dump file.
type Parser struct {
	KeyFilter func(key *DataKey) bool

	reader      byteReader
	initialized bool
	db          int
	expiry      *time.Time
	dataType    *byte
	key         string
	iterator    iterator
}

// NewParser returns a new Parser to read from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader: newBufferReader(r),
		db:     -1,
	}
}

// Next reads data from the reader until the next token and returns one of the
// following types:
//
//	*Aux
//	*DatabaseSize
//	*StringData
//	*ListHead, *ListEntry, *ListData
//	*SetHead, *SetEntry, *SetData
//	*SortedSetHead, *SortedSetEntry, *SortedSetData
//	*MapHead, *MapEntry, *MapData
//
// Next returns a io.EOF error when a EOF token is read.
func (p *Parser) Next() (interface{}, error) {
	if !p.initialized {
		if err := p.verifyMagicString(); err != nil {
			return nil, err
		}

		if err := p.verifyVersion(); err != nil {
			return nil, err
		}

		p.initialized = true
	}

	p.expiry = nil

	for {
		data, err := p.nextLoop()
		if err != nil {
			if errors.Is(err, errContinueLoop) {
				continue
			}

			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		return data, nil
	}

	return nil, io.EOF
}

func (p *Parser) verifyMagicString() error {
	buf, err := p.reader.ReadBytes(len(magicString))
	if err != nil {
		return fmt.Errorf("failed to read magic string: %w", err)
	}

	if !bytes.Equal(buf, magicString) {
		return ErrInvalidMagicString
	}

	return nil
}

func (p *Parser) verifyVersion() error {
	s, err := readStringByLength(p.reader, 4)
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}

	version, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("invalid version %q: %w", s, err)
	}

	if version < minVersion || version > maxVersion {
		return UnsupportedVersionError{Version: version}
	}

	return nil
}

func (p *Parser) nextLoop() (interface{}, error) {
	if p.dataType != nil {
		data, err := p.readData()
		if err != nil {
			return nil, err
		}

		return data, nil
	}

	dataType, err := readByte(p.reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data type: %w", err)
	}

	switch dataType {
	case opCodeExpireTimeMS:
		if p.expiry, err = readMillisecondsTime(p.reader); err != nil {
			return nil, fmt.Errorf("failed to read expire time ms: %w", err)
		}

		return nil, errContinueLoop

	case opCodeExpireTime:
		if p.expiry, err = readSecondsTime(p.reader); err != nil {
			return nil, fmt.Errorf("failed to read expire time: %w", err)
		}

		return nil, errContinueLoop

	case opCodeIdle:
		if _, err := readLength(p.reader); err != nil {
			return nil, fmt.Errorf("failed to read idle: %w", err)
		}

		return nil, errContinueLoop

	case opCodeFreq:
		if _, err := readByte(p.reader); err != nil {
			return nil, fmt.Errorf("failed to read freq: %w", err)
		}

		return nil, errContinueLoop

	case opCodeSelectDB:
		if p.db, err = readLength(p.reader); err != nil {
			return nil, fmt.Errorf("failed to read database selector: %w", err)
		}

		return nil, errContinueLoop

	case opCodeAux:
		key, err := readString(p.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read aux key: %w", err)
		}

		value, err := readString(p.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read aux value: %w", err)
		}

		return &Aux{Key: key, Value: value}, nil

	case opCodeResizeDB:
		dbSize, err := readLength(p.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read database size: %w", err)
		}

		expireSize, err := readLength(p.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read expire size: %w", err)
		}

		return &DatabaseSize{
			Size:   dbSize,
			Expire: expireSize,
		}, nil

	case opCodeModuleAux:
		// TODO

	case opCodeEOF:
		// TODO: verify checksum
		return nil, io.EOF
	}

	if p.key, err = readString(p.reader); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	p.dataType = &dataType

	return nil, errContinueLoop
}

func (p *Parser) readData() (interface{}, error) {
	key := DataKey{
		Key:      p.key,
		Expiry:   p.expiry,
		Database: p.db,
	}

	if p.KeyFilter != nil && !p.KeyFilter(&key) {
		if err := p.skipData(); err != nil {
			return nil, err
		}

		p.dataType = nil
		p.expiry = nil

		return nil, errContinueLoop
	}

	if p.iterator != nil {
		value, err := p.iterator.Next()

		if errors.Is(err, io.EOF) {
			p.dataType = nil
			p.expiry = nil
			p.iterator = nil

			return nil, errContinueLoop
		}

		if err != nil {
			return nil, fmt.Errorf("iterator error: %w", err)
		}

		return value, nil
	}

	switch *p.dataType {
	case typeString:
		value, err := readString(p.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read string: %w", err)
		}

		p.dataType = nil

		return &StringData{DataKey: key, Value: value}, nil

	case typeList:
		p.iterator = &seqIterator{
			DataKey:     key,
			Reader:      p.reader,
			ValueReader: stringValueReader{},
			Mapper:      listMapper{},
		}

		return nil, errContinueLoop

	case typeSet:
		p.iterator = &seqIterator{
			DataKey:     key,
			Reader:      p.reader,
			ValueReader: stringValueReader{},
			Mapper:      setMapper{},
		}

		return nil, errContinueLoop

	case typeZSet, typeZSet2:
		p.iterator = &seqIterator{
			DataKey:     key,
			Reader:      p.reader,
			ValueReader: sortedSetValueReader{Type: *p.dataType},
			Mapper:      sortedSetMapper{},
		}

		return nil, errContinueLoop

	case typeHash:
		p.iterator = &seqIterator{
			DataKey:     key,
			Reader:      p.reader,
			ValueReader: hashValueReader{},
			Mapper:      hashMapper{},
		}

		return nil, errContinueLoop

	case typeHashZipMap:
		p.iterator = &zipMapIterator{
			DataKey: key,
			Reader:  p.reader,
			Mapper:  hashMapper{},
		}

		return nil, errContinueLoop

	case typeListZipList:
		p.iterator = &zipListIterator{
			DataKey:     key,
			Reader:      p.reader,
			ValueReader: listZipListValueReader{},
			Mapper:      listMapper{},
			ValueLength: 1,
		}

		return nil, errContinueLoop

	case typeSetIntSet:
		p.iterator = &intSetIterator{
			DataKey: key,
			Reader:  p.reader,
			Mapper:  setMapper{},
		}

		return nil, errContinueLoop

	case typeZSetZipList:
		p.iterator = &zipListIterator{
			DataKey:     key,
			Reader:      p.reader,
			ValueReader: sortedSetZipListValueReader{},
			Mapper:      sortedSetMapper{},
			ValueLength: 2,
		}

		return nil, errContinueLoop

	case typeHashZipList:
		p.iterator = &zipListIterator{
			DataKey:     key,
			Reader:      p.reader,
			ValueReader: hashZipListValueReader{},
			Mapper:      hashMapper{},
			ValueLength: 2,
		}

		return nil, errContinueLoop

	case typeListQuickList:
		p.iterator = &quickListIterator{
			DataKey:     key,
			Reader:      p.reader,
			ValueReader: listZipListValueReader{},
			Mapper:      listMapper{},
		}

		return nil, errContinueLoop

	case typeModule2:
		length, err := readLength(p.reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read list length: %w", err)
		}

		dataType := *p.dataType
		p.dataType = nil

		switch length {
		case redisBloomBloomFilter:
			if err := readBloomFilter(p.reader); err != nil {
				return nil, err
			}

			return &BloomFilter{key}, nil
		case redisBloomCuckooFilter:
			if err := readCuckooFilter(p.reader); err != nil {
				return nil, err
			}

			return &CuckooFilter{key}, nil
		case redisBloomTopK:
			// TODO
			return nil, UnsupportedDataTypeError{DataType: dataType}
		case redisBloomTDigest:
			// TODO
			return nil, UnsupportedDataTypeError{DataType: dataType}
		case redisBloomCountMinSketch:
			// TODO
			return nil, UnsupportedDataTypeError{DataType: dataType}
		default: // other data types beside redisbloom
			return nil, UnsupportedDataTypeError{DataType: dataType}
		}
	}

	return nil, UnsupportedDataTypeError{DataType: *p.dataType}
}

func (p *Parser) skipData() error {
	switch *p.dataType {
	case typeString, typeHashZipMap, typeListZipList, typeSetIntSet, typeZSetZipList, typeHashZipList:
		return p.skipStrings(1)

	case typeList, typeSet:
		length, err := readLength(p.reader)
		if err != nil {
			return fmt.Errorf("failed to read list length: %w", err)
		}

		return p.skipStrings(length)

	case typeZSet, typeZSet2:
		length, err := readLength(p.reader)
		if err != nil {
			return fmt.Errorf("failed to read zset length: %w", err)
		}

		for i := 0; i < length; i++ {
			if err := skipString(p.reader); err != nil {
				return err
			}

			if *p.dataType == typeZSet2 {
				err = skipBinaryDouble(p.reader)
			} else {
				err = skipFloat(p.reader)
			}

			if err != nil {
				return err
			}
		}

	case typeHash:
		length, err := readLength(p.reader)
		if err != nil {
			return fmt.Errorf("failed to read hash length: %w", err)
		}

		return p.skipStrings(length * 2)

	case typeListQuickList:
		length, err := readLength(p.reader)
		if err != nil {
			return fmt.Errorf("failed to read quicklist length: %w", err)
		}

		return p.skipStrings(length)

	case typeModule:
		// TODO

	case typeModule2:
		// TODO

	case typeStreamListPacks:
		// TODO
	}

	return nil
}

func (p *Parser) skipStrings(n int) error {
	for i := 0; i < n; i++ {
		if err := skipString(p.reader); err != nil {
			return err
		}
	}

	return nil
}

// https://github.com/RedisBloom/RedisBloom/blob/21a2620e75873353fead8c5d70950d3791e36b18/src/rebloom.c#L1116-L1131
func readBloomFilter(r byteReader) error {
	// size
	_, err := redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}

	// numFilters
	numFilters, err := redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}

	// options
	_, err = redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}

	// growth
	_, err = redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}

	for i := 0; i < numFilters; i++ {
		// entries
		_, err = redisModuleReadUnsigned(r)
		if err != nil {
			return err
		}

		// error
		_, err = redisModuleReadDouble(r)
		if err != nil {
			return err
		}

		// hashes
		_, err = redisModuleReadUnsigned(r)
		if err != nil {
			return err
		}
		// bpe
		_, err = redisModuleReadDouble(r)
		if err != nil {
			return err
		}

		// bits
		_, err = redisModuleReadUnsigned(r)
		if err != nil {
			return err
		}
		// n2
		_, err = redisModuleReadUnsigned(r)
		if err != nil {
			return err
		}

		// string buffer
		_, err = redisModuleReadStringBuffer(r)
		if err != nil {
			return err
		}

		// size
		_, err = redisModuleReadUnsigned(r)
		if err != nil {
			return err
		}
	}

	if err := checkRdbModuleOpCode(r, rdbModuleOpcodeEOF); err != nil {
		return err
	}

	return nil
}

func readCuckooFilter(r byteReader) error {
	numFilters, err := redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}

	// numBuckets
	_, err = redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}

	// numItems
	_, err = redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}

	// numDeletes
	_, err = redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}
	// bucketSize
	_, err = redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}
	// maxIterations
	_, err = redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}
	// expansion
	_, err = redisModuleReadUnsigned(r)
	if err != nil {
		return err
	}

	for i := 0; i < numFilters; i++ {
		// filters[i].numBuckets
		_, err = redisModuleReadUnsigned(r)
		if err != nil {
			return err
		}

		// string buffer
		_, err = redisModuleReadStringBuffer(r)
		if err != nil {
			return err
		}
	}

	if err := checkRdbModuleOpCode(r, rdbModuleOpcodeEOF); err != nil {
		return err
	}

	return nil
}
