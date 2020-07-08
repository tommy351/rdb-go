package rdb

import (
	"fmt"
	"io"
	"os"
	"strings"
)

func ExampleParser_basic() {
	file, err := os.Open("dump.rdb")

	if err != nil {
		panic(err)
	}

	defer file.Close()

	parser := NewParser(file)

	for {
		data, err := parser.Next()

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		switch data := data.(type) {
		case *StringData:
			fmt.Println(data.Key, data.Value)

		case *ListData:
			for i, value := range data.Value {
				fmt.Println(data.Key, i, value)
			}
		}
	}
}

func ExampleParser_keyFilter() {
	file, err := os.Open("dump.rdb")

	if err != nil {
		panic(err)
	}

	defer file.Close()

	parser := NewParser(file)

	parser.KeyFilter = func(key *DataKey) bool {
		return key.Database == 0 && strings.HasPrefix(key.Key, "foo:") && !key.Expired()
	}
}
