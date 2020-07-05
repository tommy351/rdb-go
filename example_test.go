package rdb

import (
	"fmt"
	"io"
)

func ExampleParser() {
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
		}
	}
}
