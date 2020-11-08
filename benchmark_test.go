package rdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

func BenchmarkParser(b *testing.B) {
	benchmarkDumpFile := func(b *testing.B, name string) {
		buf, err := ioutil.ReadFile(fmt.Sprintf("fixtures/%s.rdb", name))
		if err != nil {
			b.Error(err)
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			parser := NewParser(bytes.NewBuffer(buf))

			for {
				_, err := parser.Next()
				if errors.Is(err, io.EOF) {
					break
				}

				if err != nil {
					b.Error(err)
				}
			}
		}
	}

	for _, name := range []string{
		"empty_database",
		"parser_filters",
		"linkedlist",
	} {
		name := name
		b.Run(name, func(b *testing.B) {
			benchmarkDumpFile(b, name)
		})
	}
}
