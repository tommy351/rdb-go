package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tommy351/goldga"
)

var _ = Describe("JSONPrinter", func() {
	matchGoldenFile := func() *goldga.Matcher {
		matcher := goldga.Match()
		matcher.Serializer = &goldga.JSONSerializer{}

		return matcher
	}

	testDumpFile := func(name string) {
		var buf bytes.Buffer

		BeforeEach(func() {
			buf.Reset()
			printer := NewJSONPrinter(&buf)
			file, err := os.Open(fmt.Sprintf("../../fixtures/%s.rdb", name))
			Expect(err).NotTo(HaveOccurred())
			defer file.Close()
			Expect(printParserData(file, printer)).To(Succeed())
		})

		It("should match the golden file", func() {
			var data []interface{}
			Expect(json.NewDecoder(&buf).Decode(&data)).To(Succeed())
			Expect(data).To(matchGoldenFile())
		})
	}

	for _, name := range []string{
		// Basic
		"empty_database",
		"keys_with_expiry",
		"multiple_databases",
		// List
		"ziplist_that_doesnt_compress",
		"ziplist_with_integers",
		// Set
		"regular_set",
		"intset_16",
		// Sorted Set
		"sorted_set_as_ziplist",
		// Hash
		"hash_as_ziplist",
	} {
		name := name
		Describe(name, func() {
			testDumpFile(name)
		})
	}
})
