package rdb

import (
	"fmt"
	"io"
	"os"

	"github.com/davecgh/go-spew/spew"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tommy351/goldga"
)

var _ = Describe("Parser", func() {
	matchGoldenFile := func() *goldga.Matcher {
		conf := spew.NewDefaultConfig()
		conf.DisablePointerAddresses = true
		conf.SortKeys = true
		conf.DisableCapacities = true

		matcher := goldga.Match()
		matcher.Serializer = &goldga.DumpSerializer{
			Config: conf,
		}

		return matcher
	}

	testDumpFile := func(name string) {
		var file *os.File

		BeforeEach(func() {
			var err error
			file, err = os.Open(fmt.Sprintf("fixtures/%s.rdb", name))
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(file.Close()).To(Succeed())
		})

		It("should match the golden file", func() {
			var result []interface{}
			parser := NewParser(file)

			for {
				data, err := parser.Next()

				if err == io.EOF {
					break
				}

				Expect(err).NotTo(HaveOccurred())
				result = append(result, data)
			}

			Expect(result).To(matchGoldenFile())
		})
	}

	for _, name := range []string{
		// Basic
		"empty_database",
		"multiple_databases",
		"keys_with_expiry",
		"easily_compressible_string_key",
		"integer_keys",
		// "non_ascii_values",
		"uncompressible_string_keys",
		// List
		"linkedlist",
		"ziplist_that_compresses_easily",
		"ziplist_that_doesnt_compress",
		"ziplist_with_integers",
		// Set
		"regular_set",
		"intset_16",
		"intset_32",
		"intset_64",
		// Sorted set
		"regular_sorted_set",
		// "sorted_set_as_ziplist",
		// Hash
		"dictionary",
		// "hash_as_ziplist",
		"zipmap_that_compresses_easily",
		"zipmap_that_doesnt_compress",
		// "zipmap_with_big_values",
	} {
		name := name
		Describe(name, func() {
			testDumpFile(name)
		})
	}
})
