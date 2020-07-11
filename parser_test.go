package rdb

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/davecgh/go-spew/spew"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
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

	setupFixture := func(file **os.File, name string) {
		BeforeEach(func() {
			f, err := os.Open(fmt.Sprintf("fixtures/%s.rdb", name))
			Expect(err).NotTo(HaveOccurred())
			*file = f
		})

		AfterEach(func() {
			Expect((*file).Close()).To(Succeed())
		})
	}

	testDumpFile := func(name string) {
		Describe(name, func() {
			var file *os.File

			setupFixture(&file, name)

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
		})
	}

	// Basic
	testDumpFile("empty_database")
	testDumpFile("multiple_databases")
	testDumpFile("keys_with_expiry")
	testDumpFile("easily_compressible_string_key")
	testDumpFile("integer_keys")
	testDumpFile("non_ascii_values")
	testDumpFile("uncompressible_string_keys")
	testDumpFile("big_values")

	// List
	testDumpFile("linkedlist")
	testDumpFile("ziplist_that_compresses_easily")
	testDumpFile("ziplist_that_doesnt_compress")
	testDumpFile("ziplist_with_integers")
	testDumpFile("quicklist")

	// Set
	testDumpFile("regular_set")
	testDumpFile("intset_16")
	testDumpFile("intset_32")
	testDumpFile("intset_64")

	// Sorted set
	testDumpFile("regular_sorted_set")
	testDumpFile("sorted_set_as_ziplist")

	// Hash
	testDumpFile("dictionary")
	testDumpFile("hash_as_ziplist")
	testDumpFile("zipmap_that_compresses_easily")
	testDumpFile("zipmap_that_doesnt_compress")
	testDumpFile("zipmap_with_big_values")

	When("file is not started with the magic string", func() {
		It("should return ErrInvalidMagicString", func() {
			parser := NewParser(bytes.NewBufferString("YOMAN"))
			_, err := parser.Next()
			Expect(err).To(Equal(ErrInvalidMagicString))
		})
	})

	When("version is not a number", func() {
		It("should return error", func() {
			parser := NewParser(bytes.NewBufferString("REDISxxxx"))
			_, err := parser.Next()
			Expect(err).To(MatchError(HavePrefix(`invalid version "xxxx"`)))
		})
	})

	When("version < 1", func() {
		It("should return UnsupportedVersionError", func() {
			parser := NewParser(bytes.NewBufferString("REDIS0000"))
			_, err := parser.Next()
			Expect(err).To(Equal(UnsupportedVersionError{Version: 0}))
		})
	})

	When("version > 9", func() {
		It("should return UnsupportedVersionError", func() {
			parser := NewParser(bytes.NewBufferString("REDIS0010"))
			_, err := parser.Next()
			Expect(err).To(Equal(UnsupportedVersionError{Version: 10}))
		})
	})

	Describe("KeyFilter", func() {
		expectKeyTo := func(actual interface{}, matcher types.GomegaMatcher) {
			Expect(actual).To(PointTo(MatchFields(IgnoreExtras, Fields{
				"DataKey": MatchFields(IgnoreExtras, Fields{
					"Key": matcher,
				}),
			})))
		}

		testExcludeKey := func(filename, key string) {
			Describe(fmt.Sprintf("Exclude %s from %s", key, filename), func() {
				var file *os.File

				setupFixture(&file, filename)

				It("should exclude the key", func() {
					parser := NewParser(file)
					parser.KeyFilter = func(k *DataKey) bool {
						return k.Key != key
					}

					for {
						data, err := parser.Next()

						if err == io.EOF {
							break
						}

						Expect(err).NotTo(HaveOccurred())
						expectKeyTo(data, Not(Equal(key)))
					}
				})
			})
		}

		// Basic
		testExcludeKey("parser_filters", "k1")
		testExcludeKey("parser_filters", "s1")
		testExcludeKey("parser_filters", "b1")

		// List
		testExcludeKey("parser_filters", "l10")
		testExcludeKey("linkedlist", "force_linkedlist")

		// Set
		testExcludeKey("parser_filters", "set1")
		testExcludeKey("regular_set", "regular_set")

		// Hash
		testExcludeKey("parser_filters", "h1")
		testExcludeKey("hash_as_ziplist", "zipmap_compresses_easily")

		// Sorted set
		testExcludeKey("parser_filters", "z1")
		testExcludeKey("regular_sorted_set", "force_sorted_set")

		Describe("Filter by database", func() {
			var file *os.File

			setupFixture(&file, "multiple_databases")

			It("should exclude database 0", func() {
				parser := NewParser(file)
				parser.KeyFilter = func(k *DataKey) bool {
					return k.Database > 0
				}

				for {
					data, err := parser.Next()

					if err == io.EOF {
						break
					}

					Expect(err).NotTo(HaveOccurred())
					expectKeyTo(data, Equal("key_in_second_database"))
				}
			})
		})
	})
})
