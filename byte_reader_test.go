package rdb

import (
	"bytes"
	"io"
	"math/rand"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func makeRandBuffer(n int) []byte {
	buf := make([]byte, n)
	// nolint: gosec
	_, err := rand.Read(buf)
	Expect(err).NotTo(HaveOccurred())
	return buf
}

func mustReadBytes(data []byte, err error) []byte {
	Expect(err).NotTo(HaveOccurred())
	return data
}

var _ = Describe("sliceReader", func() {
	It("read data", func() {
		buf := makeRandBuffer(10)
		reader := newSliceReader(buf)

		Expect(mustReadBytes(reader.ReadBytes(4))).To(Equal(buf[0:4]))
		Expect(mustReadBytes(reader.ReadBytes(8))).To(Equal(buf[4:10]))

		_, err := reader.ReadBytes(1)
		Expect(err).To(Equal(io.EOF))
	})
})

var _ = Describe("bufferReader", func() {
	It("small data", func() {
		buf := makeRandBuffer(4)
		reader := newBufferReader(bytes.NewReader(buf))

		Expect(mustReadBytes(reader.ReadBytes(1))).To(Equal(buf[0:1]))
		Expect(mustReadBytes(reader.ReadBytes(2))).To(Equal(buf[1:3]))
	})

	It("unexpected EOF", func() {
		buf := makeRandBuffer(4)
		reader := newBufferReader(bytes.NewReader(buf))

		actual, err := reader.ReadBytes(5)
		Expect(actual).To(BeNil())
		Expect(err).To(Equal(io.ErrUnexpectedEOF))
	})

	It("larger than max buffer size", func() {
		buf := makeRandBuffer(4100)
		reader := newBufferReader(bytes.NewReader(buf))

		Expect(mustReadBytes(reader.ReadBytes(1))).To(Equal(buf[0:1]))
		Expect(mustReadBytes(reader.ReadBytes(4097))).To(Equal(buf[1:4098]))
		Expect(mustReadBytes(reader.ReadBytes(2))).To(Equal(buf[4098:4100]))
	})

	It("gradually extend the capacity", func() {
		buf := makeRandBuffer(8000)
		reader := newBufferReader(bytes.NewReader(buf))

		Expect(mustReadBytes(reader.ReadBytes(4))).To(Equal(buf[0:4]))
		Expect(mustReadBytes(reader.ReadBytes(513))).To(Equal(buf[4:517]))
		Expect(mustReadBytes(reader.ReadBytes(1025))).To(Equal(buf[517:1542]))
		Expect(mustReadBytes(reader.ReadBytes(2049))).To(Equal(buf[1542:3591]))
		Expect(mustReadBytes(reader.ReadBytes(4097))).To(Equal(buf[3591:7688]))
		Expect(mustReadBytes(reader.ReadBytes(200))).To(Equal(buf[7688:7888]))
		Expect(mustReadBytes(reader.ReadBytes(100))).To(Equal(buf[7888:7988]))
	})

	It("move remaining data to the front", func() {
		buf := makeRandBuffer(1200)
		reader := newBufferReader(bytes.NewReader(buf))

		Expect(mustReadBytes(reader.ReadBytes(200))).To(Equal(buf[0:200]))
		Expect(mustReadBytes(reader.ReadBytes(400))).To(Equal(buf[200:600]))
		Expect(mustReadBytes(reader.ReadBytes(600))).To(Equal(buf[600:1200]))
	})
})
