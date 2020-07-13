package rdb

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("DataKey", func() {
	DescribeTable("Expired", func(expiry *time.Time, expected bool) {
		Expect(DataKey{Expiry: expiry}.Expired()).To(Equal(expected))
	},
		Entry("nil", nil, false),
		Entry("after now", timePtr(time.Now().Add(time.Minute)), false),
		Entry("before now", timePtr(time.Now().Add(-time.Minute)), true),
	)
})
