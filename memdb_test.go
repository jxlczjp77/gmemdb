package gmemdb_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGmemdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "内存表测试")
}
