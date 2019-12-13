package gmemdb_test

import (
	"math/rand"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/jxlczjp77/gmemdb"
)

var (
	testOtherIdx = false
	testBench    = true
)
var _ = Describe("性能测试1", func() {
	if testBench {
		testCount := 100000
		var seqs []int
		BeforeEach(func() {
			tmp := make(map[int]bool)
			seqs = make([]int, 0, testCount)
			for i := 1; i <= testCount; i++ {
				for {
					idx := rand.Intn(testCount)
					if _, ok := tmp[idx]; !ok {
						tmp[idx] = true
						break
					}
				}
			}
			for idx := range tmp {
				seqs = append(seqs, idx)
			}
		})
		Measure("Add性能测试", func(b Benchmarker) {
			rt := b.Time("无事物插入耗时", func() {
				var transaction *gmemdb.Transaction
				transaction = nil
				mdb := newTestObjMDB(testOtherIdx)
				for n := 0; n < testCount; n++ {
					i := seqs[n] + 1
					obj := &dbTestObj{Name: strconv.Itoa(i), ID1: int32(i), ID2: int32(testCount + i), Address: ""}
					mdb.Add(obj, transaction, 0)
				}
			})
			b.RecordValue("无事物插入速度(条 / 每秒)", float64(testCount)/rt.Seconds())

			rt1 := b.Time("每1000条提交插入耗时", func() {
				var transaction *gmemdb.Transaction
				transaction = gmemdb.NewTransaction()
				mdb := newTestObjMDB(testOtherIdx)
				for n := 0; n < testCount; n++ {
					i := seqs[n] + 1
					obj := &dbTestObj{Name: strconv.Itoa(i), ID1: int32(i), ID2: int32(testCount + i), Address: ""}
					mdb.Add(obj, transaction, 0)
					if n%1000 == 0 {
						transaction.Commit(0)
					}
				}
				transaction.Commit(0)
			})
			b.RecordValue("每1000条提交插入速度(条 / 每秒)", float64(testCount)/rt1.Seconds())

			rt2 := b.Time("每10000条提交插入耗时", func() {
				var transaction *gmemdb.Transaction
				transaction = gmemdb.NewTransaction()
				mdb := newTestObjMDB(testOtherIdx)
				for n := 0; n < testCount; n++ {
					i := seqs[n] + 1
					obj := &dbTestObj{Name: strconv.Itoa(i), ID1: int32(i), ID2: int32(testCount + i), Address: ""}
					mdb.Add(obj, transaction, 0)
					if n%10000 == 0 {
						transaction.Commit(0)
					}
				}
				transaction.Commit(0)
			})
			b.RecordValue("每10000条提交插入速度(条 / 每秒)", float64(testCount)/rt2.Seconds())
		}, 5)
	}
})

var _ = Describe("性能测试2", func() {
	if testBench {
		testCount := 100000
		var mdb *testObjMDB
		BeforeEach(func() {
			mdb = newTestObjMDB(true)
			for i := 1; i <= testCount; i++ {
				obj := &dbTestObj{Name: strconv.Itoa(i), ID1: int32(i), ID2: int32(testCount + i), Address: ""}
				mdb.Add(obj, nil, 0)
			}
		})

		Measure("性能测试2", func(b Benchmarker) {
			rt2 := b.Time("Find性能测试", func() {
				for i := 1; i <= testCount; i++ {
					ID1 := int32(i)
					ID2 := int32(testCount + i)
					iter := mdb.findByID(ID1, ID2)
					obj := iter.Step().(*dbTestObj)
					Expect(obj.ID1).Should(Equal(ID1))
					Expect(obj.ID2).Should(Equal(ID2))
					Expect(iter.Step()).Should(BeNil())
				}
			})
			Expect(mdb.Count()).Should(Equal(testCount))
			b.RecordValue("Find性能测试(条 / 每秒)", float64(testCount)/rt2.Seconds())

			rt3 := b.Time("删除性能测试", func() {
				for i := 1; i <= testCount; i++ {
					ID1 := int32(i)
					ID2 := int32(testCount + i)
					iter := mdb.findByID(ID1, ID2)
					obj := iter.Step().(*dbTestObj)
					mdb.Remove(obj, nil, 0)
				}
			})
			Expect(mdb.Count()).Should(BeZero())
			b.RecordValue("删除性能测试(条 / 每秒)", float64(testCount)/rt3.Seconds())
		}, 5)
	}
})
