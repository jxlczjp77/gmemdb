package gmemdb_test

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"github.com/jxlczjp77/gmemdb"
)

type dbTestObj struct {
	gmemdb.ObjectBase
	Name    string
	ID1     int32
	ID2     int32
	Address string
	Money   float64
}

type dbTestObjPB struct {
	Name    *string
	ID1     *int32
	ID2     *int32
	Address string
	Money   *float64
}

func (s *dbTestObj) Clone() *dbTestObj {
	return &dbTestObj{
		ObjectBase: s.ObjectBase,
		Name:       s.Name,
		ID1:        s.ID1,
		ID2:        s.ID2,
		Address:    s.Address,
		Money:      s.Money,
	}
}

type testObjMDB struct {
	gmemdb.ObjectFactory
}

func newTestObjMDB(otherIdx bool) *testObjMDB {
	db := &testObjMDB{}
	db.Init("testObjMDB", (*dbTestObj)(nil), (*dbTestObjPB)(nil))
	db.AddIndex("Name", func(key *gmemdb.MdbKey, obj gmemdb.IObject) error { return key.AppendString(obj.(*dbTestObj).Name) }, true)
	if otherIdx == true {
		db.AddIndex("ID1|ID2", func(key *gmemdb.MdbKey, obj gmemdb.IObject) error {
			key.AppendInt32(obj.(*dbTestObj).ID1)
			key.AppendInt32(obj.(*dbTestObj).ID2)
			return nil
		}, true)
		db.AddIndex("Address", func(key *gmemdb.MdbKey, obj gmemdb.IObject) error { return key.AppendString(obj.(*dbTestObj).Address) }, false)
	}
	return db
}

func (s *testObjMDB) addMoneyIndex() int {
	return s.AddIndex("ID1|Money", func(key *gmemdb.MdbKey, obj gmemdb.IObject) error {
		key.AppendInt32(obj.(*dbTestObj).ID1)
		key.AppendFloat64(obj.(*dbTestObj).Money)
		return nil
	}, true)
}

func (s *testObjMDB) findByName(Name string) gmemdb.Iterator {
	return s.FindByIndexName("Name").AppendString(Name).Fire()
}

func (s *testObjMDB) findByID(ID1 int32, ID2 int32) gmemdb.Iterator {
	return s.FindByIndexName("ID1|ID2").AppendInt32(ID1).AppendInt32(ID2).Fire()
}

func (s *testObjMDB) findByID1(ID1 int32) gmemdb.Iterator {
	return s.FindByIndexName("ID1|ID2").AppendInt32(ID1).Fire()
}

func (s *testObjMDB) findByAddress(addr string) gmemdb.Iterator {
	return s.FindByIndexName("Address").AppendString(addr).Fire()
}

func HaveName(n string) types.GomegaMatcher {
	return WithTransform(func(p *dbTestObj) string { return p.Name }, Equal(n))
}

func HaveID(ID1 int32, ID2 int32) types.GomegaMatcher {
	n := (uint64(ID1) << 32) | uint64(ID2)
	return WithTransform(func(p *dbTestObj) uint64 { return (uint64(p.ID1) << 32) | uint64(p.ID2) }, Equal(n))
}

func HaveAddress(n string) types.GomegaMatcher {
	return WithTransform(func(p *dbTestObj) string { return p.Address }, Equal(n))
}

func makeSortTestData() []*dbTestObj {
	testObjs := make([]*dbTestObj, 0)
	ID1 := 1
	ID2 := 10000
	Name := "张三"
	for i := 0; i < 50; i++ {
		nn := fmt.Sprintf("%s%d", Name, i+ID1)
		money := float64(i)*0.01 + float64(ID1)
		testObjs = append(testObjs, &dbTestObj{Name: nn, ID1: int32(ID1), ID2: int32(ID2 + i), Address: "张三地址", Money: -1.0 * money})
	}
	ID1++
	ID2 = 20000
	Name = "李四"
	for i := 0; i < 50; i++ {
		nn := fmt.Sprintf("%s%d", Name, i+ID1)
		money := float64(i)*0.01 + float64(ID1)
		testObjs = append(testObjs, &dbTestObj{Name: nn, ID1: int32(ID1), ID2: int32(ID2 + i), Address: "李四地址", Money: money})
	}
	ID1++
	ID2 = 30000
	Name = "王五"
	for i := 0; i < 50; i++ {
		nn := fmt.Sprintf("%s%d", Name, i+ID1)
		money := float64(i)*0.01 + float64(ID1)
		testObjs = append(testObjs, &dbTestObj{Name: nn, ID1: int32(ID1), ID2: int32(ID2 + i), Address: "王五地址", Money: money})
	}
	return testObjs
}

func randomIndexs(n int) []int {
	t := make([]int, 0)
	t1 := make([]int, n)
	for i := 0; i < n; i++ {
		t1[i] = i
	}
	for len(t) < n {
		i := 0
		if len(t1) > 1 {
			i = rand.Intn(len(t1) - 1)
		}
		if i >= 0 && i < len(t1) {
			t = append(t, t1[i])
			t1 = append(t1[:i], t1[i+1:]...)
		}
	}
	return t
}

type SortByMoneyList []*dbTestObj

func (s SortByMoneyList) Len() int           { return len(s) }
func (s SortByMoneyList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SortByMoneyList) Less(i, j int) bool { return s[i].Money < s[j].Money }

var _ = Describe("内存表测试", func() {
	var mdb *testObjMDB
	testObjs := []*dbTestObj{
		&dbTestObj{Name: "张三1", ID1: 1, ID2: 10011, Address: "张三地址", Money: 1.01},
		&dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址", Money: 1.02},
		&dbTestObj{Name: "张三3", ID1: 1, ID2: 10013, Address: "张三地址", Money: 1.03},
		&dbTestObj{Name: "李四1", ID1: 2, ID2: 10021, Address: "李四地址", Money: 2.01},
		&dbTestObj{Name: "李四2", ID1: 2, ID2: 10022, Address: "李四地址", Money: 2.02},
		&dbTestObj{Name: "李四3", ID1: 2, ID2: 10023, Address: "李四地址", Money: 2.03},
		&dbTestObj{Name: "王五1", ID1: 3, ID2: 10031, Address: "李四地址4", Money: 3.01},
		&dbTestObj{Name: "王五2", ID1: 3, ID2: 10032, Address: "李四地址4", Money: 3.02},
		&dbTestObj{Name: "王五3", ID1: 3, ID2: 10033, Address: "李四地址4", Money: 3.03},
	}
	CheckObjects := func() {
		for _, obj := range testObjs {
			dbObj := mdb.findByName(obj.Name).Step()
			Expect(dbObj).ShouldNot(BeNil())
			Expect(dbObj).Should(Equal(obj))
			Expect(dbObj.(*dbTestObj).Address).Should(Equal(obj.Address))
			Expect(dbObj.(*dbTestObj).Name).Should(Equal(obj.Name))
			Expect(dbObj.(*dbTestObj).ID1).Should(Equal(obj.ID1))
			Expect(dbObj.(*dbTestObj).ID2).Should(Equal(obj.ID2))
		}
	}
	BeforeEach(func() {
		mdb = newTestObjMDB(true)
		for _, obj := range testObjs {
			mdb.Add(obj, nil, 0)
		}
		CheckObjects()
	})

	It("按名词查找测试", func() {
		var iter gmemdb.Iterator

		iter = mdb.findByName("张三2")
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(BeNil())

		iter = mdb.findByName("李四1")
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四1")))
		Expect(iter.Step()).Should(BeNil())
	})

	It("ID组合索引查找测试", func() {
		var iter gmemdb.Iterator

		iter = mdb.findByID(1, 10012)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(BeNil())

		iter = mdb.findByID(2, 10021)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四1")))
		Expect(iter.Step()).Should(BeNil())

		iter = mdb.findByID1(1)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(iter.Step()).Should(BeNil())

		iter = mdb.findByID1(2)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四3")))
		Expect(iter.Step()).Should(BeNil())

		iter = mdb.findByID1(1)
		for i := 0; i < len(testObjs)-1; i++ {
			obj := testObjs[i]
			t := iter.RawStep()
			Expect(t).ShouldNot(BeNil())
			Expect(obj.Name).Should(Equal(t.(*dbTestObj).Name))
		}
	})

	It("迭代器测试", func() {
		// 主索引迭代
		id := uint32(1)
		iter := mdb.Begin(0)
		for obj := iter.Step(); obj != nil; obj = iter.Step() {
			Expect(obj.GetID()).Should(Equal(id))
			id++
		}
		Expect(int(id - 1)).Should(Equal(len(testObjs)))

		// 按名字索引迭代
		id = uint32(1)
		iter = mdb.Begin(1)
		for obj := iter.Step(); obj != nil; obj = iter.Step() {
			id++
		}
		Expect(int(id - 1)).Should(Equal(len(testObjs)))

		// 按ID索引迭代
		id = uint32(1)
		iter = mdb.Begin(2)
		for obj := iter.Step(); obj != nil; obj = iter.Step() {
			id++
		}
		Expect(int(id - 1)).Should(Equal(len(testObjs)))

		// 按地址索引迭代
		id = uint32(1)
		iter = mdb.Begin(3)
		for obj := iter.Step(); obj != nil; obj = iter.Step() {
			id++
		}
		Expect(int(id - 1)).Should(Equal(len(testObjs)))
	})

	It("RAW迭代器测试", func() {
		id := uint32(2)
		iter := mdb.findByName("张三2")
		for iter.RawNext() {
			Expect(iter.Value().GetID()).Should(Equal(id))
			id++
		}
		Expect(int(id - 1)).Should(Equal(len(testObjs)))

		id = uint32(2)
		iter = mdb.findByID(1, 10012)
		for iter.RawNext() {
			Expect(iter.Value().GetID()).Should(Equal(id))
			id++
		}
		Expect(int(id - 1)).Should(Equal(len(testObjs)))
	})

	It("按非唯一索引查找测试", func() {
		var iter gmemdb.Iterator

		iter = mdb.findByAddress("张三地址")
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(iter.Step()).Should(BeNil())

		iter = mdb.findByAddress("李四地址")
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四3")))
		Expect(iter.Step()).Should(BeNil())

		iter = mdb.findByAddress("李四地址4")
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("王五1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("王五2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("王五3")))
		Expect(iter.Step()).Should(BeNil())

		// 非唯一索引长度最长允许255字节
		obj := &dbTestObj{Name: "赵六", ID1: 4, ID2: 10041}
		obj.Address = strings.Repeat("1", 255)
		Expect(mdb.Add(obj, nil, 0)).Should(BeTrue())
		Expect(mdb.findByAddress(obj.Address).Step()).Should(And(Not(BeNil()), HaveName("赵六")))
	})

	It("非唯一索引组合Key测试", func() {
		var iter gmemdb.Iterator
		testDB := newTestObjMDB(false)
		testDB.AddIndex("ID1|ID2", func(key *gmemdb.MdbKey, obj gmemdb.IObject) error {
			key.AppendInt32(obj.(*dbTestObj).ID1)
			key.AppendInt32(obj.(*dbTestObj).ID2)
			return nil
		}, false)
		for _, obj := range testObjs {
			testDB.Add(obj, nil, 0)
		}

		iter = testDB.findByID(1, 10012)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(BeNil())

		iter = testDB.findByID(2, 10021)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四1")))
		Expect(iter.Step()).Should(BeNil())

		iter = testDB.findByID1(1)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(iter.Step()).Should(BeNil())

		iter = testDB.findByID1(2)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("李四3")))
		Expect(iter.Step()).Should(BeNil())

		// 新增几个 ID1: 1, ID2: 10012 对象
		zs2Tmp1 := &dbTestObj{Name: "张三2 ID组合1", ID1: 1, ID2: 10012, Address: "张三地址"}
		zs2Tmp2 := &dbTestObj{Name: "张三2 ID组合2", ID1: 1, ID2: 10012, Address: "张三地址"}
		zs2Tmp3 := &dbTestObj{Name: "张三2 ID组合3", ID1: 1, ID2: 10012, Address: "张三地址"}
		Expect(testDB.Add(zs2Tmp1, nil, 0)).Should(BeTrue())
		Expect(testDB.Add(zs2Tmp2, nil, 0)).Should(BeTrue())
		Expect(testDB.Add(zs2Tmp3, nil, 0)).Should(BeTrue())

		// 按照组合key完整查找
		iter = testDB.findByID(1, 10012)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2 ID组合1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2 ID组合2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2 ID组合3")))
		Expect(iter.Step()).Should(BeNil())

		// 按照组合key部分查找
		iter = testDB.findByID1(1)
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2 ID组合1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2 ID组合2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2 ID组合3")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(iter.Step()).Should(BeNil())
	})

	It("索引冲突测试", func() {
		// Add: 名字冲突
		addObj1 := &dbTestObj{Name: "张三1", ID1: 1, ID2: 10011, Address: "张三地址1"}
		Expect(func() { mdb.Add(addObj1, nil, 0) }).Should(Panic())
		CheckObjects()

		// Add: ID冲突
		addObj2 := &dbTestObj{Name: "张三1_", ID1: 1, ID2: 10011, Address: "张三地址1"}
		Expect(func() { mdb.Add(addObj2, nil, 0) }).Should(Panic())
		CheckObjects()

		// Update: 源对象没有设置ID索引
		updOldObj1 := &dbTestObj{Name: "张三1", ID1: 1, ID2: 10011, Address: "张三地址1"}
		updNewObj1 := updOldObj1.Clone()
		updNewObj1.Address += "___"
		Expect(func() { mdb.Update(updOldObj1, updNewObj1, nil, 0) }).Should(Panic())
		CheckObjects()

		// Update: 新对象的id2变了,但和其他对象冲突
		updOldObj2 := mdb.findByName("张三1").Step().(*dbTestObj)
		updNewObj2 := updOldObj2.Clone()
		updNewObj2.ID2++
		Expect(func() { mdb.Update(updOldObj2, updNewObj2, nil, 0) }).Should(Panic())
		CheckObjects()

		// Update: 新对象的name变了,但和其他对象冲突
		updOldObj3 := mdb.findByName("张三1").Step().(*dbTestObj)
		updNewObj3 := updOldObj2.Clone()
		updNewObj3.Name = "张三2"
		Expect(func() { mdb.Update(updOldObj3, updNewObj3, nil, 0) }).Should(Panic())
		CheckObjects()
	})

	It("更新时索引变化测试", func() {
		updOldObj1 := mdb.findByName("张三1").Step().(*dbTestObj)
		updNewObj1 := updOldObj1.Clone()
		updNewObj1.ID2 += 10000
		updNewObj1.Name += "___"
		Expect(mdb.Update(updOldObj1, updNewObj1, nil, 0)).Should(BeTrue())
		obj := mdb.findByID(updNewObj1.ID1, updNewObj1.ID2).Step()
		Expect(obj).Should(And(Not(BeNil()), HaveID(updNewObj1.ID1, updNewObj1.ID2)))
		obj = mdb.findByName("张三1___").Step()
		Expect(obj).Should(And(Not(BeNil()), HaveID(updNewObj1.ID1, updNewObj1.ID2)))

		// 老的ID和名字已经查找不到了
		Expect(mdb.findByID(updOldObj1.ID1, updOldObj1.ID2).Step()).Should(BeNil())
		Expect(mdb.findByName(updOldObj1.Name).Step()).Should(BeNil())
	})

	It("事物基本测试", func() {
		transaction := gmemdb.NewTransaction()
		zs2 := mdb.findByName("张三2").Step().(*dbTestObj)
		zs2Tmp := zs2.Clone()
		zs2Tmp.Address += "_DDDD"
		mdb.Update(zs2, zs2Tmp, transaction, 0)
		transaction.Commit(0)
		Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveAddress(zs2Tmp.Address)))

		mdb.Remove(zs2, transaction, 0)
		Expect(mdb.findByName("张三2").Step()).Should(BeNil())
		transaction.Rollback()
		Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveAddress(zs2Tmp.Address)))
	})

	It("事物回滚点测试1", func() {
		transaction := gmemdb.NewTransaction()
		zs1 := mdb.findByName("张三1").Step().(*dbTestObj)
		zs2 := mdb.findByName("张三2").Step().(*dbTestObj)
		zs3 := mdb.findByName("张三3").Step().(*dbTestObj)
		mdb.Remove(zs1, transaction, 0)
		Expect(mdb.findByName("张三1").Step()).Should(BeNil())

		savePoint1 := transaction.AllocSavePoint()
		mdb.Remove(zs2, transaction, 0)
		Expect(mdb.findByName("张三2").Step()).Should(BeNil())

		savePoint2 := transaction.AllocSavePoint()
		mdb.Remove(zs3, transaction, 0)
		Expect(mdb.findByName("张三3").Step()).Should(BeNil())

		savePoint2.Rollback()
		Expect(mdb.findByName("张三3").Step()).Should(And(Not(BeNil()), HaveName("张三3")))

		savePoint1.Rollback()
		Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveName("张三2")))

		transaction.Commit(0)
		Expect(mdb.findByName("张三1").Step()).Should(BeNil())
		Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(mdb.findByName("张三3").Step()).Should(And(Not(BeNil()), HaveName("张三3")))
	})

	It("事物回滚点测试2：跨过多个回滚点回滚", func() {
		transaction := gmemdb.NewTransaction()
		zs1 := mdb.findByName("张三1").Step().(*dbTestObj)
		zs2 := mdb.findByName("张三2").Step().(*dbTestObj)
		zs3 := mdb.findByName("张三3").Step().(*dbTestObj)
		mdb.Remove(zs1, transaction, 0)
		Expect(mdb.findByName("张三1").Step()).Should(BeNil())

		savePoint1 := transaction.AllocSavePoint()
		mdb.Remove(zs2, transaction, 0)
		Expect(mdb.findByName("张三2").Step()).Should(BeNil())

		savePoint2 := transaction.AllocSavePoint()
		mdb.Remove(zs3, transaction, 0)
		Expect(mdb.findByName("张三3").Step()).Should(BeNil())

		savePoint1.Rollback()
		Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(mdb.findByName("张三3").Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(savePoint1.Invalid()).Should(BeTrue())
		Expect(savePoint2.Invalid()).Should(BeTrue())

		transaction.Commit(0)
		Expect(mdb.findByName("张三1").Step()).Should(BeNil())
		Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(mdb.findByName("张三3").Step()).Should(And(Not(BeNil()), HaveName("张三3")))
	})

	It("事物回滚点测试3：跨过多个回滚点全部回滚", func() {
		transaction := gmemdb.NewTransaction()
		zs1 := mdb.findByName("张三1").Step().(*dbTestObj)
		zs2 := mdb.findByName("张三2").Step().(*dbTestObj)
		zs3 := mdb.findByName("张三3").Step().(*dbTestObj)
		mdb.Remove(zs1, transaction, 0)
		Expect(mdb.findByName("张三1").Step()).Should(BeNil())

		savePoint1 := transaction.AllocSavePoint()
		mdb.Remove(zs2, transaction, 0)
		Expect(mdb.findByName("张三2").Step()).Should(BeNil())

		savePoint2 := transaction.AllocSavePoint()
		mdb.Remove(zs3, transaction, 0)
		Expect(mdb.findByName("张三3").Step()).Should(BeNil())
		mdb.Add(&dbTestObj{Name: "张三4", ID1: 1, ID2: 10014, Address: "张三地址4"}, transaction, 0)
		Expect(mdb.findByName("张三4").Step()).Should(And(Not(BeNil()), HaveName("张三4")))

		transaction.Rollback()
		Expect(savePoint1.Invalid()).Should(BeTrue())
		Expect(savePoint2.Invalid()).Should(BeTrue())
		Expect(mdb.findByName("张三1").Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(mdb.findByName("张三3").Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(mdb.findByName("张三4").Step()).Should(BeNil())
	})

	It("提交触发器测试", func() {
		transaction := gmemdb.NewTransaction()
		type Action struct {
			tag    string
			obj    *dbTestObj
			oldObj *dbTestObj
		}
		actions := []Action{
			{"add", &dbTestObj{Name: "张三4", ID1: 1, ID2: 10014, Address: "张三地址4"}, nil},
			{"add", &dbTestObj{Name: "张三5", ID1: 1, ID2: 10015, Address: "张三地址5"}, nil},
			{"upd", &dbTestObj{Name: "张三4", ID1: 1, ID2: 10014, Address: "张三地址4DDD"}, nil},
			{"upd", &dbTestObj{Name: "张三5", ID1: 1, ID2: 10015, Address: "张三地址5DDD"}, nil},
			{"savepoint:1", nil, nil},
			{"upd", &dbTestObj{Name: "张三4", ID1: 1, ID2: 10014, Address: "张三地址4DDD__"}, nil},
			{"del", &dbTestObj{Name: "张三5", ID1: 1, ID2: 10015, Address: "张三地址5"}, nil},
			{"upd", &dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址2222"}, nil},
			{"savepoint:2", nil, nil},
			{"del", &dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址2"}, nil},
			{"add", &dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址2"}, nil},
			{"upd", &dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址2222233333"}, nil},
			{"savepoint:3", nil, nil},
			{"upd", &dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址2222244444"}, nil},
			{"upd", &dbTestObj{Name: "张三4", ID1: 1, ID2: 10014, Address: "张三地址4DDD5555"}, nil},
			{"del", &dbTestObj{Name: "张三4", ID1: 1, ID2: 10014, Address: "张三地址4"}, nil},
			{"del", &dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址22222"}, nil},
			{"rollback:2", nil, nil},
			{"upd", &dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址22222"}, nil},
			{"upd", &dbTestObj{Name: "张三3", ID1: 1, ID2: 10013, Address: "张三地址333333"}, nil},
			{"savepoint:4", nil, nil},
			{"del", &dbTestObj{Name: "张三3", ID1: 1, ID2: 10013, Address: "张三地址3333333"}, nil},
		}
		shoulds := []Action{
			{"add", &dbTestObj{Name: "张三4", ID1: 1, ID2: 10014, Address: "张三地址4DDD__"}, nil},
			{"upd", &dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址22222"},
				&dbTestObj{Name: "张三2", ID1: 1, ID2: 10012, Address: "张三地址"}},
			{"del", &dbTestObj{Name: "张三3", ID1: 1, ID2: 10013, Address: "张三地址"}, nil},
		}

		var results []Action
		add := func(fid uint32, obj gmemdb.IObject, reason int32) {
			t := obj.(*dbTestObj)
			results = append(results, Action{"add", t, nil})
		}
		upd := func(fid uint32, obj gmemdb.IObject, newObj gmemdb.IObject, reason int32) {
			results = append(results, Action{"upd", newObj.(*dbTestObj), obj.(*dbTestObj)})
		}
		del := func(fid uint32, obj gmemdb.IObject, reason int32) {
			t := obj.(*dbTestObj)
			results = append(results, Action{"del", t, nil})
		}
		trigger := gmemdb.MakeCommitTrigger(add, upd, del)
		mdb.AddCommitTrigger(trigger)
		savepoints := make(map[int]*gmemdb.TransactionSavePoint)
		for _, action := range actions {
			if action.tag == "add" {
				mdb.Add(action.obj, transaction, 0)
			} else if action.tag == "upd" {
				old := mdb.findByName(action.obj.Name).Step()
				Expect(old).ShouldNot(BeNil())
				mdb.Update(old, action.obj, transaction, 0)
			} else if action.tag == "del" {
				old := mdb.findByName(action.obj.Name).Step()
				Expect(old).ShouldNot(BeNil())
				mdb.Remove(old, transaction, 0)
			} else if strings.HasPrefix(action.tag, "savepoint") {
				idStr := action.tag[len("savepoint:"):]
				savepointID, _ := strconv.Atoi(idStr)
				savepoints[savepointID] = transaction.AllocSavePoint()
			} else if strings.HasPrefix(action.tag, "rollback") {
				idStr := action.tag[len("rollback:"):]
				savepointID, _ := strconv.Atoi(idStr)
				savepoints[savepointID].Rollback()
			}
		}
		transaction.Commit(0)
		Expect(len(shoulds)).Should(Equal(len(results)))
		for i, v := range results {
			k := shoulds[i]
			Expect(v.obj.Address).Should(Equal(k.obj.Address))
			Expect(v.obj.Name).Should(Equal(k.obj.Name))
			Expect(v.obj.ID1).Should(Equal(k.obj.ID1))
			Expect(v.obj.ID2).Should(Equal(k.obj.ID2))
			Expect(v.tag).Should(Equal(k.tag))
			if v.oldObj == nil {
				Expect(k.oldObj).Should(BeNil())
			} else {
				Expect(k.oldObj).ShouldNot(BeNil())
				Expect(v.oldObj.Address).Should(Equal(k.oldObj.Address))
				Expect(v.oldObj.Name).Should(Equal(k.oldObj.Name))
				Expect(v.oldObj.ID1).Should(Equal(k.oldObj.ID1))
				Expect(v.oldObj.ID2).Should(Equal(k.oldObj.ID2))
			}
		}
		for _, sp := range savepoints {
			Expect(sp.Invalid()).Should(BeTrue())
		}
	})
	It("protobuf 反射测试", func() {
		zs1 := mdb.findByName("张三1").Step().(*dbTestObj)
		pb1 := &dbTestObjPB{Name: new(string)}
		*pb1.Name = zs1.Name
		iter, err := mdb.FindByPB(pb1)
		Expect(err).Should(BeNil())
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName(zs1.Name)))

		pb2 := &dbTestObjPB{ID1: proto.Int32(zs1.ID1), ID2: proto.Int32(zs1.ID2)}
		iter, err = mdb.FindByPB(pb2)
		Expect(err).Should(BeNil())
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName(zs1.Name)))

		pb3 := &dbTestObjPB{Address: zs1.Address}
		iter, err = mdb.FindByPB(pb3)
		Expect(err).Should(BeNil())
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(iter.Step()).Should(BeNil())

		pb31 := &dbTestObjPB{ID1: proto.Int32(zs1.ID1)}
		iter, err = mdb.FindByPB(pb31)
		Expect(err).Should(BeNil())
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(iter.Step()).Should(BeNil())

		pb4 := mdb.RecordToPB(zs1, true)
		Expect(pb4).ShouldNot(BeNil())
		Expect(*pb4.(*dbTestObjPB).Name).Should(Equal(zs1.Name))
		Expect(*pb4.(*dbTestObjPB).ID1).Should(Equal(zs1.ID1))
		Expect(*pb4.(*dbTestObjPB).ID2).Should(Equal(zs1.ID2))
		Expect(pb4.(*dbTestObjPB).Address).Should(Equal(zs1.Address))

		zs2 := mdb.findByName("张三2").Step().(*dbTestObj)
		pb5 := mdb.RecordToPB(zs2, true)
		Expect(pb5).ShouldNot(BeNil())
		Expect(*pb5.(*dbTestObjPB).Name).Should(Equal(zs2.Name))
		Expect(*pb5.(*dbTestObjPB).ID1).Should(Equal(zs2.ID1))
		Expect(*pb5.(*dbTestObjPB).ID2).Should(Equal(zs2.ID2))
		Expect(pb5.(*dbTestObjPB).Address).Should(Equal(zs2.Address))

		zs2Tmp := mdb.PBToRecord(pb5, true)
		Expect(zs2Tmp).ShouldNot(BeNil())
		Expect(zs2Tmp.(*dbTestObj).Name).Should(Equal(zs2.Name))
		Expect(zs2Tmp.(*dbTestObj).ID1).Should(Equal(zs2.ID1))
		Expect(zs2Tmp.(*dbTestObj).ID2).Should(Equal(zs2.ID2))
		Expect(zs2Tmp.(*dbTestObj).Address).Should(Equal(zs2.Address))
	})

	It("从大到小排序测试", func() {
		mdb = newTestObjMDB(false)
		idxNum := mdb.addMoneyIndex()
		mdb.GetIndex(idxNum).SortGreat()
		testObjs := makeSortTestData()
		v := randomIndexs(len(testObjs))
		var ss SortByMoneyList
		for _, i := range v {
			mdb.Add(testObjs[i], nil, 0)
			ss = append(ss, testObjs[i])
		}
		sort.Sort(ss)
		obj := ss[len(ss)-1]
		iter := mdb.FindByIndex(idxNum).AppendInt32(obj.ID1).Fire()
		for i := len(ss) - 1; i >= 0; i-- {
			obj := ss[i]
			t := iter.RawStep()
			Expect(t).ShouldNot(BeNil())
			Expect(obj.Name).Should(Equal(t.(*dbTestObj).Name))
		}
	})

	It("从小到大排序测试", func() {
		mdb = newTestObjMDB(false)
		idxNum := mdb.addMoneyIndex()
		mdb.GetIndex(idxNum).SortLess()
		testObjs := makeSortTestData()
		v := randomIndexs(len(testObjs))
		var ss SortByMoneyList
		for _, i := range v {
			mdb.Add(testObjs[i], nil, 0)
			ss = append(ss, testObjs[i])
		}
		sort.Sort(ss)
		obj := ss[0]
		iter := mdb.FindByIndex(idxNum).AppendInt32(obj.ID1).Fire()
		for i := 0; i < len(ss); i++ {
			obj := ss[i]
			t := iter.RawStep()
			Expect(t).ShouldNot(BeNil())
			Expect(obj.Name).Should(Equal(t.(*dbTestObj).Name))
		}
	})

	Context("浮点数索引测试", func() {
		BeforeEach(func() {
			mdb = newTestObjMDB(false)
			mdb.AddIndex("Money", func(key *gmemdb.MdbKey, obj gmemdb.IObject) error { return key.AppendFloat64(obj.(*dbTestObj).Money) }, false)
		})
		It("顺序测试", func() {
			var ss SortByMoneyList
			testObjs := makeSortTestData()
			for _, obj := range testObjs {
				mdb.Add(obj, nil, 0)
				ss = append(ss, obj)
			}

			sort.Sort(ss)
			iter := mdb.FindByIndexName("Money").AppendFloat64(ss[0].Money).Fire()
			for i := 0; i < len(ss); i++ {
				obj := ss[i]
				t := iter.RawStep()
				Expect(t).ShouldNot(BeNil())
				Expect(obj.Name).Should(Equal(t.(*dbTestObj).Name))
			}
		})

		It("倒序测试", func() {
			var ss SortByMoneyList
			mdb.GetIndexByName("Money").SortGreat()
			testObjs := makeSortTestData()
			for _, obj := range testObjs {
				mdb.Add(obj, nil, 0)
				ss = append(ss, obj)
			}

			sort.Sort(ss)
			iter := mdb.FindByIndexName("Money").AppendFloat64(ss[len(ss)-1].Money).Fire()
			for i := len(ss) - 1; i >= 0; i-- {
				obj := ss[i]
				t := iter.RawStep()
				Expect(t).ShouldNot(BeNil())
				Expect(obj.Name).Should(Equal(t.(*dbTestObj).Name))
			}
		})
	})

	It("RAW迭代器测试：定位到最后一个迭代", func() {
		testDB := newTestObjMDB(false)
		testDB.Add(testObjs[0], nil, 0)
		testDB.Add(testObjs[1], nil, 0)
		iter := testDB.findByName("张三2")
		Expect(iter.RawNext()).Should(BeTrue())
		Expect(iter.Value().GetID()).Should(BeEquivalentTo(2))
		Expect(iter.RawNext()).Should(BeFalse())
	})

	It("非唯一索引排序测试", func() {
		// 不管是从大到小排序,还是从小到大排序,
		// 同一个key下的多个不同值必须按照插入先后排序
		testDB1 := newTestObjMDB(true)
		testDB1.GetIndex(3).SortGreat()
		for _, obj := range testObjs {
			testDB1.Add(obj, nil, 0)
		}
		iter := testDB1.findByAddress("张三地址")
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(iter.Step()).Should(BeNil())

		testDB2 := newTestObjMDB(true)
		testDB2.GetIndex(3).SortLess()
		for _, obj := range testObjs {
			testDB2.Add(obj, nil, 0)
		}
		iter = testDB2.findByAddress("张三地址")
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三1")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三2")))
		Expect(iter.Step()).Should(And(Not(BeNil()), HaveName("张三3")))
		Expect(iter.Step()).Should(BeNil())
	})

	It("迭代循环中删除数据", func() {
		testDB := newTestObjMDB(false)
		transaction := gmemdb.NewTransaction()
		iter := testDB.Begin(1)
		iter.LockDB()
		for iter.Next() {
			testDB.Remove(iter.Value(), transaction, 0)
		}
		iter.UnLockDB()
		transaction.Commit(0)
		Expect(testDB.Count()).Should(Equal(0))
	})

	Context("正负整数索引测试", func() {
		var testObjs []*dbTestObj
		minId := int32(-50)
		maxId := int32(50)
		BeforeEach(func() {
			mdb = newTestObjMDB(true)
			testObjs = testObjs[:0]
			for i := minId; i <= maxId; i++ {
				nn := fmt.Sprintf("%d", i)
				testObjs = append(testObjs, &dbTestObj{Name: nn, ID1: int32(i), ID2: 0, Address: "", Money: 0.0})
			}
		})
		It("顺序测试", func() {
			for _, obj := range testObjs {
				mdb.Add(obj, nil, 0)
			}
			iter := mdb.findByID1(minId)
			for i := minId; i <= maxId; i++ {
				t := iter.RawStep()
				Expect(t).ShouldNot(BeNil())
				Expect(t.(*dbTestObj).ID1).Should(Equal(i))
			}
		})

		It("倒序测试", func() {
			mdb.GetIndexByName("ID1|ID2").SortGreat()
			for _, obj := range testObjs {
				mdb.Add(obj, nil, 0)
			}
			iter := mdb.findByID1(maxId)
			for i := maxId; i >= minId; i-- {
				t := iter.RawStep()
				Expect(t).ShouldNot(BeNil())
				Expect(t.(*dbTestObj).ID1).Should(Equal(i))
			}
		})
	})
})
