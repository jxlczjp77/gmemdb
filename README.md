gmemdb
=========
高性能 go 内存对象存储，支持事物、事物回滚点、组合索引、多索引、非唯一索引、动作触发器、提交触发器、迭代器。
不像大多数内存数据库需要将对象序列化后再存储到底层，gmemdb直接存储 go 对象指针。

每张表（gmemdb.ObjectFactory）存储指定 go 结构的对象，该结构需要派生自gmemdb.ObjectBase，里面定义了PrimaryID作为主键，主键由 gmemdb 自动管理，主索引编号为0。
```go
type dbTestObj struct { // 定义表字段
	gmemdb.ObjectBase
	Name    string
	ID1     int32
	ID2     int32
	Address string
	Money   float64
}

type testObjMDB struct { // 定义数据表
	gmemdb.ObjectFactory
}

// 创建表
db := &testObjMDB{}

// 初始化表内容，参数(*dbTestObj)(nil), (*dbTestObjPB)(nil)用于底层将db对象转换为同字段名称的pb对象，目前dump表用的是protobuf做序列化。
db.Init("testObjMDB", (*dbTestObj)(nil), (*dbTestObjPB)(nil))

// 添加 Name 字段作为唯一索引
db.AddIndex("Name", func(key *gmemdb.MdbKey, obj gmemdb.IObject) error { return key.AppendString(obj.(*dbTestObj).Name) }, true)

iter := db.Begin(0) // 按主索引遍历
for obj := iter.Step(); obj != nil; obj = iter.Step() {
    // ...
}
```

组合索引
```go
// 添加(ID1,ID2)作为非唯一组合索引
idxNum := testDB.AddIndex("ID1|ID2", func(key *gmemdb.MdbKey, obj gmemdb.IObject) error {
    key.AppendInt32(obj.(*dbTestObj).ID1)
    key.AppendInt32(obj.(*dbTestObj).ID2)
    return nil
}, false)

// 仅给定ID1 = 1作为查询条件
iter := s.FindByIndex(idxNum)
	.AppendInt32(1) // ID1 = 1
	.Fire()
for obj := iter.Step(); obj != nil; obj = iter.Step() {
    // ...
}
```

事物支持，一个事物对象可以管理持多张表，示例仅创建了一张表。
```go
transaction := gmemdb.NewTransaction()
zs1 := mdb.findByName("张三1").Step().(*dbTestObj)
zs2 := mdb.findByName("张三2").Step().(*dbTestObj)
zs3 := mdb.findByName("张三3").Step().(*dbTestObj)
mdb.Remove(zs1, transaction, 0) // 移除 张三1 对象
Expect(mdb.findByName("张三1").Step()).Should(BeNil())

savePoint1 := transaction.AllocSavePoint() // 插入事物回滚点
mdb.Remove(zs2, transaction, 0) // 移除 张三2 对象
Expect(mdb.findByName("张三2").Step()).Should(BeNil())

savePoint2 := transaction.AllocSavePoint() // 插入事物回滚点
mdb.Remove(zs3, transaction, 0)
Expect(mdb.findByName("张三3").Step()).Should(BeNil())

savePoint2.Rollback() // 回滚到 savePoint2 之前
Expect(mdb.findByName("张三3").Step()).Should(And(Not(BeNil()), HaveName("张三3")))

savePoint1.Rollback() // 回滚到 savePoint1 之前
Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveName("张三2")))

transaction.Commit(0) // 提交事物
Expect(mdb.findByName("张三1").Step()).Should(BeNil())
Expect(mdb.findByName("张三2").Step()).Should(And(Not(BeNil()), HaveName("张三2")))
Expect(mdb.findByName("张三3").Step()).Should(And(Not(BeNil()), HaveName("张三3")))
```

迭代器
=========
```go
// Step 迭代所有 ID1 = 1 的对象
iter := s.FindByIndex(idxNum)
	.AppendInt32(1) // ID1 = 1
	.Fire()
for obj := iter.Step(); obj != nil; obj = iter.Step() {
    // ...
}

// RawStep 迭代所有 ID1 = 1 的对象，超出范围后继续迭代到数据表末尾
iter := s.FindByIndex(idxNum)
	.AppendInt32(1) // ID1 = 1
	.Fire()
for obj := iter.RawStep(); obj != nil; obj = iter.RawStep() {
    // ...
}
```

动作触发器，提交触发器：支持动作合并，事物中对同一个对象的多次操作会被合并为一个或多个动作。
```go
It("提交触发器测试", func() {
	transaction := gmemdb.NewTransaction()
	// 构建测试用例
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
	add := func(fid uint32, obj gmemdb.IObject, reason int32) { // 对象插入回调函数
		t := obj.(*dbTestObj)
		results = append(results, Action{"add", t, nil})
	}
	upd := func(fid uint32, obj gmemdb.IObject, newObj gmemdb.IObject, reason int32) { // 对象更新回调函数
		results = append(results, Action{"upd", newObj.(*dbTestObj), obj.(*dbTestObj)})
	}
	del := func(fid uint32, obj gmemdb.IObject, reason int32) { // 对象删除回调函数
		t := obj.(*dbTestObj)
		results = append(results, Action{"del", t, nil})
	}
	trigger := gmemdb.MakeCommitTrigger(add, upd, del) // 创建提交触发器
	mdb.AddCommitTrigger(trigger) // 添加提交触发器到数据表
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
	transaction.Commit(0) // 提交事物
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
```

iradix
=========
iradix 是 [radix tree](http://en.wikipedia.org/wiki/Radix_tree) 的 immutable 实现，支持 key 顺序和倒序排列，前缀查询，支持事物、事物回滚点、迭代器，这里用作 gmemdb 的索引。为减轻GC压力，iradix 默认会收集临时节点供后续使用，实测能大大降低GC负担，性能提高近10倍。iradix 不允许在迭代循环中删除对象，删除的对象被重用可能会损坏迭代器。如果需要在迭代循环中删除对象，可以调用txn.LockDB临时禁止回收功能，循环结束后调用txn.UnLockDB重新启用回收功能。

```go
testDB := newTestObjMDB(false)
transaction := gmemdb.NewTransaction()
iter := testDB.Begin(1)
iter.LockDB() // 禁止回收临时对象，允许迭代循环中删除对象
for iter.Next() {
    testDB.Remove(iter.Value(), transaction, 0)
}
iter.UnLockDB()
transaction.Commit(0)
Expect(testDB.Count()).Should(Equal(0))
```

性能测试
=========
普通PC
i5-4460 cpu 3.20GHZ
16.0 GB内存
windows 10
```
Running Suite: 内存表测试
==============================
Random Seed: 1576204706
Will run 25 of 25 specs

+++++++++++++++++++++++
------------------------------
+ [MEASUREMENT]
  Ran 5 samples:
  Find性能测试:
    Fastest Time: 0.239s
    Slowest Time: 0.284s
    Average Time: 0.264s ± 0.015s
  Find性能测试(条 / 每秒):
    Smallest: 352313.520
     Largest: 418649.846
     Average: 380145.286 ± 22291.783
  删除性能测试:
    Fastest Time: 0.952s
    Slowest Time: 1.057s
    Average Time: 0.987s ± 0.039s
  删除性能测试(条 / 每秒):
    Smallest: 94571.832
     Largest: 104991.528
     Average: 101434.354 ± 3829.916
------------------------------
+ [MEASUREMENT]
  Ran 5 samples:
  无事物插入耗时:
    Fastest Time: 0.855s
    Slowest Time: 0.961s
    Average Time: 0.900s ± 0.038s
  无事物插入速度(条 / 每秒):
    Smallest: 104009.526
     Largest: 117025.146
     Average: 111267.841 ± 4605.469
  每1000条提交插入耗时:
    Fastest Time: 0.312s
    Slowest Time: 0.395s
    Average Time: 0.344s ± 0.032s
  每1000条提交插入速度(条 / 每秒):
    Smallest: 253309.617
     Largest: 320695.062
     Average: 292971.260 ± 25587.870
  每10000条提交插入耗时:
    Fastest Time: 0.295s
    Slowest Time: 0.356s
    Average Time: 0.329s ± 0.020s
  每10000条提交插入速度(条 / 每秒):
    Smallest: 280659.449
     Largest: 339176.094
     Average: 305177.774 ± 19082.366
------------------------------

Ran 25 of 25 Specs in 24.508 seconds
SUCCESS! -- 25 Passed | 0 Failed | 0 Pending | 0 Skipped
PASS
```