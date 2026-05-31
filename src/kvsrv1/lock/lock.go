package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

// 用刚才单机的kv server来实现一个lock
// 最原始的想法是CAS,具体应该如何实现，每一个lock都相当于是在内存中的一对key value.
// 这里应该是多个client来竞争lock
type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string // 锁名称 key
	id       string // 哪个client占有了lock value,所以client也需要有一个name?
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
// 每个client会持有这样一个对象，但是刚创建的时候肯定还没有放在内存中去
// 你可以想象，这个区域就相当于是内存中的client对象持有的一个区域，我们站在KV存储池的边上尝试进行对比和替换的操作
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		id:       kvtest.RandValue(8), // 创建的时候给一个随机名称
	}
	return lk
}

func (lk *Lock) Acquire() {
	// CAS 循环尝试获取lock
	for {
		// 获取当前lock的所有信息
		id, version, err := lk.ck.Get(lk.lockname)

		// lock不存在
		if err == rpc.ErrNoKey {
			// 在内存中放一个
			if lk.ck.Put(lk.lockname, lk.id, 0) == rpc.OK {
				return
			} else {
				// 可能瞬间被占有了，然后进行重试
			}

		}

		// 可能丢包了
		if err != rpc.OK {
			continue
		}

		// 获取到了，此时没有client持有
		if id == "" {
			if lk.ck.Put(lk.lockname, lk.id, version) == rpc.OK {
				return
			} else {
				continue
			}
		}

		// 此时可能被自己持有，“可重入锁”
		if lk.id == id {
			return
		}

		// 被其他client获取，重新自旋
	}
}

func (lk *Lock) Release() {
	for {
		id, version, err := lk.ck.Get(lk.lockname)

		if err != rpc.OK || id != lk.id {
			// 不是你自己的lock,所以不能进行释放
			return
		}

		// 这里应该进行清理的操作
		if lk.ck.Put(lk.lockname, "", version) == rpc.OK {
			return
		} else {
			continue
		}
	}
}
