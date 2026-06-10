package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

// 主要在这个文件实现raft算法
import (
	"bytes"

	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// 每个peer的状态枚举
const (
	follower  = 0
	candidate = 1
	leader    = 2
)

const (
	MinElectionTimeout = 150 * time.Millisecond
	MaxElectionTimeout = 300 * time.Millisecond
)

// randomTimeout 返回一个在 [Min, Max) 之间的随机时间
// 必须在持有锁的情况下调用，或者 rf.r 仅在单线程/特定 goroutine 中使用
func (rf *Raft) randomTimeout() time.Duration {
	// Max - Min 得到随机区间的长度 (这里是 150ms)
	extraRange := int64(MaxElectionTimeout - MinElectionTimeout)

	// rf.r.Int63n(N) 返回 [0, N) 之间的随机整数
	randomExtra := rf.r.Int63n(extraRange)

	// 基准时间 + 随机漂移量
	return MinElectionTimeout + time.Duration(randomExtra)
}

type LogEntry struct {
	Term    int64
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 根据论文的结构:
	// 持久性状态
	currentTerm int64      // 当前所处的任期
	votedFor    int        // 给哪个candidate投票了,防止恢复之后给旧leader投票，破坏语义
	log         []LogEntry // 包含term和要执行的指令,log肯定需要做持久化,之后还会压缩和恢复之类的.

	// 当我们实现日志压缩之后,这里slice的index就不等于log的逻辑index了,还需要持久化和快照相关的状态
	lastIncludedIndex int64  // 快照已经覆盖的最高日志的index,裁掉前缀后，rf.log 只存 lastIncludedIndex 之后 的 entry。
	lastIncludedTerm  int64  // 上面这条日志的term
	snapshot          []byte // 当前快照的字节

	// 易失性状态
	commitIndex  int64 // 最高已经提交的log index
	lastApplied  int64 // 最高已经执行的log index 把这个两个分开就是把本地执行和回复client的操作进行解耦
	currentState int   // 当前peer的状态

	// leader持有的状态,应该在选举之后进行初始化
	nextIndex     []int64   // 对于每个server,维护下一个要发送过去的index(回退的时候使用)
	matchIndex    []int64   // 已知最高已经复制过去的index
	lastHeartBeat time.Time // leader上次发心跳是什么时候

	// 超时选举
	lastRPC time.Time
	r       *rand.Rand

	// 和上层应用进行通信 协程间的通信手法
	applyCh chan raftapi.ApplyMsg
}

// ====== 工具方法 ======
// 上锁，防止race
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.currentState == leader
}

// 需要是1-base么？此处要注意
// 为空的时候，返回的就是0 这里是逻辑的日志编号
func (rf *Raft) GetLastLogIndex() int {
	return int(rf.lastIncludedIndex) + len(rf.log) - 1
}

// 根据逻辑下表获取真实的物理下标
func (rf *Raft) physicalIndex(index int) int {
	if index > int(rf.lastIncludedIndex) {
		return index - int(rf.lastIncludedIndex)
	} else if index == int(rf.lastIncludedIndex) {
		// dummy log
		return 0
	} else {
		return -1
	}
}

func (rf *Raft) GetLastLogTerm() int64 {
	return rf.log[rf.physicalIndex(rf.GetLastLogIndex())].Term
}

// 根据逻辑日志index获取日志
// 如果不存在，返回空日志
func (rf *Raft) logAt(logical int) LogEntry {
	pIndex := rf.physicalIndex(logical)
	if pIndex != -1 {
		return rf.log[pIndex]
	} else {
		return LogEntry{}
	}
}

// PrevLogIndex 的 term；若 logical == lastIncludedIndex 用 lastIncludedTerm
func (rf *Raft) logTerm(logical int) int64 {
	if logical == int(rf.lastIncludedIndex) {
		return rf.lastIncludedTerm
	} else {
		return rf.log[rf.physicalIndex(logical)].Term
	}
}

// 根据commitIndex进行状态机的应用
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		// 最后应用小于最后提交,那么我们可以进行状态机的应用
		if rf.lastApplied < rf.commitIndex {
			// 最后应用的位置是大于快照边界的,证明此时没有applied的entry还在log中
			rf.lastApplied++
			if rf.lastApplied > rf.lastIncludedIndex {

				index := rf.lastApplied
				cmd := rf.logAt(int(index)).Command
				rf.mu.Unlock()

				// 给上层应用发送消息
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      cmd,
					CommandIndex: int(index),
				}
			} else {
				// 否则的话,此时的entry就在快照中，需要构造一个快照应用
				rf.lastApplied = rf.lastIncludedIndex
				snapshot := bytes.Clone(rf.snapshot)
				lastIncludedTerm := rf.lastIncludedTerm
				lastIncludedIndex := rf.lastIncludedIndex
				rf.mu.Unlock()

				rf.applyCh <- raftapi.ApplyMsg{
					SnapshotValid: true,
					Snapshot:      snapshot,
					SnapshotTerm:  int(lastIncludedTerm),
					SnapshotIndex: int(lastIncludedIndex),
				}
			}

		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// 我们被投票的时候，需要首先检查来自candidate的日志是否是更新的
func (rf *Raft) isLogUpToDate(lastLogIndex int64, lastLogTerm int) bool {
	myIndex := int64(rf.GetLastLogIndex())
	myTerm := rf.GetLastLogTerm()

	if lastLogTerm != int(myTerm) {
		return lastLogTerm > int(myTerm)
	}

	return lastLogIndex >= myIndex
}

// 返回 leader log 中 term 最后出现的 logical index；没有则 -1
// 持有锁的情况下进行调用
func (rf *Raft) lastIndexOfTerm(term int64) int64 {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return rf.lastIncludedIndex + int64(i)
		}
	}
	return -1
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
// 按顺序序列化成字节流,然后写入磁盘,之后再进行恢复
// 持久化状态的时候应该持有lock
// 实现快照的时候，后面的snapshot也需要被保存
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
// 从之前保存的字节流中恢复状态
func (rf *Raft) readPersist(data []byte) {
	// 没有传输数据的情况
	if data == nil {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int64
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int64
	var lastIncludedTerm int64

	// 反序列化失败
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
	// 同时此处还需要恢复快照
	rf.snapshot = rf.persister.ReadSnapshot()
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 上层服务已经创建快照，我们应该进行对应的裁剪 -> 日志压缩
// 这里的index就是当前的快照包含的最高log持有的index
// 裁剪的时候肯定需要持有lock
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 快照的最高点没有落在我的窗口之内
	if index <= int(rf.lastIncludedIndex) || index > rf.GetLastLogIndex() {
		return
	} else {
		// 触发截断的逻辑
		lastIncludedTerm := rf.logTerm(index)
		lastIncludedIndex := int64(index)
		rf.snapshot = bytes.Clone(snapshot)

		// 裁剪
		pIndex := rf.physicalIndex(index)
		// 此时前面还是存在一个dummyNode节点
		rf.log = append([]LogEntry{{Term: lastIncludedTerm}}, rf.log[pIndex+1:]...)
		// 你现在把这个快照提交了，相当于这个状态机已经被应用了
		if rf.commitIndex < lastIncludedIndex || rf.lastApplied < lastIncludedIndex {
			rf.commitIndex = lastIncludedIndex
			rf.lastApplied = lastIncludedIndex
		}
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.persist()
	}

}

// 实现candidate请求投票的RPC
type RequestVoteArgs struct {
	Term         int64 // 候选人当前任期
	CandidateId  int   // 候选人ID
	LastLogIndex int64 // 最后log的index
	LastLogTerm  int64 //  最后log的term,这两个参数就用来比较日志新旧
}

type RequestVoteReply struct {
	Term        int64 // 回应者的任期
	VoteGranted bool  // 是否投票了
}

// 实现leader追加日志和心跳检测的RPC
type AppendEntriesArgs struct {
	Term         int64      // leader当前任期
	LeaderId     int        // 当前leader的id
	PrevLogIndex int64      // 上个最后log的index 这两个参数相当于用来做前缀匹配(我们在论文中提到过)
	PrevLogTerm  int64      // 上个最后log的term
	Entries      []LogEntry // 需要追加的日志,如果是空，那就是心跳检测
	LeaderCommit int64      // leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int64
	Success bool // follower是否和prevLog Term & Index是匹配的

	// 发生冲突的时候做fastback
	XTerm  int64 // 冲突的term
	XIndex int64 // term冲突的时候,此时log上这个term的第一条index
	XLen   int64 // 当follower的log太短的时候 此时follower log的具体长度
}

// 实现leader给follower发送快照的RPC
// 理论上需要分chunk,但是我们这里就一次性发完看看
type InstallSnapshotArgs struct {
	Term              int64
	LeaderId          int
	LastIncludedIndex int64
	LastIncludedTerm  int64
	Data              []byte // 字节数组
}

type InstallSnapshotReply struct {
	Term int64 // follower的currentTerm
}

// 我是follower,当我拿到一个来自于leader的快照的时候我应该如何进行处理
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentState = follower
		rf.votedFor = -1

		rf.persist()
		reply.Term = rf.currentTerm
	}

	// 这种可能就是重复RPC导致的
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return // reply.Term 已设即可
	}

	// 保存这些持久化状态
	rf.snapshot = bytes.Clone(args.Data)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// 拿到快照之后，就清理掉现在的状态
	rf.log = []LogEntry{{Term: rf.lastIncludedTerm}}

	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}

	rf.persist()
	rf.lastRPC = time.Now()
}

// 我是follower,当有candidate想让我投票的时候，我该如何处理？
// 这个视角应该就是我看args,然后填reply即可
// 在相同任期下，你才能进行投票的操作
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 任期比我更小，不能给你投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 任期比我更大，我先升级一下自己
	if args.Term > rf.currentTerm {
		rf.currentState = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1 // 清理一下本次term的投票

		rf.persist()
	}

	// 任期和我相同(包括刚升级结束的)
	// 我投票了，但是本来就没给你投
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 处理log,是否够新,这里我们先看Term,然后看Log
	// 实际工程有预投票的实现，比较有意思，我们这里不管
	if !rf.isLogUpToDate(args.LastLogIndex, int(args.LastLogTerm)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// ok
	rf.votedFor = args.CandidateId

	// 因为进行了一次投票所以需要持久化
	rf.persist()
	// 这次选票成功，也认为是一次压制
	rf.lastRPC = time.Now()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// 我是follower,当leader给我日志的时候我应该如何处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader 但是term比我小，肯定不行,并且我也不会加上你的日志
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 比我更大，我先把自己的Term调整和伟大的leader一样
	if args.Term >= rf.currentTerm {
		rf.currentState = follower
		rf.currentTerm = args.Term
		rf.lastRPC = time.Now() // 本次我确实被成功压制了
	}

	reply.Term = rf.currentTerm

	// 日志的一致性检查，这是论文中提到的, 对应log的index和term都相同，前缀就应该完全相同
	if args.PrevLogIndex > int64(rf.GetLastLogIndex()) {
		// follower的index太小
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		// fast backup 利用逻辑末尾index来处理
		reply.XLen = int64(rf.GetLastLogIndex())
		return
	}

	// 这里就是我希望leader直接给我发快照
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.lastIncludedIndex
		return
	}

	myLogTerm := rf.logTerm(int(args.PrevLogIndex))

	if myLogTerm != args.PrevLogTerm {
		// follower对应位置的term对不上号
		reply.Success = false
		reply.XTerm = myLogTerm
		reply.XIndex = args.PrevLogIndex
		// 往前遍历,找这个term下的第一条index
		for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
			if rf.logTerm(int(index)) != myLogTerm {
				reply.XIndex = index + 1
				break
			}
		}
		reply.XLen = int64(rf.GetLastLogIndex())
		return
	}

	// leader之前来的两个prev index和term都能对上号
	index := args.PrevLogIndex + 1
	// 遍历传递过来的所有entry
	// 注意循环的写法
	for _, entry := range args.Entries {
		pIndex := rf.physicalIndex(int(index))
		if int(pIndex) < len(rf.log) {
			if rf.log[pIndex].Term != int64(entry.Term) {
				// 真的不相同，log需要进行截断
				rf.log = rf.log[:pIndex]
				rf.log = append(rf.log, entry)
			}
			// 相同就是重复的了，那就是真的相同
		} else {
			rf.log = append(rf.log, entry)
		}
		index++
	}

	// 等到日志写完了之后再做持久化
	rf.persist()

	// 既然新写入了日志，我下来更新我自己的commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := args.PrevLogIndex + int64(len(args.Entries))
		rf.commitIndex = min(args.LeaderCommit, lastNewIndex)
	}

	reply.Success = true
}

// 收到来自于client的回复之后来尝试增加leader的commit
// 默认持有lock的时候调用
func (rf *Raft) TryCommit() {
	for index := rf.GetLastLogIndex(); index > int(rf.lastIncludedIndex); index-- {
		// 这里还需要注意的是前缀提交的问题，就是我只能提交term和我本次日志相同的entry（前缀安全性）
		if rf.logAt(index).Term != rf.currentTerm {
			// 先break,你自己任期先至少记录一个log再说
			break
		}
		matchServers := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}

			if rf.matchIndex[peer] >= int64(index) {
				matchServers++
			}
		}
		// 已经被多数派认可，可以提交这个日志
		if matchServers >= len(rf.peers)/2+1 {
			rf.commitIndex = int64(index)
			break
		}
	}
}

// 调用时候不能持有lock
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 给goroutine抽象出来的方法，防止race
func goGetlogTermFromCopyLog(prevLogIndex int64, lastIncludedIndex int64, lastIncludedTerm int64, logCopy []LogEntry) int64 {
	if prevLogIndex == lastIncludedIndex {
		return lastIncludedTerm
	} else {
		return logCopy[prevLogIndex-lastIncludedIndex].Term
	}
}

// 计算一下物理index
func goGetPhysicalIndex(logical int64, lastIncluded int64) int64 {
	if logical < lastIncluded {
		return -1
	} else {
		return logical - lastIncluded
	}
}

// 暂时写一个全员发送心跳的方法,这就是leader的视角
// 普通函数，默认调用方不应该持有lock,自己管理
func (rf *Raft) startHeartBeat() {

	rf.mu.Lock()

	if rf.currentState != leader {
		rf.mu.Unlock()
		return
	}
	// 应该留快照,不能让多线程竞争
	// 注意数组深拷贝
	nextIndex := make([]int64, len(rf.nextIndex))
	copy(nextIndex, rf.nextIndex)
	currentTerm := rf.currentTerm
	leaderId := rf.me
	leaderCommit := rf.commitIndex

	lastIncludedIndex := rf.lastIncludedIndex
	lastIncludedTerm := rf.lastIncludedTerm
	logCopy := append([]LogEntry{}, rf.log...)
	snapshot := bytes.Clone(rf.snapshot)

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			// 根据现在维护的状态,到底是发日志还是发快照
			if nextIndex[p] <= lastIncludedIndex {
				args := InstallSnapshotArgs{
					Term:              currentTerm,
					LeaderId:          leaderId,
					LastIncludedIndex: lastIncludedIndex,
					LastIncludedTerm:  lastIncludedTerm,
					Data:              snapshot,
				}

				reply := InstallSnapshotReply{}
				ok := rf.sendInstallSnapshot(p, &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.currentState = follower
					rf.votedFor = -1
					rf.persist()
					return
				}

				if rf.currentState == leader && rf.currentTerm == currentTerm {
					rf.nextIndex[p] = lastIncludedIndex + 1
					rf.matchIndex[p] = lastIncludedIndex
					rf.TryCommit()
				}
				return
			}

			prevLogIndex := nextIndex[p] - 1

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  goGetlogTermFromCopyLog(prevLogIndex, lastIncludedIndex, lastIncludedTerm, logCopy),
				Entries:      logCopy[int(goGetPhysicalIndex(nextIndex[p], lastIncludedIndex)):], // 按nextIndex发送之后所有的log日志,这里需要先计算一个物理下标
				LeaderCommit: leaderCommit,
			}

			reply := AppendEntriesReply{}
			rf.sendAppendEntries(p, &args, &reply)

			// 我们拿到回复之后
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 我压制你，但是你term居然比我还大
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentState = follower
				rf.votedFor = -1
				rf.persist()
				return
			}

			// 我现在还是leader 并且对方也回复成功了 同此此时的Term还没有发生变化
			sentEntriesLength := int64(len(args.Entries))
			if rf.currentState == leader && reply.Success && rf.currentTerm == currentTerm {
				rf.matchIndex[p] = args.PrevLogIndex + sentEntriesLength
				rf.nextIndex[p] = rf.matchIndex[p] + 1
				// 提交成功一次，尝试更改leader的对应的commitIndex
				rf.TryCommit()
			}

			if rf.currentState == leader && reply.Success == false && rf.currentTerm == currentTerm {
				// 这里实现fastback优化
				// 1.follower的日志太短
				if reply.XTerm == -1 {
					rf.nextIndex[p] = reply.XLen
				} else {
					// 2.此时index对上了，但是Term没对上
					lastIndex := rf.lastIndexOfTerm(reply.XTerm)

					if lastIndex == -1 {
						rf.nextIndex[p] = reply.XIndex
					} else {
						rf.nextIndex[p] = lastIndex + 1
					}
				}

				rf.nextIndex[p] = max(rf.nextIndex[p], 1)
			}

		}(peer)
	}
}

// 超时，发起一次大选举
// 普通函数，默认调用方不应该持有lock,自己管理
func (rf *Raft) startElection() {

	rf.mu.Lock()

	// 此时持有lock,修改状态
	rf.currentState = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastRPC = time.Now()

	rf.persist()

	// 记录此时的日志状态等 快照 不要读共享状态
	termAtStart := rf.currentTerm
	lastLogIndex := rf.GetLastLogIndex()
	lastLogTerm := rf.GetLastLogTerm()
	grantedVotes := 1

	// 解锁
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		// 通过传参捕获局部变量
		go func(p int) {

			// 组装参数,准备并发goroutine
			args := RequestVoteArgs{
				Term:         termAtStart,
				CandidateId:  rf.me,
				LastLogIndex: int64(lastLogIndex),
				LastLogTerm:  lastLogTerm,
			}

			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(p, &args, &reply)
			// RPC失败，网络问题
			if !ok {
				return
			}

			// 成功调用,需要暂时持有lock
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 1.若对方的term更高
			if reply.Term > rf.currentTerm {
				rf.currentState = follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1

				rf.persist()
				return
			}

			// 2.等待选举期间过期（比如收到了来自合法的leader的心跳）
			if rf.currentState != candidate {
				return
			}

			// 3.Term是否过期
			if rf.currentTerm != termAtStart {
				return
			}

			// 统计票数，对方是否给我投票了
			if reply.VoteGranted {
				grantedVotes++

				if grantedVotes > len(rf.peers)/2 {
					rf.currentState = leader
					// 我是leader了，现在又要重新维护大家
					for index := 0; index < len(rf.peers); index++ {
						rf.nextIndex[index] = int64(rf.GetLastLogIndex()) + 1
						rf.matchIndex[index] = 0
					}
					// 选举成leader之后应该立刻全部发送心跳进行压制
					rf.mu.Unlock()
					rf.startHeartBeat()
					rf.mu.Lock()
				}
			}

		}(peer)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 这个函数的意义就相当于client发来了一条日志
// 注意log确实应该是1-base的.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := int(rf.currentTerm)
	if rf.currentState != leader {
		return -1, term, false
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.log = append(rf.log, entry)

	// 加上日志之后赶紧持久化
	rf.persist()

	index := rf.GetLastLogIndex()
	// 添加日志之后就赶紧全员发送日志
	go rf.startHeartBeat()
	return index, term, true
}

func (rf *Raft) ticker() {
	for true {
		ms := 10 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond) // 这里先小睡一下

		rf.mu.Lock()
		elapsed := time.Since(rf.lastRPC)
		timeout := rf.randomTimeout()

		if rf.currentState != leader && elapsed >= timeout {
			// 此时真的发生了选举超时的问题,我现在需要发起选举了
			rf.mu.Unlock()
			rf.startElection()
			rf.mu.Lock()
		}

		if rf.currentState == leader {
			// leader应该周期性调用，暂时是心跳包
			// 10s之内不要调用超过十次心跳
			if time.Since(rf.lastHeartBeat).Milliseconds() > 100 {
				rf.mu.Unlock()
				rf.startHeartBeat()
				rf.mu.Lock()
				rf.lastHeartBeat = time.Now()
			}
		}

		// 最佳实践肯定是自己的上的锁，就在本方法内部解掉.
		// 锁契约一定要解耦
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// 初始化状态
	rf.currentState = follower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1 // 默认未进行投票的操作
	// 不能留空，放一个dummy log占位
	rf.log = []LogEntry{{Term: 0}}
	// 这里要make初始化
	rf.nextIndex = make([]int64, len(rf.peers))
	rf.matchIndex = make([]int64, len(rf.peers))

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// 通信channel
	rf.applyCh = applyCh

	// 两组维护的数据进行初始化
	for index := 0; index < len(rf.nextIndex); index++ {
		rf.nextIndex[index] = int64(rf.GetLastLogIndex()) + 1
	}

	for index := 0; index < len(rf.matchIndex); index++ {
		rf.matchIndex[index] = 0
	}

	// 初始化完毕,看看是否存在可以恢复的部分
	rf.readPersist(persister.ReadRaftState())
	rf.lastApplied = rf.lastIncludedIndex

	// 超时选举
	rf.lastRPC = time.Now()
	rf.lastHeartBeat = time.Now()
	// 这里按me给每个peer设置不同的种子
	rf.r = rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*1000))

	// start ticker goroutine to start elections
	go rf.ticker()

	// 起这个提交状态机的协程
	go rf.applier()

	return rf
}
