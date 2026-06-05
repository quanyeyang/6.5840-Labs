package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

// 主要在这个文件实现raft算法
import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
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
	votedFor    int        // 给哪个candidate投票了
	log         []LogEntry // 包含term和要执行的指令

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
}

// ====== 工具方法 ======
// 上锁，防止race
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.currentState == leader
}

// 需要是1-base么？此处要注意
func (rf *Raft) GetLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) GetLastLogTerm() int64 {
	return rf.log[rf.GetLastLogIndex()].Term
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	PrevLogIndex int64      // 上个最后log的index
	PrevLogTerm  int64      // 上个最后log的term
	Entries      []LogEntry // 需要追加的日志,如果是空，那就是心跳检测
	LeaderCommit int64      // leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int64
	Success bool // follower是否和prevLog Term & Index是匹配的
}

// 我是follower,当有candidate想让我投票的时候，我该如何处理？
// 锁在这里是什么情况？这个视角应该就是我看args,然后填reply即可
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
	}

	// 任期和我相同(包括刚升级结束的)
	// 我投票了，但是本来就没给你投
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 处理log,是否够新

	// ok
	rf.votedFor = args.CandidateId
	// 这次选票成功，也认为是一次压制
	rf.lastRPC = time.Now()
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// 我是follower,当leader给我日志的时候我应该如何处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader 但是term比我小，肯定不行
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 比我更大
	if args.Term >= rf.currentTerm {
		rf.currentState = follower
		rf.currentTerm = args.Term
		rf.lastRPC = time.Now() // 本次我确实被成功压制了
	}

	// 检查日志一致性，3A可以不做？
	reply.Term = rf.currentTerm
	reply.Success = true
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

// 暂时写一个全员发送心跳的方法,这就是leader的视角
// 普通函数，默认调用方不应该持有lock,自己管理
func (rf *Raft) startHeartBeat() {

	rf.mu.Lock()

	if rf.currentState != leader {
		rf.mu.Unlock()
		return
	}
	// 应该留快照,不能让多线程竞争
	currentTerm := rf.currentTerm
	leaderId := rf.me
	prevLogIndex := rf.GetLastLogIndex()
	prevLogTerm := rf.GetLastLogTerm()
	leaderCommit := rf.commitIndex

	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(p int) {
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     leaderId,
				PrevLogIndex: int64(prevLogIndex),
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogEntry{},
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
				return
			}

			// 我现在还是leader 并且对方也回复成功了
			if rf.currentState == leader && reply.Success {
				// 处理log index
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

	// 两组维护的数据进行初始化
	for index := 0; index < len(rf.nextIndex); index++ {
		rf.nextIndex[index] = int64(len(rf.log) + 1)
	}

	for index := 0; index < len(rf.matchIndex); index++ {
		rf.matchIndex[index] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 超时选举
	rf.lastRPC = time.Now()
	rf.lastHeartBeat = time.Now()
	// 这里按me给每个peer设置不同的种子
	rf.r = rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*1000))

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
