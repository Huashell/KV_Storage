package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sort"

	"6.5840/labgob"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	ELECTION_TIMEOUT = 200
	HEARTBEAT        = 100 * time.Millisecond
)

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int // 给lab3使用

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	currentTerm int     // raft节点当前的周期
	votedFor    int     // 在当前获得选票的候选⼈的Id
	logs        []Entry // 复制日志队列
	leaderId    int     // 当前领导人的Id
	state       State   // 本节点的角色

	electionTimer  *time.Ticker // 选举计时器
	heartbeatTimer *time.Ticker // 心跳包计时器
	// 2B
	commitIndex  int   // 已知的最大被提交的日志条目的索引值
	lastApplied  int   // 最后被应用到状态机的日志条目索引值
	nextIndex    []int // 对于每⼀个服务器，需要发送给他的下⼀个日志条目的索引值（初始化为领导⼈最后索引值加⼀）
	matchIndex   []int // 对于每⼀个服务器，已经复制给他的日志的最高索引值
	applyCh      chan ApplyMsg
	applyMsgCond *sync.Cond // 提交日志到上层应用的条件变量 只有在成功提交新的日志的时候才会触发
	isAppendLog  uint32     // 是否正在加入新日志到applyCh
}

type Entry struct {
	Command interface{} // 日志中包含的命令
	Term    int         // 日志在哪个周期中产生的
	Index   int         // 日志的索引
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	rf.votedFor = -1
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (args.Term > rf.currentTerm || args.Term == rf.currentTerm) && rf.isLogMatch(args.LastLogIndex, args.LastLogTerm) {
		//原为args.Term > rf.currentTerm || args.Term == rf.currentTerm && rf.isLogMatch(args.LastLogIndex, args.LastLogTerm)
		//一直无法通过TestBackup2B
		if rf.state != FOLLOWER {
			rf.changeState(FOLLOWER)
		}
		rf.currentTerm = args.Term
		rf.resetElectionTimeout()
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

}

func (rf *Raft) requestVotes() {
	rf.votedFor = rf.me
	rf.currentTerm += 1
	acquireVotes := 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	//使用循环调用rpc可能受网络影响导致一直阻塞，使用协程可以达到异步的目的
	// for peer :=  range rf.peers {
	// 	rf.mu.Lock()
	// 	if peer != rf.me {
	// 		reply := &RequestVoteReply{}
	// 		rf.peers[peer].Call("Raft.RequestVote", args, reply)
	// 		if reply.VoteGranted {
	// 			acquireVotes++
	// 			if acquireVotes >= len(rf.peers) / 2{
	// 				rf.changeState(LEADER)
	// 				rf.leaderId = rf.me
	// 			}
	// 		} else if reply.Term > rf.currentTerm{
	// 		}
	// 	}
	// 	rf.mu.Unlock()
	// }
	hostnum := len(rf.peers)
	rf.persist()
	for peer := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				reply := &RequestVoteReply{}
				if rf.peers[peer].Call("Raft.RequestVote", args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.state == CANDIDATE && rf.currentTerm == args.Term {
						if reply.VoteGranted {
							acquireVotes++
							if acquireVotes > hostnum/2 {
								rf.changeState(LEADER)
								rf.sendEntries()
							}
						}
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.changeState(FOLLOWER)
						}
					}
				}
			}(peer)
		}
	}
}

// AppendEntriesArgs
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Logs         []Entry
	LeaderCommit int
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if rf.state != FOLLOWER {
		rf.changeState(FOLLOWER)
	}
	rf.currentTerm = args.Term
	rf.persist()

	if len(args.Logs) == 0 {
		//心跳
		rf.resetElectionTimeout()
	}

	if args.PreLogIndex > rf.getLastLog().Index || rf.logs[args.PreLogIndex-rf.getFirstLog().Index].Term != args.PreLogTerm {
		reply.Success, reply.Term = false, rf.currentTerm
		lastIndex := rf.getLastLog().Index
		firstIndex := rf.getFirstLog().Index
		if args.PreLogIndex > lastIndex {
			//PreLogIndex超越了当前主机的最新
			reply.XIndex, reply.XTerm = lastIndex+1, -1
		} else {
			//PreLogIndex对应的log的term在leader和当前主机中不相同
			reply.XTerm = rf.logs[args.PreLogIndex-firstIndex].Term
			index := args.PreLogIndex
			for index >= firstIndex && rf.logs[index-firstIndex].Term == reply.XTerm {
				index--
			}
			reply.XIndex = index
		}
		return
	}
	ind := rf.cropLogs(args)
	//DPrintf("follower %d accept log from index=%d to %d", rf.me)
	rf.logs = append(rf.logs, args.Logs[ind:]...)
	newCommitIndex := min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyMsgCond.Signal()
	}
	reply.Success, reply.Term = true, rf.currentTerm
}

func (rf *Raft) sendEntries() {
	for i := range rf.peers {
		if rf.me != i {
			go func(peer int) {
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}

				firstIndex := rf.getFirstLog().Index
				if rf.nextIndex[peer] <= firstIndex {
					args := rf.newInstallSnapshotArgs()
					rf.mu.Unlock()

					reply := &InstallSnapshotReply{}
					if rf.peers[peer].Call("Raft.InstallSnapshot", args, reply) {
						rf.mu.Lock()
						rf.handleInstallSnapshotResponse(peer, args, reply)
						rf.mu.Unlock()
					}
				} else {
					args := rf.newAppendEntriesArgs(peer)
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					DPrintf("leader %d send logs to %d", rf.me, peer)
					if rf.peers[peer].Call("Raft.AppendEntries", args, reply) {
						rf.mu.Lock()
						rf.handleAppendEntriesResponse(peer, args, reply)
						rf.mu.Unlock()
					}
				}

			}(i)
		}
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A
	if rf.state == LEADER && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.changeState(FOLLOWER)
		} else {
			// 2B 对方任期等于当前任期 或小于当前任期 那么判断日志接收的情况
			if reply.Success {
				rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PreLogIndex+len(args.Logs))
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				rf.updateLeaderCommitIndex()
			} else if reply.Term == rf.currentTerm {
				DPrintf("leader node {%d} term {%d} xIndex = %d peer = %d matchIndex = %d\n", rf.me, rf.currentTerm, reply.XIndex, peer, rf.matchIndex[peer])
				firstIndex := rf.getFirstLog().Index
				rf.nextIndex[peer] = max(reply.XIndex, rf.matchIndex[peer]+1)

				if reply.XTerm != -1 {
					boundary := max(firstIndex, reply.XIndex)
					for i := args.PreLogIndex; i >= boundary; i-- {
						if rf.logs[i-firstIndex].Term == reply.XTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
				DPrintf("leader node {%d} term {%d} update %d nextIndex = %d\n", rf.me, rf.currentTerm, peer, rf.nextIndex[peer])
			}
		}

	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FirstSnapindex := rf.getFirstLog().Index
	if FirstSnapindex >= index {
		return
	}
	var newLog []Entry
	rf.logs = append(newLog, rf.logs[(index-FirstSnapindex):]...)
	rf.persister.Save(nil, snapshot)
}

type InstallSnapshotArgs struct {
	Snapshot    []byte
	Finterm     int
	Finindex    int
	CurrentTerm int
	LeaderId    int
}

type InstallSnapshotReply struct {
	RetTerm int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	curTerm := rf.currentTerm
	reply.RetTerm = curTerm
	if args.CurrentTerm < curTerm {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.CurrentTerm
	rf.changeState(FOLLOWER)

	if rf.commitIndex >= args.Finindex {
		rf.mu.Unlock()
		return
	}

	if rf.getLastLog().Index <= args.Finindex {
		rf.logs = make([]Entry, 1)
	} else {
		var tmp []Entry
		rf.logs = append(tmp, rf.logs[args.Finindex-rf.getFirstLog().Index:]...)
	}

	rf.logs[0].Term = args.Finterm
	rf.logs[0].Index = args.Finindex
	rf.logs[0].Command = nil
	rf.persister.Save(nil, args.Snapshot)

	rf.lastApplied, rf.commitIndex = args.Finindex, args.Finindex
	rf.mu.Unlock()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.Finterm,
		SnapshotIndex: args.Finindex,
	}
}

func (rf *Raft) handleInstallSnapshotResponse(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state == LEADER && rf.currentTerm == args.CurrentTerm {
		if rf.currentTerm < reply.RetTerm {
			rf.currentTerm = reply.RetTerm
			rf.votedFor = -1
			rf.changeState(FOLLOWER)
			rf.persist()
		} else if rf.currentTerm == reply.RetTerm {
			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.Finindex)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}

	DPrintf("leader node {%d} term {%d} receive command %v\n", rf.me, rf.currentTerm, command)
	newLog := rf.appendLog(command) // 日志队列中加入一条新log
	index = newLog.Index
	term = rf.currentTerm         // 当前的任期
	isLeader = rf.state == LEADER // 是否是leader

	rf.sendEntries()
	return index, term, isLeader
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER

	// 初始化的时候开启选举计时器 关闭心跳包 只有当选了leader才开启心跳包
	rf.electionTimer = time.NewTicker(randomElectionTime())
	rf.heartbeatTimer = time.NewTicker(HEARTBEAT)
	rf.heartbeatTimer.Stop()
	// 2B
	rf.logs = make([]Entry, 1) // 设置第一个日志条目为空日志
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyMsgCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	rf.readPersist(persister.ReadRaftState())
	// initialize from state persisted before a crash
	// 2D
	firstLog := rf.getFirstLog()
	rf.lastApplied = firstLog.Index
	rf.commitIndex = firstLog.Index
	// 2B
	lastLog := rf.getLastLog()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()
	return rf
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// 广播时间 << 选举超时时间 << 平均故障时间
		// 测试器要求领导者发送心跳rpc每秒不超过10次 所以广播时间最小100ms/次

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf("node {%d} term {%d} start election\n", rf.me, rf.currentTerm)
			// FOLLOWER --> CANDIDATE
			rf.changeState(CANDIDATE)
			// 重置选举超时计时器
			rf.resetElectionTimeout()
			rf.requestVotes()
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			// 只有领导人可以发送心跳包
			// 锁放在上面 不然有数据竞争
			rf.mu.Lock()
			if rf.state == LEADER {
				DPrintf("node {%d} term {%d} send heartbeat\n", rf.me, rf.currentTerm)
				rf.sendEntries()
			}
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) updateLeaderCommitIndex() {

	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sortMatchIndex[rf.me] = rf.getLastLog().Index
	sort.Ints(sortMatchIndex)

	newCommitIndex := sortMatchIndex[n/2] // 因为本节点默认是满足条件的 所以不能够使用n/2 + 1
	if newCommitIndex > rf.commitIndex && newCommitIndex <= rf.getLastLog().Index && rf.logs[newCommitIndex-rf.getFirstLog().Index].Term == rf.currentTerm {
		DPrintf("leader node {%d} term {%d} update commitIndex %d to %d\n", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.applyMsgCond.Signal() // 容易忘记通知
	}
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) isLogMatch(lastLogIndex int, lastLogTerm int) bool {
	lastLog := rf.getLastLog()
	if lastLogTerm > lastLog.Term || (lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index) {
		return true
	}

	return false
}

func randomElectionTime() time.Duration {
	// 超时时间偏差 0 ~ 200 ms
	ms := rand.Int63() % 200
	// 超时计时器 200 ~ 400 ms
	eleTime := time.Duration(ELECTION_TIMEOUT+ms) * time.Millisecond
	return eleTime
}

func (rf *Raft) appendLog(command interface{}) Entry {
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   rf.getLastLog().Index + 1,
	}

	rf.logs = append(rf.logs, entry)
	rf.persist()
	//rf.persist()
	return entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	//rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.logs)
	// raftstate := w.Bytes()
	rf.persister.Save(rf.getState().Bytes(), nil)
}

func (rf *Raft) getState() *bytes.Buffer {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}
}

// 裁剪日志 并返回最后一个匹配的索引
func (rf *Raft) cropLogs(args *AppendEntriesArgs) int {
	// 得到第一条日志的索引
	firstIndex := rf.getFirstLog().Index

	// 遍历新加入的日志
	for i, entry := range args.Logs {
		// 一旦新加入日志条目的索引超出了日志队列的范围 那么原队列不需要裁剪 把接收到的日志中不存在的日志添加到本地日志队列
		// 如果某个新加入的日志对应的任期不等于本地日志对应的任期 那么本地日志从这个位置以及以后的日志全部丢弃
		if entry.Index >= firstIndex+len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			var tmp []Entry
			rf.logs = append(tmp, rf.logs[:entry.Index-firstIndex]...)
			return i
		}
	}

	return len(args.Logs)
}

func (rf *Raft) changeState(state State) {
	if state == FOLLOWER {
		DPrintf("node {%d} term {%d} change ==> follower\n", rf.me, rf.currentTerm)
		rf.resetElectionTimeout()
		rf.heartbeatTimer.Stop()
		rf.persist()
	} else if state == LEADER {
		DPrintf("node {%d} term {%d} change ==> leader\n", rf.me, rf.currentTerm)
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HEARTBEAT)
	}
	rf.persist()
	rf.state = state
}

func (rf *Raft) newAppendEntriesArgs(peer int) *AppendEntriesArgs {
	firstIndex := rf.getFirstLog().Index
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  rf.nextIndex[peer] - 1,
		PreLogTerm:   rf.logs[rf.nextIndex[peer]-1-firstIndex].Term,
		Logs:         cloneLogs(rf.logs[rf.nextIndex[peer]-firstIndex:]),
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) applyMsg() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyMsgCond.Wait()
		}
		applyEntries := make([]Entry, rf.commitIndex-rf.lastApplied)
		firstIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex
		copy(applyEntries, rf.logs[rf.lastApplied-firstIndex+1:rf.commitIndex-firstIndex+1])

		rf.isAppendLog = 1
		rf.mu.Unlock()

		for _, entry := range applyEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}

		atomic.StoreUint32(&rf.isAppendLog, 0)

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()

	}
}

func (rf *Raft) newInstallSnapshotArgs() *InstallSnapshotArgs {
	args := &InstallSnapshotArgs{
		CurrentTerm: rf.currentTerm,
		LeaderId:    rf.me,
		Finindex:    rf.getFirstLog().Index,
		Finterm:     rf.getFirstLog().Term,
		Snapshot:    rf.persister.snapshot,
	}
	return args
}

// 重置本节点的超时截止时间
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer.Reset(randomElectionTime())
}
