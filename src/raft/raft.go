// Inspired by https://zhuanlan.zhihu.com/p/411463748 and https://zhuanlan.zhihu.com/p/388478813
package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
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

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state      string
	heartBeat  time.Duration
	eleTimeout time.Duration
	eleTime    time.Time

	log       Log
	entryCh   chan *Entry
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	currentTerm int
	votedFor    int
	votedCnt    int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	includeIndex int
	includeTerm  int
	snapshot     []byte
}

type Entry struct {
	Command interface{}
	Index   int
	Term    int
}

type Log []Entry

func (log Log) firstlog() Entry {
	return log[0]
}

func (log Log) lastlog() Entry {
	return log[len(log)-1]
}

func (log Log) at(x int) Entry {
	x -= log[0].Index
	return log[x]
}

func (log Log) assign(x int, value Entry) {
	x -= log[0].Index
	log[x] = value
}

func (log Log) upper(x int) Log {
	x -= log[0].Index
	return log[x:]
}

func (log Log) truncate(x int) Log {
	x -= log[0].Index
	return log[:x]
}

type PersistData struct {
	CurrentTerm  int
	VotedFor     int
	Log          Log
	Snapshot     []byte
	IncludeIndex int
	IncludeTerm  int
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
	Server           int
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < reply.Term {
		return
	}
	rf.resetTerm(args.Term)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.state == "Leader")
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	per := PersistData{
		CurrentTerm:  rf.currentTerm,
		VotedFor:     rf.votedFor,
		Log:          rf.log,
		Snapshot:     []byte{},
		IncludeIndex: rf.includeIndex,
		IncludeTerm:  rf.includeTerm,
	}
	err := e.Encode(per)
	if err != nil {
		fmt.Println("Encode error")
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistdata PersistData
	err := d.Decode(&persistdata)
	if err != nil {
		fmt.Println("ReadPersist decode error")
		return
	}

	rf.votedFor = persistdata.VotedFor
	rf.currentTerm = persistdata.CurrentTerm
	rf.log = persistdata.Log
	rf.snapshot = persistdata.Snapshot
	rf.includeIndex = persistdata.IncludeIndex
	rf.includeTerm = persistdata.IncludeTerm
	rf.lastApplied = persistdata.IncludeIndex
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.includeTerm > lastIncludedTerm || rf.includeIndex > lastIncludedTerm {
		return false
	}
	rf.log = rf.log.upper(lastIncludedIndex + 1)
	rf.lastApplied = lastIncludedIndex
	rf.snapshot = snapshot
	rf.includeIndex = lastIncludedIndex
	rf.includeTerm = lastIncludedTerm
	rf.resetTimer()
	rf.persist()
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log = rf.log.upper(index)
	rf.snapshot = snapshot
	firstlog := rf.log.firstlog()
	rf.includeIndex = firstlog.Index
	rf.includeTerm = firstlog.Term
	rf.lastApplied = firstlog.Index
	rf.persist()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) resetTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.state = "Follower"
		rf.eleTimeout = 280*time.Millisecond + time.Duration(rand.Intn(130))*time.Millisecond
		rf.persist()
	}
}

func (rf *Raft) election() {
	rf.resetTerm(rf.currentTerm + 1)
	rf.state = "Candidate"
	rf.votedFor = rf.me
	rf.persist()
	rf.resetTimer()
	//fmt.Println(rf.state, rf.me, "begin election")

	lastlog := rf.log.lastlog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastlog.Index,
		LastLogTerm:  lastlog.Term,
	}
	fmt.Println(rf.state, rf.me, "begin election", args)
	rf.votedCnt = 1
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go rf.sendRequest(peer, &args)
		}
	}
}

func (rf *Raft) sendRequest(server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.state, rf.me, "received vote from", server, "got", reply.VoteGranted)
	if reply.Term > rf.currentTerm {
		rf.resetTerm(reply.Term)
		return
	}

	if reply.Term < rf.currentTerm || !reply.VoteGranted {
		return
	}

	rf.votedCnt++
	if rf.votedCnt > len(rf.peers)/2 &&
		rf.currentTerm == args.Term &&
		rf.state == "Candidate" {
		rf.state = "Leader"
		rf.votedCnt = 0
		lastlog := rf.log.lastlog()
		for peer, _ := range rf.peers {
			rf.nextIndex[peer] = lastlog.Index + 1
			rf.matchIndex[peer] = 0
		}
		fmt.Println(rf.me, "become leader", rf.nextIndex, rf.matchIndex)
		rf.leaderAppendEntries(true)
	}

}

func (rf *Raft) leaderAppendEntries(heartbeat bool) {

	lastlog := rf.log.lastlog()
	for peer, _ := range rf.peers {
		if peer == rf.me {
			rf.resetTimer()
			continue
		}
		if lastlog.Index > rf.nextIndex[peer] || heartbeat {
			nextIndex := rf.nextIndex[peer]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastlog.Index+1 < nextIndex {
				nextIndex = lastlog.Index
			}
			prevLog := rf.log.at(nextIndex - 1)

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      rf.log.upper(nextIndex),
				LeaderCommit: rf.commitIndex,
			}
			//fmt.Println("leader send append to", peer)
			go rf.leaderSend(peer, &args)
		}
	}
}

func (rf *Raft) leaderCommit() {
	for i := rf.commitIndex + 1; i <= rf.log.lastlog().Index; i++ {
		if rf.log.at(i).Term != rf.currentTerm {
			continue
		}
		cnt := 1
		for peer, _ := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= i {
				cnt++
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) leaderSend(peer int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, &reply)

	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("leader receive", peer, reply)
	if reply.Term > rf.currentTerm {
		rf.resetTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		if reply.Success {
			matchIndex := args.PrevLogIndex + len(args.Entries)
			nextIndex := matchIndex + 1
			fmt.Println(rf.me, "append success extend", len(args.Entries), "from", peer)
			rf.matchIndex[peer] = max(matchIndex, rf.matchIndex[peer])
			rf.nextIndex[peer] = max(nextIndex, rf.nextIndex[peer])
		} else if rf.nextIndex[peer] > 1 { // doesn't implement the optimization
			rf.nextIndex[peer] = rf.includeIndex
		}
		//fmt.Println("nextIndex", rf.nextIndex)
		rf.leaderCommit()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println(rf.state, rf.me, "received heartbeat from", args.LeaderId)
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term > rf.currentTerm {
		rf.resetTerm(args.Term)
		return
	}

	if args.Term < rf.currentTerm {
		return
	}
	//fmt.Println(rf.me, "here1")
	rf.resetTimer()

	if rf.state == "Candidate" {
		rf.state = "Follower"
	}

	lastlog := rf.log.lastlog()
	//fmt.Println(rf.me, "here", lastlog.Index, args.PrevLogIndex)
	if lastlog.Index < args.PrevLogIndex {
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm { // ?
		return
	}
	for _, entry := range args.Entries {
		if entry.Index <= rf.log.lastlog().Index && rf.log.at(entry.Index).Term != entry.Term {
			rf.log = rf.log.truncate(entry.Index)
			rf.persist()
		}
		if entry.Index > rf.log.lastlog().Index {
			//fmt.Println(rf.me, "append", entry)
			rf.log = append(rf.log, entry)
			rf.persist()
			//break
		}
	}

	//fmt.Println(rf.me, "update commit?", args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastlog().Index)
		rf.apply()
	}
	fmt.Println(rf.me, "receive append")
	reply.Success = true
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println(rf.state, rf.me, "received RequestVote from", args.CandidateId, rf.votedFor)
	if args.Term > rf.currentTerm {
		rf.resetTerm(args.Term)
	}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	lastlog := rf.log.lastlog()
	upToDate := args.LastLogTerm > lastlog.Term ||
		(args.LastLogTerm == lastlog.Term && args.LastLogIndex >= lastlog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.resetTimer()
		return
	}

	reply.VoteGranted = false
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	if rf.state != "Leader" {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.log.lastlog().Index + 1
	term = rf.currentTerm

	rf.log = append(rf.log, Entry{
		Command: command,
		Index:   index,
		Term:    term,
	})
	fmt.Println(rf.state, rf.me, "received command", command)
	rf.persist()
	rf.leaderAppendEntries(false)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		//fmt.Println(rf.heartBeat.String())
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()

		if rf.state == "Leader" {
			rf.leaderAppendEntries(true)
		}

		if time.Now().Sub(rf.eleTime) >= rf.eleTimeout {
			rf.election()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied && rf.log.lastlog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = "Follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeat = 100 * time.Millisecond
	rand.Seed(int64(rf.me))
	rf.eleTimeout = 480*time.Millisecond + time.Duration(rand.Intn(200))*time.Millisecond
	//fmt.Println(rf.me, rf.eleTimeout)
	rf.resetTimer()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.commitIndex = 0
	rf.lastApplied = 0
	//rf.currentTerm = 0
	rf.log = make(Log, 0)
	rf.log = append(rf.log, Entry{
		Command: -1,
		Term:    0,
		Index:   0,
	})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.includeIndex = 0
	rf.includeTerm = 0

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) resetTimer() {
	rf.eleTime = time.Now()
}
