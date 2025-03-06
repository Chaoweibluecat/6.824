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
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	votedFor  int                 // who i voted for
	term      int                 // current term nv
	log       []LogEntry          // test

	state         int
	lastHeartBeat int64

	nextIndex   []int
	matchIndex  []int
	lastApplied int

	// instance唯一,所以发消息前要double check是不是当前任期的消息
	swicthToFollowerChan chan struct{}
	followerAppendCond   []sync.Cond
	commitUpdateCond     *sync.Cond
	commitIndex          int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

const NO_VOTE_YET int = -1

const LEADER int = 2
const FOLLOWER int = 0
const CANDIDATE int = 1

func assert(condition bool, message string) {
	if !condition {
		panic("Assertion failed: " + message)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// 此处需要mutex的原因是1.读两个变量不是原子的 2.有些函数异步执行，直到改完state释放锁前，状态都不应该暴露
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return (int)(rf.term), rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	CandidataId  int
	Term         int
	LastLogIndex int // see raft 5.4
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	} else {
		return rf.log[len(rf.log)-1].Index
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("%d :receive vote, request %v", rf.me, args)
	rf.mu.Lock()
	// 过期的candidate,拒绝
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		log.Printf("%d :voting rejected because legacy term, request %v, reponse %v", rf.me, args, reply)
		rf.mu.Unlock()
		return
	}
	switchToFollower := args.Term > rf.term && rf.state != FOLLOWER
	if args.Term > rf.term {
		rf.votedFor = NO_VOTE_YET
		rf.state = FOLLOWER
		rf.term = args.Term
	}
	// 过期
	if rf.votedFor == NO_VOTE_YET || rf.votedFor == args.CandidataId {
		// 额外的candidate check, 确保candidate有所有的committedLog
		// (voter否决lastLog没有自己新的candidate)
		// 如果candidate没有所有commitedLog,它就不会有majority选票
		lastLogTerm := rf.lastLogTerm()
		lastLogIndex := rf.lastLogIndex()

		if lastLogTerm < args.Term || (lastLogTerm == args.Term && args.LastLogIndex >= lastLogIndex) {
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidataId
			log.Printf("%d :vote yes, request %v, reponse %v", rf.me, args, reply)
			rf.mu.Unlock()
			if switchToFollower {
				rf.swicthToFollowerChan <- struct{}{}
			}
			return
		}
	}

	log.Printf("%d :voting rejected because I alreadt vote %d, my term %d", rf.me, rf.votedFor, rf.term)
	reply.Term = args.Term
	reply.VoteGranted = false
	rf.mu.Unlock()
	if switchToFollower {
		rf.swicthToFollowerChan <- struct{}{}
	}
	// Your code here (2A, 2B).
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat() {
	for idx := range rf.peers {
		if idx != rf.me {
			go rf.sendAppendEntriesOnce(idx, true)
			// go func(idx int) {
			// 	heartBeat := AppendEntriesRequest{}
			// 	heartBeat.Term = currentTerm
			// 	reponse := AppendEntriesResponse{}
			// 	ok := rf.sendAppendRPC(idx, &heartBeat, &reponse)
			// 	if ok && !reponse.Success {
			// 		rf.mu.Lock()
			// 		if reponse.Term > rf.term {
			// 			if rf.term == currentTerm && rf.state == LEADER {
			// 				rf.term = reponse.Term
			// 				rf.swicthToFollowerChan <- struct{}{}
			// 			}
			// 		}
			// 		rf.mu.Unlock()
			// 	}
			// }(idx)
		}
	}
}

func (rf *Raft) sendAppendRPC(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	log.Printf("%d: receive append entry!, term %d", rf.me, args.Term)
	rf.mu.Lock()
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.term {
		rf.votedFor = NO_VOTE_YET
		rf.term = args.Term
	}
	rf.lastHeartBeat = time.Now().UnixMilli()
	if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		log.Printf("%d: rejected AppendRpc because of log inconsistency!, term %d", rf.me, args.Term)
		reply.Success = false
	} else {
		reply.Success = true
		if len(args.Entries) != 0 {
			for idx := range args.Entries {
				leaderIdx := args.Entries[idx].Index
				if leaderIdx <= len(rf.log) && args.Entries[idx].Term != rf.log[leaderIdx-1].Term {
					rf.log[leaderIdx-1] = args.Entries[idx]
				} else if leaderIdx == len(rf.log)+1 {
					rf.log = append(rf.log, args.Entries[idx])
				}
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}

	if rf.state != FOLLOWER {
		log.Printf("%d: another leader, shift into follower", rf.me)
		rf.state = FOLLOWER
		rf.mu.Unlock()
		rf.swicthToFollowerChan <- struct{}{}
	} else {
		rf.mu.Unlock()
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
	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		isLeader = false
		return index, term, isLeader
	}
	// start from 1
	newLog := LogEntry{rf.term, len(rf.log) + 1, command}
	rf.log = append(rf.log, newLog)
	assert(len(rf.log) == rf.lastLogIndex(), "failed ")
	log.Printf("%d: start command : %v", rf.me, newLog)

	go func() {
		for idx := range rf.followerAppendCond {
			if idx != rf.me {
				rf.followerAppendCond[idx].Signal()
			}
		}
	}()
	return index, term, isLeader

	// Your code here (2B).

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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendAppendEntriesOnce(idx int, isHeartBeat bool) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	request := AppendEntriesRequest{}
	request.Term = rf.term
	request.LeaderId = rf.me
	real_next_idx := rf.nextIndex[idx] - 1
	real_prev_log_idx := real_next_idx - 1
	if real_prev_log_idx < len(rf.log) {
		request.PrevLogIndex = 0
		request.PrevLogTerm = 0
	} else {
		request.PrevLogIndex = rf.log[real_prev_log_idx].Index
		request.PrevLogTerm = rf.log[real_prev_log_idx].Term
	}
	if isHeartBeat {
		request.Entries = []LogEntry{}
	} else {
		request.Entries = rf.log[real_next_idx:len(rf.log)]
	}
	request.LeaderCommit = rf.commitIndex
	reponse := AppendEntriesResponse{}
	rf.mu.Unlock()
	ok := rf.sendAppendRPC(idx, &request, &reponse)
	if ok && !reponse.Success {
		rf.mu.Lock()
		// 确保rpc返回时，仍然在有效任期
		if rf.term == request.Term && rf.state == LEADER {
			// 因为任期过期失败
			if reponse.Term > rf.term {
				rf.term = reponse.Term
				rf.swicthToFollowerChan <- struct{}{}
			} else {
				// 因为log一致性失败,decrement next
				rf.nextIndex[idx] -= 1
				//retry
			}
		}
		rf.mu.Unlock()
	} else if ok && !isHeartBeat {
		rf.mu.Lock()
		if rf.term == request.Term && rf.state == LEADER {
			//update matchIdx,用成功request的最后一条日志的index,heartbeat不更新
			rf.matchIndex[idx] = request.Entries[len(request.Entries)-1].Index
			rf.nextIndex[idx] = rf.matchIndex[idx] + 1
			rf.updateCommitIdx()
		}
		rf.mu.Unlock()
	} else {
		// retry
	}

	// rf.mu.Lock()
	// lastlogIdx := len(rf.log)
	// for idx := range rf.peers {
	// 	if idx == rf.me {
	// 		continue
	// 	}
	// 	if lastlogIdx >= rf.nextIndex[idx] {
	// 		go func(idx int) {
	// 			prevLogIndex := 0
	// 			prevLogTerm := 0
	// 			if len(rf.log) != 0 {
	// 				prevLogIndex = len(rf.log)
	// 				prevLogTerm = (int)(rf.log[len(rf.log)-1].Term)
	// 			}
	// 			args := AppendEntriesRequest{
	// 				rf.term,
	// 				rf.me,
	// 				prevLogIndex,
	// 				prevLogTerm,
	// 				rf.log[rf.nextIndex[idx]-1 : len(rf.log)-1],
	// 				rf.commitIndex}
	// 			response := AppendEntriesResponse{}
	// 			ok := rf.sendAppendRPC(idx, &args, &response)

	// 		}(idx)
	// 	}
	// }
}

// require rf.mu
func (rf *Raft) updateCommitIdx() {
	majority := rf.majority()
	prevCommit := rf.commitIndex
	// term单调递增，找到一个小于当前term的日志可以直接跳出循环了（不能提交非当前term)
	for i := len(rf.log) - 1; i >= rf.commitIndex && rf.log[i].Term == rf.term; i-- {
		count := 1
		for j, match := range rf.matchIndex {
			if j == rf.me {
				continue
			} else {
				if match >= rf.log[i].Index {
					count += 1
				}
				if count >= majority {
					rf.commitIndex = rf.log[i].Index
					break
				}
			}
		}
	}
	if prevCommit != rf.commitIndex {
		rf.commitUpdateCond.Signal()
	}
}

func (rf *Raft) majority() int {
	return (len(rf.peers) + 1) / 2
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
	rf.term = 0
	rf.lastHeartBeat = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.swicthToFollowerChan = make(chan struct{})
	rf.followerAppendCond = make([]sync.Cond, len(rf.peers))
	for idx := range rf.peers {
		if idx != me {
			rf.followerAppendCond[idx] = *sync.NewCond(&sync.Mutex{})
		}
	}
	rf.commitUpdateCond = sync.NewCond(&rf.mu)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	//异步apply
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			for rf.lastApplied == rf.commitIndex {
				rf.commitUpdateCond.Wait()
			}
			logs := make([]LogEntry, rf.commitIndex-rf.lastApplied)
			copy(logs, rf.log[rf.lastApplied:rf.commitIndex])
			rf.mu.Unlock()
			for _, log := range logs {
				msg := ApplyMsg{}
				msg.Command = log.Command
				msg.CommandIndex = log.Index
				msg.CommandValid = true
				applyCh <- msg
			}
		}
	}()

	go func() {
		rf.mu.Lock()
		for !rf.killed() {
			if rf.state == FOLLOWER {
				rf.checkHeartBeat()
			} else if rf.state == CANDIDATE {
				rf.electAsCandidate()
			} else if rf.state == LEADER {
				rf.sendHeartBeat()
				rf.mu.Unlock()
				select {
				case <-time.After(time.Duration(150) * (time.Millisecond)):
					rf.mu.Lock()
					// timeout, no state change
					continue
				case <-rf.swicthToFollowerChan:
					rf.mu.Lock()
					log.Printf("%d: channel message acceptted", rf.me)
					continue
				}
			}
		}
	}()

	for idx := range rf.followerAppendCond {
		if idx != rf.me {
			go rf.sendFollowerRountine(idx)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf

}

func (rf *Raft) sendFollowerRountine(idx int) {
	rf.followerAppendCond[idx].L.Lock()
	defer rf.followerAppendCond[idx].L.Unlock()
	for !rf.killed() {
		for !rf.needSendLog(idx) {
			rf.followerAppendCond[idx].Wait()
		}
		for rf.needSendLog(idx) {
			rf.sendAppendEntriesOnce(idx, false)
		}
	}
}

func (rf *Raft) needSendLog(idx int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == LEADER && rf.lastLogIndex() >= rf.nextIndex[idx]
}

func (rf *Raft) checkHeartBeat() {
	lastHeartBeat := rf.lastHeartBeat
	rf.mu.Unlock()
	time.Sleep(time.Duration(randTimeout()) * time.Millisecond)
	rf.mu.Lock()
	if rf.lastHeartBeat == lastHeartBeat {
		log.Printf("%d: elect timeout!, now I will run election as term %d", rf.me, rf.term+1)
		rf.state = CANDIDATE
	}
}

func randTimeout() int {
	max := big.NewInt(150)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return (int)(x + 150)
}

// require mutex?
func (rf *Raft) electAsCandidate() {
	rf.votedFor = rf.me
	rf.term += 1
	yes := 1
	no := 0
	electionEndChan := make(chan struct{})
	electionEnd := false
	timeout := randTimeout()
	cur_term := rf.term
	var mu sync.Mutex
	for idx := range rf.peers {
		if idx != rf.me {
			go func(idx int) {
				request := RequestVoteArgs{}
				response := RequestVoteReply{}
				request.CandidataId = rf.me
				request.Term = cur_term
				request.LastLogIndex = rf.lastLogIndex()
				request.LastLogTerm = rf.lastLogTerm()
				res := rf.sendRequestVote(idx, &request, &response)
				mu.Lock()
				defer mu.Unlock()
				if electionEnd {
					return
				}
				if res && response.VoteGranted {
					yes += 1
				} else {
					no += 1
				}
				if yes >= (len(rf.peers)+1)/2 || no >= (len(rf.peers)+1)/2 {
					electionEnd = true
					rf.mu.Lock()
					// 同步更新状态, 并让发送消息让主循环继续
					if rf.term == cur_term && yes >= (len(rf.peers)+1)/2 {
						log.Printf("%d I win election for term %d", rf.me, rf.term)
						rf.state = LEADER
						// leader的必要初始化
						// nextIndex, matchidx是volatile的,每次当选后从0开始,在appendRPC中更新
						// 需要在这个线程持有锁时同步进行，确保原子性
						rf.matchIndex = make([]int, len(rf.peers))
						rf.nextIndex = make([]int, len(rf.peers))
						for idx := range rf.peers {
							rf.matchIndex[idx] = 0
							// leader last Idx = len(rf.log), next = .. + 1
							rf.nextIndex[idx] = len(rf.log) + 1
						}
					}
					rf.mu.Unlock()
					electionEndChan <- struct{}{}
				}
			}(idx)
		}
	}
	rf.mu.Unlock()
	select {
	case <-electionEndChan:
		rf.mu.Lock()
		close(electionEndChan)
		return
	case <-time.After(time.Duration(timeout) * (time.Millisecond)):
		log.Printf("%d %dms elaspsed, TimeoutOut And No winner", rf.me, timeout)
		rf.mu.Lock()
		// timeout, no state change
		return
	case <-rf.swicthToFollowerChan:
		rf.mu.Lock()
		return
	}

}
