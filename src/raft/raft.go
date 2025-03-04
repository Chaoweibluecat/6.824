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
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	votedFor      int                 // who i voted for
	term          int32               // current term nv
	log           []LogEntry          // test
	lastLogIndex  int32
	state         int
	lastHeartBeat int64
	nextIndex     []int
	// instance唯一,所以发消息前要double check是不是当前任期的消息
	swicthToFollowerChan chan struct{}
	commitIndex          int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

type LogEntry struct {
	Term int32
}

const NO_VOTE_YET int = -1

const LEADER int = 2
const FOLLOWER int = 0
const CANDIDATE int = 1

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
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
	Term         int32
	LastLogIndex int32 // see raft 5.4
	LastLogTerm  int32
}

type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         int32
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

type AppendEntriesResponse struct {
	Term    int32
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	log.Printf("%d :receive vote, request %v", rf.me, args)
	rf.mu.Lock()
	myTerm := rf.term
	myvote := rf.votedFor
	// 过期的candidate,拒绝
	if args.Term < myTerm {
		reply.Term = myTerm
		reply.VoteGranted = false
		log.Printf("%d :voting rejected because legacy term, request %v, reponse %v", rf.me, args, reply)
		rf.mu.Unlock()
		return
	}
	switchToFollower := args.Term > myTerm && rf.state != FOLLOWER
	if args.Term > myTerm {
		rf.votedFor = NO_VOTE_YET
		rf.state = FOLLOWER
	}
	// 过期
	if myvote == NO_VOTE_YET || myvote == args.CandidataId {
		// 额外的candidate check, 确保candidate有所有的committedLog
		// (voter否决lastLog没有自己新的candidate)
		// 如果candidate没有所有commitedLog,它就不会有majority选票
		var last_log_term int32
		if len(rf.log) == 0 {
			last_log_term = -1
		} else {
			last_log_term = rf.log[len(rf.log)-1].Term
		}

		if last_log_term < args.Term || (last_log_term == args.Term && args.LastLogIndex >= rf.lastLogIndex) {
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
	currentTerm := rf.term
	for idx := range rf.peers {
		if idx != rf.me {
			go func(idx int) {
				heartBeat := AppendEntriesRequest{}
				heartBeat.Term = currentTerm
				reponse := AppendEntriesResponse{}
				ok := rf.sendAppendRPC(idx, &heartBeat, &reponse)
				if ok && !reponse.Success {
					rf.mu.Lock()
					if reponse.Term > rf.term {
						if rf.term == currentTerm && rf.state == LEADER {
							rf.term = reponse.Term
							rf.mu.Unlock()
							rf.swicthToFollowerChan <- struct{}{}
						}
					}
					rf.mu.Unlock()
				}
			}(idx)
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
		rf.mu.Unlock()
		return
	}
	//catch up
	rf.term = args.Term
	current := time.Now().UnixMilli()
	atomic.StoreInt64(&rf.lastHeartBeat, current)
	rf.votedFor = NO_VOTE_YET
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
	rf.mu.Lock()
	if rf.state != LEADER {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	newLog := LogEntry{rf.term}
	prevLogIndex := 0
	prevLogTerm := 0
	if len(rf.log) != 0 {
		prevLogIndex = len(rf.log)
		prevLogTerm = (int)(rf.log[len(rf.log)-1].Term)
	}
	rf.log = append(rf.log, newLog)
	index = len(rf.log)
	currentTerm := rf.term
	term = int(rf.term)
	rf.mu.Unlock()
	return index, term, true
	append := AppendEntriesRequest{currentTerm, rf.me, prevLogIndex, prevLogTerm, []LogEntry{newLog}, rf.commitIndex}
	for idx := range rf.peers {
		if idx != rf.me {

		}
	}
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
	rf.lastLogIndex = 0
	rf.term = 0
	rf.lastHeartBeat = 0
	rf.commitIndex = 0
	rf.swicthToFollowerChan = make(chan struct{})
	go func() {
		rf.mu.Lock()
		for {
			rf.votedFor = NO_VOTE_YET
			if rf.state == FOLLOWER {
				rf.checkHeartBeat()
			} else if rf.state == CANDIDATE {
				rf.electAsCandidate()
			} else if rf.state == LEADER {
				rf.sendHeartBeat()
				rf.sendAppendEntries()
				rf.mu.Unlock()
				select {
				case <-time.After(time.Duration(200) * (time.Millisecond)):
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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf

}
func (rf *Raft) sendAppendEntries() {
	go func ()  {
		
	}
}

func (rf *Raft) checkHeartBeat() {
	lastHeartBeat := atomic.LoadInt64(&rf.lastHeartBeat)
	rf.mu.Unlock()
	time.Sleep(time.Duration(randTimeout()) * time.Millisecond)
	rf.mu.Lock()
	if rf.lastHeartBeat == lastHeartBeat {
		log.Printf("%d: elect timeout!, now I will run election as term %d", rf.me, rf.term+1)
		rf.state = CANDIDATE
	}
}

func randTimeout() int {
	max := big.NewInt(200)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return (int)(x + 200)
}

// require mutex?
func (rf *Raft) electAsCandidate() {
	rf.votedFor = rf.me
	atomic.AddInt32(&rf.term, 1)
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
				if len(rf.log) == 0 {
					request.LastLogIndex = 0
					request.LastLogTerm = 0
				}
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
					electionEndChan <- struct{}{}
				}
			}(idx)
		}
	}
	rf.mu.Unlock()
	select {
	case <-electionEndChan:
		close(electionEndChan)
		rf.mu.Lock()
		if yes >= (len(rf.peers)+1)/2 {
			log.Printf("%d I win election for term %d", rf.me, rf.term)
			rf.state = LEADER
		}
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
