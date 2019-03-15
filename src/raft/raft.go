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
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	commitIndex int
	lastApplied int
	role        raftRole

	logs       []LogEntry
	nextIndex  []int
	matchIndex []int

	applyCh        chan ApplyMsg
	electionCh     chan electionSignal
	heartBeatCh    chan heartBeatSignal
	shutdownCh 	   chan int
	electionTimer  *time.Timer
	heartBeatTimer *time.Timer

	ignoreElection bool
}

type raftRole string

const (
	leader    raftRole = "leader"
	follower           = "follower"
	candidate          = "candidate"
)

type electionSignal int

const (
	startElection    electionSignal = -1
	electionCanceled                = -2
)

type heartBeatSignal int

const (
	elected heartBeatSignal = -1
	normal                  = -2
)

type vote int

const (
	voteGranted  = 1
	voteRejected = 0
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	DPrintf("I(%d) am a %s of term=%d", rf.me, rf.role, rf.currentTerm)
	return rf.currentTerm, rf.role == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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


//
// restore previously persisted state.
//
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 		 int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		DPrintf("peer=%d: candidate's term=%d is older than currentTerm=%d, reject\n", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	previousRole := rf.role
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = follower
	}

	if previousRole == candidate {
		rf.electionCh <- electionCanceled
		DPrintf("peer=%d: send signal to terminate current node's election\n", rf.me);
		rf.resetElectionTimer()
	}

	reply.Term = max(args.Term, rf.currentTerm)
	if  rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.isCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
			DPrintf("peer=%d: grante vote to peer=%d in term=%d\n", rf.me, args.CandidateId, rf.currentTerm)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.role = follower
			rf.resetElectionTimer()
		} else {
			DPrintf("peer=%d: peer=%d's log is incomplete, reject.", rf.me, args.CandidateId)
			reply.VoteGranted = false
		}
	} else {
		DPrintf("peer=%d: vote is already granted to peer=%d in term=%d\n", rf.me, rf.votedFor, rf.currentTerm)
		reply.VoteGranted = false
	}
}

// if the logs have last entries with different terms, then the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is more up-to-date.
func (rf *Raft) isCandidateLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	return  lastLogTerm > rf.logs[len(rf.logs) - 1].Term || lastLogTerm == rf.logs[len(rf.logs) - 1].Term && lastLogIndex >= len(rf.logs) - 1;
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId 		  int
	PrevLogIndex 	  int
	PrevLogTerm 	  int
	Entries 		  []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term                        int
	Success                     bool
	ConflictingLogEntry1stIndex int
	ConflictingLogEntryTerm     int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("peer=%d: leader has term=%d which is older than current term=%d, reject\n", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictingLogEntry1stIndex = -1
		reply.ConflictingLogEntryTerm = -1
		return
	}

	// if caller has higher term, update node status
	previousRole := rf.role
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = follower
	}

	if previousRole == candidate  {
		rf.electionCh <- electionCanceled
	}
	rf.resetElectionTimer()

	// Replay false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("peer=%d rejects applying entries, args=%+v", rf.me, *args)
		reply.Term = rf.currentTerm
		reply.Success = false
		if args.PrevLogIndex >= len(rf.logs) {
			reply.ConflictingLogEntryTerm = -1
			reply.ConflictingLogEntry1stIndex = len(rf.logs)
		} else {
			_, start, _ := rf.hasTerm(rf.logs[len(rf.logs) -1].Term)
			reply.ConflictingLogEntryTerm = rf.logs[len(rf.logs) -1].Term
			reply.ConflictingLogEntry1stIndex = start
		}

		return
	}

	DPrintf("peer=%d: appending entries, args=%+v", rf.me, *args)

	reply.Term = rf.currentTerm
	reply.Success = true

	idx := args.PrevLogIndex + 1
	for _, logEntry := range args.Entries {
		// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and
		// all that follow it
		if idx < len(rf.logs) && rf.logs[idx].Term != logEntry.Term {
			rf.logs = rf.logs[:idx]
		}
		if idx < len(rf.logs) {
			rf.logs[idx] = logEntry
		} else {
			rf.logs = append(rf.logs, logEntry)
		}
		idx ++
	}

	prevCommitIndex := rf.commitIndex
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
		for i := prevCommitIndex + 1; i < rf.commitIndex + 1; i ++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command: rf.logs[i].Command,
				CommandIndex: i}
		}
		DPrintf("peer=%d updates commit index to %d", rf.me, rf.commitIndex);
	}
	DPrintf("peer=%d's log entries=%+v", rf.me, rf.logs)
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader {
		return 0, rf.currentTerm, false
	}

	index := len(rf.logs)
	DPrintf("peer=%d starts agreement for term=%d, index=%d", rf.me, rf.currentTerm, index)
	// if command received from client : append entry to local log
	rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	return index, rf.currentTerm, rf.role == leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("peer=%d shut down", rf.me)
	// this is a bad hack. use different shutdown channel for different monitors
	rf.shutdownCh <- 1
	rf.shutdownCh <- 1
	rf.shutdownCh <- 1
	rf.shutdownCh <- 1
}

func (rf *Raft) isMajority(a int) bool {
	return a >= len(rf.peers) / 2 + 1
}

func (rf *Raft) findMajorityN() int {
	l := len(rf.matchIndex)
	c := make([]int, l)
	copy(c, rf.matchIndex)
	sort.Ints(c)
	return c[l/2];  // 5 / 2 = 2 -> 3rd element, 3 / 2 = 1 -> 2nd element
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		rf.ignoreElection = true
	}
	rf.electionTimer.Reset(getNextElectionTimeOut())
}

func (rf *Raft) hasTerm(term int) (bool, int, int) {
	i, start, found := 0, 0, false;
	for ; i < len(rf.logs); i++ {
		if rf.logs[i].Term == term {
			found = true
			break
		}
	}
	if found {
		start = i
		for i = start + 1; i < len(rf.logs); i++ {
			if rf.logs[i].Term != term {
				break
			}
		}
		return found, start, i - 1
	}
	return false, 0, 0
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1, 10)
	// since log index starts at 0, put a dummy entry at index 0 with term 0(terms start at 1)
	rf.logs[0] = LogEntry{0, -1}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = follower

	rf.applyCh = applyCh
	rf.electionCh = make(chan electionSignal)
	rf.heartBeatCh = make(chan heartBeatSignal)
	rf.shutdownCh = make(chan int)
	rf.electionTimer = time.NewTimer(getNextElectionTimeOut())
	rf.heartBeatTimer = time.NewTimer(getHeartBeatInterval())

	// heart beat monitor
	go func(rf *Raft) {
		for {
			select {
			case <-rf.heartBeatTimer.C:
				rf.heartBeatTimer.Reset(getHeartBeatInterval())
				rf.heartBeatCh <- normal
			case <-rf.shutdownCh:
				DPrintf("peer=%d terminates heat beat monitor", rf.me)
				return
			}
		}
	}(rf);

	// heart beat processor
	go func(rf *Raft) {
		for {
			select {
			case <-rf.heartBeatCh:
				rf.mu.Lock()
				if rf.role != leader {
					rf.mu.Unlock()
					continue
				}
				// If there exists an N such that N > commitIndex, a majority
				// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
				DPrintf("peer=%d, nextIndex=%+v", rf.me, rf.nextIndex)
				DPrintf("peer=%d, matchIndex=%+v", rf.me, rf.matchIndex)
				n := rf.findMajorityN();
				if rf.logs[n].Term == rf.currentTerm {
					DPrintf("peer=%d, tries to commit index=%d, currentCommitIndex=%d", rf.me, n, rf.commitIndex)
					prevCommitIndex := rf.commitIndex;
					rf.commitIndex = max(rf.commitIndex, n)
					for i := prevCommitIndex + 1; i < rf.commitIndex+1; i ++ {
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      rf.logs[i].Command,
							CommandIndex: i,
						}
					}
				}
				rf.mu.Unlock()

				currentTerm := rf.currentTerm
				for peerId := 0; peerId < len(rf.peers); peerId ++ {
					if peerId == rf.me {
						continue
					}

					go func(peerId, currentTerm int) {
						rf.mu.Lock()
						args := AppendEntriesArgs{
							Term:              currentTerm,
							LeaderId:          rf.me,
							PrevLogIndex:      max(rf.nextIndex[peerId]-1, 0),
							PrevLogTerm:       rf.logs[max(rf.nextIndex[peerId]-1, 0)].Term,
							Entries:           rf.logs[rf.nextIndex[peerId]:],
							LeaderCommitIndex: rf.commitIndex,
						}
						proposedMatchIndex := len(rf.logs) - 1;
						rf.mu.Unlock()
						reply := AppendEntriesReply{}
						DPrintf("peer=%d sends heat beat to peer=%d, args=%+v", rf.me, peerId, args)
						success := rf.sendAppendEntries(peerId, &args, &reply)
						if success {
							rf.mu.Lock()
							if reply.Term > currentTerm {
								rf.currentTerm = max(rf.currentTerm, reply.Term)
								rf.role = follower
							} else if reply.Success {
								rf.nextIndex[peerId] = max(proposedMatchIndex+1, rf.nextIndex[peerId])
								rf.matchIndex[peerId] = max(proposedMatchIndex, rf.matchIndex[peerId]);
							} else {
								// if AppendEntries fails because of log inconsistency: decrement nextIndex and retry in next heat beat
								// improved: if leader has log entries with the follower's conflicting term:
								if hasConflictingTerm, _, end := rf.hasTerm(reply.ConflictingLogEntryTerm); hasConflictingTerm {
									// move nextIndex[i] back to leader's last entry for the conflicting term
									rf.nextIndex[peerId] = end
								} else {
									// move nextIndex[i] back to follower's first index for the conflicting term
									rf.nextIndex[peerId] = max(reply.ConflictingLogEntry1stIndex, 1)
								}
								//rf.nextIndex[peerId] --
							}
							rf.mu.Unlock()
						}
					}(peerId, currentTerm);
				}
			case <-rf.shutdownCh:
				DPrintf("peer=%d terminates heart beat handler", rf.me)
				return
			}
		}
	}(rf);

	// election time out monitor
	go func(rf *Raft) {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.electionCh <- startElection
			case <-rf.shutdownCh:
				DPrintf("peer=%d terminates election time out monitor", rf.me)
				return
			}
		}
	}(rf);

	// election time out processor
	go func(rf *Raft) {
		for {
			select {
			case <-rf.electionCh:
				DPrintf("peer=%d, election times out, ignoreElection=%v", rf.me, rf.ignoreElection)
				if rf.ignoreElection {
					rf.mu.Lock()
					rf.ignoreElection = false
					rf.electionTimer.Stop()
					rf.electionTimer.Reset(getNextElectionTimeOut())
					rf.mu.Unlock()
					continue
				}
			startOver:
				rf.mu.Lock()
				if rf.role == leader {
					rf.mu.Unlock()
					continue
				}

				rf.role = candidate
				rf.currentTerm += 1
				currentTerm := rf.currentTerm
				rf.mu.Unlock()

				// send vote requests
				DPrintf("peer=%d starts election for term=%d", rf.me, currentTerm)
				voteCh := make(chan vote, 5)
				voteCh <- voteGranted // vote for self
				rf.mu.Lock()
				rf.votedFor = rf.me
				rf.resetElectionTimer()
				rf.mu.Unlock()
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					if peerId == rf.me {
						continue
					}
					//go requestVote(rf.me, peerId, client, stateSnapshot, voteC)
					go func(peerId int) {
						DPrintf("peer=%d is requesting vote from peer=%d for term=%d\n", rf.me, peerId, currentTerm)
						args := RequestVoteArgs{
							Term:        currentTerm,
							CandidateId: rf.me,
							LastLogIndex: len(rf.logs) - 1,
							LastLogTerm: rf.logs[len(rf.logs) - 1].Term,
						}
						reply := RequestVoteReply{}
						success := rf.sendRequestVote(peerId, &args, &reply)
						if !success {
							//fmt.Printf("Fail to request vote from peer=%d\n", peerId)
							voteCh <- voteRejected
							return
						}

						if reply.VoteGranted {
							DPrintf("peer=%d received vote from peer=%d for term=%d\n", rf.me, peerId, currentTerm)
							voteCh <- voteGranted
						} else {
							DPrintf("peer=%d got rejected from peer=%d on term=%d\n", rf.me, peerId, currentTerm)
							voteCh <- voteRejected
						}
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.role = follower
						}
					}(peerId);
				}

				// wait for vote response
				DPrintf("peer=%d is waiting for response for term=%d from other peers\n", rf.me, currentTerm)
				votes := 0
				received := 0
				terminate := false
				for !terminate {
					select {
					case signal := <-rf.electionCh:
						if signal == startElection {
							goto startOver
						} else if signal == electionCanceled {
							DPrintf("peer=%d election terminates\n", rf.me)
							terminate = true
						}
					case vote := <-voteCh:
						received += 1
						if vote == voteGranted {
							votes += 1
							DPrintf("peer=%d has received %d votes in term=%d", rf.me, votes, currentTerm)
						}
						if votes >= len(rf.peers)/2+1 {
							DPrintf("peer=%d has been elected as the leader", rf.me)
							rf.mu.Lock()
							rf.role = leader
							// reinitialize nextIndex[] and natchIndex[]
							for i := 0; i < len(peers); i++ {
								rf.nextIndex[i] = len(rf.logs)
								rf.matchIndex[i] = 0;
							}
							rf.matchIndex[rf.me] = len(rf.logs) - 1
							rf.heartBeatCh <- elected // send initial heart beat
							rf.mu.Unlock()
							terminate = true
						} else if received == len(rf.peers) {
							//DPrintf("peer=%d fails to be elected as the leader", rf.me)
							terminate = true
						}
					}
				}
			case <-rf.shutdownCh:
				DPrintf("peer=%d terminates election handler", rf.me)
				return;
			}
		}
	}(rf);

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func getNextElectionTimeOut() time.Duration {
	return time.Duration((rand.Intn(151) + 300)) * time.Millisecond;
}

func getHeartBeatInterval() time.Duration {
	return 120 * time.Millisecond;
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}