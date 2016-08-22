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

import "sync"
import "labrpc"
import "time"
import "fmt"
import "math/rand"
import "bytes"
import "encoding/gob"



const (
	_ = iota
	FOLLOWER
	CANDIDATE
	LEADER

	TM_HB = 100	// heartbeat timeout
	TM_EC = 300	// election timeout 150~300
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	//LogIndex int
	LogTerm int
	LogCmd interface{}
}

//
// A Go object implementing a single *Raft peer*.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/*
	Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	currentTerm		latest term server has seen (initialized to 0 on first boot, increases monotonically) 
	votedFor	candidateId that received vote in current term (or null if none)
	log[]	log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	*/

	currentTerm		int
	votedFor		int
	recvedVoteNum	int
	leaderId		int
	role			int
	log		[]LogEntry //first Index is 1
	
	//rv,ae rpc reply channel
	rv_ch		chan RequestVoteReply
	ae_ch		chan AppendEntriesReply
	stop_ch		chan bool
	//election,heartbeat timer
	ec_timer	*time.Timer
	hb_timer	*time.Timer

	apply_ch	chan ApplyMsg

	/*
	Volatile state on all servers:
	commitIndex		index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied		index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	*/
	commitIndex		int
	lastApplied		int
	
	/*
	Volatile state on *leaders*:(Reinitialized after election)
	nextIndex[]		for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex[]	for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	*/
	nextIndex	[]int
	matchIndex	[]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = (rf.me == rf.leaderId)
	
	fmt.Printf("GetState S%v term[%v] isleader[%v]\n", rf.me, term, isleader)
	return term, isleader
}

//log with one dummy Term0 
//both term & index starts begin 1
func (rf *Raft) getLastIndex() int {
	return len(rf.log)-1
}
func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//*IMPORTANT*:
//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)

//
// RequestVote RPC
//
type RequestVoteArgs struct {
	Term			int // candidate’s term
	CandidateId		int //candidate requesting vote
	LastLogIndex	int //index of candidate’s last log entry (§5.4) 
	LastLogTerm		int //term of candidate’s last log entry (§5.4)
}
type RequestVoteReply struct {
	Term		int //currentTerm, for candidate to update itself 
	VoteGranted bool //true means candidate received vote
}
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//1. Reply false if term < currentTerm (§5.1)
		reply.VoteGranted = false
		reply.Term = 0
		return
	} 

	if args.Term > rf.currentTerm {
		//If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
		rf.to_follower(args.Term, -1)
	}

	reply.Term = rf.currentTerm
	if	(rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.getLastTerm() || (args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex()) ) {
		//2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// AppendEntries RPC
//
type AppendEntriesArgs struct {
	Term		int //leader’s term
	LeaderId	int //so follower can redirect clients
	PrevLogIndex	int //index of log entry immediately preceding new ones
	PrevLogTerm		int //term of prevLogIndex entry
	Entries		[]LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency) 
	LeaderCommit	int //leader’s commitIndex
}
type AppendEntriesReply struct {
	Term	int //currentTerm, for leader to update itself 
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if	args.Term < rf.currentTerm ||
		args.PrevLogIndex > rf.getLastIndex() || 
		args.PrevLogTerm != rf.log[args.PrevLogIndex].LogTerm {
		//1. Reply false if term < currentTerm (§5.1)
		//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} 
	
	//3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	if args.PrevLogIndex < len(rf.log) {
		//args.Entries[i].LogTerm != rf.log[args.PrevLogIndex+i].LogTerm {
		//truncate log
		rf.log = rf.log[0:args.PrevLogIndex+1]
	}
		
	for i:=0; i<len(args.Entries); i++ {
		//4. Append any new entries not already in the log
		entry := args.Entries[i]
		//fmt.Printf("debug entry:%v\n", entry)
		rf.log = append(rf.log, LogEntry{LogTerm:entry.LogTerm, LogCmd:entry.LogCmd}) 
		fmt.Printf("show S%v log:%v\n", rf.me, rf.log)
	}
		
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		min := args.LeaderCommit
		if len(rf.log) < min {
			min = len(rf.log)
		}
		rf.commitIndex = min
	}

	//and do apply
	rf.doApply()

	
	reply.Success = true
	rf.to_follower(args.Term, args.LeaderId);
	reply.Term = rf.currentTerm
}
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Printf("\tdebug in AppendEntries end reply%v\n",reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := 0
	term := rf.currentTerm
	isLeader := (rf.me == rf.leaderId)

	if !isLeader {
		return index, term, isLeader
	}

	//If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
	index = rf.getLastIndex() + 1
	rf.log = append(rf.log, LogEntry{LogTerm:term, LogCmd:command}) 
	//index = rf.getLastIndex() + 1
	//rf.log = append(rf.log, LogEntry{LogTerm:term,LogCmd:command,LogIndex:index}) 
	//rf.persist()

	//use ApplyMsg inform client 
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.stop_ch <- true
}

func (rf *Raft) doApply() {
	//fmt.Printf("begin doApply S%v %v %v\n", rf.me, rf.commitIndex,rf.lastApplied)
	
	//If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		
		msg := ApplyMsg{Index:rf.lastApplied,Command:rf.log[rf.lastApplied].LogCmd}
		fmt.Printf("apply msg S%v [%v]\n", rf.me, msg)
		rf.apply_ch <- msg
	}
}

func (rf *Raft) boardcastRV() {
	fmt.Printf("boardcastRV S%v\n", rf.me)
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(i, args, &reply)
				if (ok) {
					rf.rv_ch <- reply
				}
			}(i)
		}
	}
}

func (rf *Raft) boardcastAE() {
	fmt.Printf("boardcastAE S%v\n", rf.me)
	
	/*
	Term		int //leader’s term
	LeaderId	int //so follower can redirect clients
	PrevLogIndex	int //index of log entry immediately preceding new ones
	PrevLogTerm		int //term of prevLogIndex entry
	Entries		[]LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency) 
	LeaderCommit	int //leader’s commitIndex
	*/

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				//fmt.Printf("\tdebug in go %v\n", i)
				args.PrevLogIndex = rf.nextIndex[i]-1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].LogTerm
				//If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
				log_cnt := len(rf.log)-args.PrevLogIndex-1
				if log_cnt <= 0 {
					args.Entries = make([]LogEntry, 0 )
				} else {
					args.Entries = make([]LogEntry, log_cnt )
					copy(args.Entries, rf.log[args.PrevLogIndex+1:])
				}
				fmt.Printf("ae send log[%v:len%v,%v] to S%v\n", args.PrevLogIndex+1, log_cnt, args.Entries, i)
				
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(i, args, &reply)

				if (ok) {
					//• If successful: update nextIndex and matchIndex for follower (§5.3)
					//• If AppendEntries fails because of log inconsistency:decrement nextIndex and retry (§5.3)
					if reply.Success {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = len(rf.log)-1
					} else {
						rf.nextIndex[i]-- //slow backtrace
					}
					
					//no need reply now
					rf.ae_ch <- reply
					//fmt.Printf("\tdebug after <- in go %v\n", i)
				}
			}(i)
		}
	}
}


func (rf *Raft) to_candidate() {
	rf.role = CANDIDATE
	ec_timeout := time.Duration(rand.Int63() % TM_EC + TM_EC) * time.Millisecond
	hb_timeout := 100000 * time.Millisecond

	rf.ec_timer.Reset(ec_timeout)
	rf.hb_timer.Reset(hb_timeout)

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.recvedVoteNum = 1
	rf.leaderId = -1
}
func (rf *Raft) to_follower(term int, leaderid int) {
	rf.role = FOLLOWER
	ec_timeout := time.Duration(rand.Int63() % TM_EC + TM_EC) * time.Millisecond
	hb_timeout := 100000 * time.Millisecond

	rf.ec_timer.Reset(ec_timeout)
	rf.hb_timer.Reset(hb_timeout)

	rf.currentTerm = term
	rf.votedFor = -1
	rf.recvedVoteNum = 1
	rf.leaderId = leaderid
}
func (rf *Raft) to_leader() {
	rf.role = LEADER
	//rf.votedFor = -1
	rf.leaderId = rf.me

	//Reinitialized 2Index after election
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastIndex() + 1 //initialized to leader last log index + 1
		rf.matchIndex[i] = 0
	}

	rf.ec_timer.Reset(100000*time.Millisecond)
	rf.hb_timer.Reset(TM_HB*time.Millisecond)
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
	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.recvedVoteNum = 0
	rf.leaderId = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.role = FOLLOWER
	//rf.rv_ch = make(chan RequestVoteReply, 1) //FIXME 1 to small, will block!
	rf.rv_ch = make(chan RequestVoteReply, 100)
	rf.ae_ch = make(chan AppendEntriesReply, 100)
	rf.stop_ch = make(chan bool,1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.apply_ch = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		//set init ec timer
		ec_timeout := time.Duration(rand.Int63() % TM_EC + TM_EC) * time.Millisecond
		hb_timeout := 100000 * time.Millisecond
		rf.ec_timer = time.NewTimer(ec_timeout)
		rf.hb_timer = time.NewTimer(hb_timeout)
	
		//core loop
		for {
			//wait event
			select {
			case <-rf.ec_timer.C:
				rf.mu.Lock()
				fmt.Printf("election timeout S%v\n", rf.me)
				//do action
				switch rf.role {
				case FOLLOWER,CANDIDATE:
					//Start a new round of elections
					rf.to_candidate()
					
					rf.boardcastRV()
				case LEADER:
					fmt.Println("election timeout and is LEADER, do nothing")
				}
				rf.mu.Unlock()
			case reply := <-rf.rv_ch:
				rf.mu.Lock()
				fmt.Printf("recved RV reply S%v %v\n", rf.me, reply)
				if reply.Term > rf.currentTerm {
					rf.to_follower(reply.Term, -1)
				} else {
					switch rf.role {
					//case CANDIDATE,FOLLOWER:
					case CANDIDATE:
						//check recv vote num
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
						}
						if reply.VoteGranted {
							rf.recvedVoteNum++
						}

						if rf.recvedVoteNum > len(rf.peers)/2 {
							rf.to_leader()
							//send ae rpc
							rf.boardcastAE()
						} 
					//case LEADER:
					case FOLLOWER,LEADER:
						//fmt.Println("recv RV reply and is LEADER, do nothing!")
						fmt.Println("recv RV reply and is FOLLOWER/LEADER, do nothing!")
					}
				}
				rf.mu.Unlock()
			case reply := <-rf.ae_ch:
				rf.mu.Lock()
				fmt.Printf("recv ae reply: %v\n", reply)
				if reply.Term > rf.currentTerm {
					rf.to_follower(reply.Term, -1)
				}
				if reply.Success && rf.me == rf.leaderId {
					fmt.Printf("show matchIndex %v %v\n", rf.matchIndex, rf.log)
					
					//only leader recv AE reply in here?
					
					//If there *exists* an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
					N := rf.commitIndex
					last := rf.getLastIndex()
					for i:= rf.commitIndex + 1; i <= last; i++ { 
						if rf.log[i].LogTerm != rf.currentTerm {
							continue
						}
				
						//var cnt int = 0
						var cnt int = 1 //leader always match itself
						for j := range rf.peers {
							if rf.matchIndex[j] >= i {
								cnt++
							}
						}
						if cnt > len(rf.peers)/2 {
							N = i
						}
					}
					rf.commitIndex = N
					rf.doApply()
				}

				rf.mu.Unlock()

			case <-rf.hb_timer.C:
				switch rf.role {
				case LEADER:
					rf.mu.Lock()
					rf.ec_timer.Reset(100000*time.Millisecond)
					rf.hb_timer.Reset(TM_HB*time.Millisecond)
					rf.mu.Unlock()
					//send ae rpc
					rf.boardcastAE()
				case FOLLOWER,CANDIDATE:
					fmt.Println("heartbeat timeout and is FOLLOWER/CANDIDATE, do nothing!")
				}
			case <-rf.stop_ch:
				fmt.Printf("S%v normal stop\n", rf.me)
			}
		}
	}()

	return rf
}


