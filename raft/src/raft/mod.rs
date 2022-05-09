use std::collections::HashSet;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot;
use rand::Rng;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const TICK_TIME_OUT: u64 = 200;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

/// Persistent State of a Raft Peer
#[derive(Default, Debug)]
struct PersistentState {
    // current term,
    term: u64,
    // leader that receives vote in current term
    vote_for: Option<u64>,
    // log: (term, data),
    _log: Vec<(u64, Vec<u8>)>,
}

impl PersistentState {
    fn term(&self) -> u64 {
        self.term
    }

    fn term_mut(&mut self) -> &mut u64 {
        &mut self.term
    }

    fn vote_for(&self) -> &Option<u64> {
        &self.vote_for
    }

    fn vote_for_mut(&mut self) -> &mut Option<u64> {
        &mut self.vote_for
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    pub fn term(&self) -> u64 {
        self.term
    }
}

/// An RPC reply received by the raft peer to be
/// processed in an unified way
#[derive(Debug)]
enum RPCEvent {
    RequestVote(RequestVoteReply),
    AppendEntry(AppendEntryReply),
}

// get a random election timeout between 800ms to 2000ms
fn get_election_timeout() -> u128 {
    rand::thread_rng().gen_range(800, 2000)
}
// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,

    // persistent state
    state: PersistentState,
    role: Option<Role>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain,

    // use boot time to keep track of elapsed time
    boot_time: Instant,
    // next time of heart beat (in millisecond) since boot
    // next time to start an election
    election_next_time: u128,

    // count vote from other peers
    get_vote_from: HashSet<u64>,

    // raft's apply channel
    _apply_ch: UnboundedSender<ApplyMsg>,

    // raft sends its RPCEvent to this channel, which will be polled
    // in a background thread
    // entry: (from, event)
    rpc_ch: Option<Sender<(usize, RPCEvent)>>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Default::default(),
            role: None,
            boot_time: Instant::now(),
            election_next_time: 0,
            get_vote_from: HashSet::new(),
            _apply_ch: apply_ch,
            rpc_ch: None,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);
        // term starts at 0
        rf.become_follower(0);

        rf
    }

    fn become_follower(&mut self, term: u64) {
        debug!("Raft {} becomes Follower at Term {}", self.me, term);
        // update state
        *self.state.term_mut() = term;
        *self.state.vote_for_mut() = None;

        self.role = Some(Role::Follower);
        self.election_next_time += get_election_timeout();
    }

    fn become_candidate(&mut self) {
        let new_term = self.state.term() + 1;
        debug!("Raft {} becomes Candidate at Term {}", self.me, new_term);

        // become candidate
        *self.state.term_mut() = new_term;
        *self.state.vote_for_mut() = Some(self.me as u64);
        self.role = Some(Role::Candidate);

        // vote for self
        self.get_vote_from.clear();
        self.get_vote_from.insert(self.me as u64);

        self.election_next_time = self.boot_time.elapsed().as_millis() + get_election_timeout();

        // start election
        self.start_election()
    }

    fn start_election(&mut self) {
        for peer in 0..self.peers.len() {
            if peer != self.me {
                self.send_request_vote(
                    peer,
                    RequestVoteArgs {
                        term: self.state.term(),
                        candidate_id: self.me as u64,
                        last_log_term: 0,
                        last_log_index: 0,
                    },
                )
            }
        }
    }

    fn become_leader(&mut self) {
        debug!(
            "Raft {} becomes Leader in Term {}",
            self.me,
            self.state.term()
        );

        self.role = Some(Role::Leader);

        // initialize leader state

        // start heart beat
        self.heartbeat();
    }

    fn tick(&mut self) {
        if let Some(role) = self.role {
            match role {
                Role::Leader => {
                    self.heartbeat();
                }
                Role::Follower | Role::Candidate => {
                    if self.boot_time.elapsed().as_millis() >= self.election_next_time {
                        self.become_candidate();
                    }
                }
            }
        }
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(&self, server: usize, args: RequestVoteArgs) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let rpc_ch = self.rpc_ch.clone();

        if let Some(ch) = rpc_ch {
            peer.spawn(async move {
                let reply = peer_clone.request_vote(&args).await;

                if let Ok(reply) = reply {
                    ch.send((server, RPCEvent::RequestVote(reply)))
                        .expect("RPC listening end is dropped");
                }
            })
        }
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.state.term() {
            self.become_follower(args.term);
        }

        let vote_granted = if args.term == self.state.term() {
            match self.state.vote_for().as_ref() {
                Some(&old) => old == args.candidate_id,
                None => {
                    self.can_vote_for(args.candidate_id, args.last_log_term, args.last_log_index)
                }
            }
        } else {
            false
        };

        RequestVoteReply {
            term: self.state.term(),
            vote_granted,
        }
    }

    fn can_vote_for(&mut self, candidate: u64, _last_log_term: u64, _last_log_index: u64) -> bool {
        // vote for candidate and suppress next election
        *self.state.vote_for_mut() = Some(candidate);
        self.election_next_time = self.boot_time.elapsed().as_millis() + get_election_timeout();
        debug!(
            "Raft {} Votes for {} in Term {}",
            self.me,
            candidate,
            self.state.term()
        );

        true
    }

    fn heartbeat(&self) {
        for peer in 0..self.peers.len() {
            if peer != self.me {
                self.send_append_entry(
                    peer,
                    AppendEntryArgs {
                        term: self.state.term(),
                        leader_id: self.me as u64,
                    },
                )
            }
        }
    }

    fn send_append_entry(&self, server: usize, args: AppendEntryArgs) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let rpc_ch = self.rpc_ch.clone();

        if let Some(ch) = rpc_ch {
            peer.spawn(async move {
                let reply = peer_clone.append_entry(&args).await;

                if let Ok(reply) = reply {
                    ch.send((server, RPCEvent::AppendEntry(reply)))
                        .expect("RPC listening end is dropped");
                }
            })
        }
    }

    fn handle_append_entry(&mut self, args: AppendEntryArgs) -> AppendEntryReply {
        if args.term > self.state.term() {
            self.become_follower(args.term);
        }

        let success = if args.term == self.state.term() {
            // suppress next election
            self.election_next_time = self.boot_time.elapsed().as_millis() + get_election_timeout();
            true
        } else {
            false
        };

        AppendEntryReply {
            term: self.state.term(),
            success,
        }
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }

    fn on_request_vote_reply(&mut self, from: usize, reply: RequestVoteReply) {
        // raft rule: any time a server encounters bigger term
        // it immediately becomes follower for the term
        if reply.term > self.state.term() {
            self.become_follower(reply.term);
        }

        // if we still care about this reply?
        if reply.term == self.state.term()
            && self.role == Some(Role::Candidate)
            && reply.vote_granted
        {
            self.get_vote_from.insert(from as u64);
            if self.get_vote_from.len() > self.peers.len() / 2 {
                self.become_leader();
            }
        }
    }

    fn on_append_entry_reply(&mut self, _from: usize, _reply: AppendEntryReply) {
        // pass
    }

    fn on_event(&mut self, from: usize, event: RPCEvent) {
        match event {
            RPCEvent::RequestVote(reply) => self.on_request_vote_reply(from, reply),
            RPCEvent::AppendEntry(reply) => self.on_append_entry_reply(from, reply),
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        let _ = self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Option<Raft>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(mut raft: Raft) -> Node {
        let (tx, rx) = mpsc::channel();
        // let me = raft.me;
        raft.rpc_ch = Some(tx);

        let node = Node {
            raft: Arc::new(Mutex::new(Some(raft))),
        };

        let rf = node.raft.clone();

        // ticker thread
        std::thread::spawn(move || loop {
            {
                let mut rf_guard = rf.lock().unwrap();

                if let Some(rf_guard) = rf_guard.as_mut() {
                    rf_guard.tick();
                } else {
                    break;
                }
            }

            std::thread::sleep(Duration::from_millis(TICK_TIME_OUT));
        });

        let rf = node.raft.clone();
        // rpc polling thread
        std::thread::spawn(move || {
            for (from, event) in rx.iter() {
                // info!("Raft {} get RPC {:?} from {}", me, event, from);
                {
                    let mut rf_guard = rf.lock().unwrap();

                    if let Some(rf_guard) = rf_guard.as_mut() {
                        rf_guard.on_event(from, event);
                    } else {
                        break;
                    }
                }
            }
        });

        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        let raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_ref() {
            raft.state.term()
        } else {
            debug!("Query term on closed raft");
            u64::MAX
        }
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        let raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_ref() {
            raft.role == Some(Role::Leader)
        } else {
            debug!("Query is_leader on closed raft");
            false
        }
    }

    pub fn get_state(&self) -> State {
        let raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_ref() {
            State {
                term: raft.state.term(),
                is_leader: raft.role == Some(Role::Leader),
            }
        } else {
            debug!("Query state on closed raft");
            State {
                term: 0,
                is_leader: false,
            }
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let raft = self.raft.clone();
        let (tx, rx) = oneshot::channel();

        // we use a background thread to wait for locking
        // and communicate via a channel
        std::thread::spawn(move || {
            let mut rf = raft.lock().unwrap();
            let rf_ref = rf.as_mut();
            let res = if let Some(rf) = rf_ref {
                Ok(rf.handle_request_vote(args))
            } else {
                Err(labrpc::Error::Stopped)
            };
            tx.send(res).expect("Receiving End Closed");
        });

        rx.await.expect("Sending End Closed")
    }

    async fn append_entry(&self, args: AppendEntryArgs) -> labrpc::Result<AppendEntryReply> {
        let raft = self.raft.clone();
        let (tx, rx) = oneshot::channel();

        // we use a background thread to wait for locking
        // and communicate via a channel
        std::thread::spawn(move || {
            let mut rf = raft.lock().unwrap();
            let rf_ref = rf.as_mut();
            let res = if let Some(rf) = rf_ref {
                Ok(rf.handle_append_entry(args))
            } else {
                Err(labrpc::Error::Stopped)
            };
            tx.send(res).expect("Receiving End Closed");
        });

        rx.await.expect("Sending End Closed")
    }
}
