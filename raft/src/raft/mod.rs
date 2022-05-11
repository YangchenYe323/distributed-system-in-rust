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

mod log;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::append_entry_args::Entry;
use crate::proto::raftpb::*;

const TICK_TIME_OUT: u64 = 100;

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
    log: log::Log,
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

    fn log(&self) -> &log::Log {
        &self.log
    }

    fn log_mut(&mut self) -> &mut log::Log {
        &mut self.log
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
    apply_ch: UnboundedSender<ApplyMsg>,

    // raft sends its RPCEvent to this channel, which will be polled
    // in a background thread
    // entry: (from, last_log_index, event)
    // last_log_index is used to update peer's next_index and match_index
    rpc_ch: Option<Sender<(usize, u64, RPCEvent)>>,

    // leader state
    // peer -> index of next log entry to send
    next_index: Vec<u64>,
    // peer -> last matching log index
    match_index: Vec<u64>,
    // index of the largest committed log entry
    // a log n is committed if for more than half peers j,
    // next_index[j] >= n
    commit_index: u64,
    // flag indicating whether the leader has committed
    // in the current term
    has_committed_in_term: bool,
    // index of last applied log
    apply_index: u64,
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
        let num_peers = peers.len();

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
            apply_ch,
            rpc_ch: None,
            next_index: vec![0; num_peers],
            match_index: vec![0; num_peers],
            commit_index: 0,
            has_committed_in_term: false,
            apply_index: 0,
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
        self.election_next_time = self.boot_time.elapsed().as_millis() + get_election_timeout();
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
                        last_log_term: self.state.log().last_log_term(),
                        last_log_index: self.state.log().last_log_index(),
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
        let last_log_index = self.state.log().last_log_index();
        // next index is initialized to last_log_index + 1 so that the new
        // leader will never try to sync a log entry of previous terms
        self.next_index = vec![last_log_index + 1; self.peers.len()];
        self.match_index = vec![0; self.peers.len()];
        self.has_committed_in_term = false;

        // start heart beat
        self.heartbeat();
    }

    fn tick(&mut self) {
        if let Some(role) = self.role {
            match role {
                Role::Leader => {
                    self.heartbeat();

                    // // sync log
                    // let index = self.state.log().last_log_index();
                    // for peer in 0..self.peers.len() {
                    //     if peer != self.me && self.next_index[peer] <= index {
                    //         self.send_log_to(peer);
                    //     }
                    // }
                }
                Role::Follower | Role::Candidate => {
                    if self.boot_time.elapsed().as_millis() >= self.election_next_time {
                        self.become_candidate();
                    }
                }
            }

            // apply message
            if self.apply_index < self.commit_index {
                self.apply();
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
        let last_log_index = args.last_log_index;

        let rpc_ch = self.rpc_ch.clone();

        if let Some(ch) = rpc_ch {
            peer.spawn(async move {
                let reply = peer_clone.request_vote(&args).await;

                if let Ok(reply) = reply {
                    ch.send((server, last_log_index, RPCEvent::RequestVote(reply)))
                        .expect("RPC listening end is dropped");
                }
            })
        }
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.state.term() {
            self.become_follower(args.term);
        }

        // check if we can grant this vote
        let vote_granted = if args.term == self.state.term() {
            match self.state.vote_for().as_ref() {
                // we have voted for someone in the current term
                Some(&old) => old == args.candidate_id,
                // check log up_to_date
                None => self.can_vote_for(args.last_log_term, args.last_log_index),
            }
        } else {
            // leader's term < our term
            false
        };

        if vote_granted {
            *self.state.vote_for_mut() = Some(args.candidate_id);
            self.election_next_time = self.boot_time.elapsed().as_millis() + get_election_timeout();
            debug!(
                "Raft {} Votes for {} in Term {}",
                self.me,
                args.candidate_id,
                self.state.term()
            );
        }

        RequestVoteReply {
            term: self.state.term(),
            vote_granted,
        }
    }

    fn can_vote_for(&mut self, last_log_term: u64, last_log_index: u64) -> bool {
        let self_last_log_index = self.state.log().last_log_index();
        let self_last_log_term = self.state.log().last_log_term();

        last_log_term > self_last_log_term // leader's last term is newer
            || (last_log_term == self_last_log_term // leader's lat term is the same and leader's log is longer
                && last_log_index >= self_last_log_index)
    }

    fn heartbeat(&mut self) {
        for peer in 0..self.peers.len() {
            if peer != self.me {
                if self.has_committed_in_term {
                    self.send_log_to(peer);
                } else {
                    // we cannot sync with followers before we have
                    // committed a log in our current term,
                    // so just use heartbeat to avoid re-election fire
                    self.send_append_entry(
                        peer,
                        AppendEntryArgs {
                            term: self.state.term(),
                            leader_id: self.me as u64,
                            prev_log_term: 0,
                            prev_log_index: 0,
                            entries: vec![],
                            leader_commit: 0,
                        },
                    );
                }
            }
        }
    }

    fn send_append_entry(&self, server: usize, args: AppendEntryArgs) {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        // last_log_index is the index of the last log we send in this rpc
        let last_log_index = args.prev_log_index + args.entries.len() as u64;

        let rpc_ch = self.rpc_ch.clone();

        if let Some(ch) = rpc_ch {
            peer.spawn(async move {
                let reply = peer_clone.append_entry(&args).await;

                if let Ok(reply) = reply {
                    ch.send((server, last_log_index, RPCEvent::AppendEntry(reply)))
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

            // check if we have the same log at prev_log_index
            // for heartbeat: index = 0 and term = 0, which will always match
            // as this is the first trivial log
            args.prev_log_index <= self.state.log().last_log_index()
                && args.prev_log_term == self.state.log().term_at_index(args.prev_log_index)
        } else {
            false
        };

        if success {
            // try to sync log
            let mut start_log_index = args.prev_log_index + 1;
            // note: heartbeat message has no entries
            for Entry { term, data } in args.entries {
                // here we conflict with leader, so truncate
                // all our existing log entries
                if start_log_index <= self.state.log().last_log_index()
                    && self.state.log().term_at_index(start_log_index) != term
                {
                    debug!(
                        "Raft {} differs with Leader {} on Inde {}: {} vs {}",
                        self.me,
                        args.leader_id,
                        start_log_index,
                        self.state.log().term_at_index(start_log_index),
                        term,
                    );

                    self.state.log_mut().clear_from_index(start_log_index);
                }

                // start_log_index might be greater than last_log_index due to
                // previous truncating
                if start_log_index > self.state.log().last_log_index() {
                    self.state.log_mut().append_log((term, data));
                }

                start_log_index += 1;
            }
            self.commit_index = std::cmp::max(self.commit_index, args.leader_commit);
        }

        AppendEntryReply {
            term: self.state.term(),
            success,
        }
    }

    // start syncing on the given command,
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.role != Some(Role::Leader) {
            Err(Error::NotLeader)
        } else {
            info!("Raft {} starts agreement on {:?}", self.me, command);

            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            let term = self.state.term;
            let index = self.state.log_mut().append_log((term, buf));

            // I and myself are trivially matched
            self.next_index[self.me] = index + 1;
            self.match_index[self.me] = index;

            for peer in 0..self.peers.len() {
                if peer != self.me && self.next_index[peer] <= index {
                    self.send_log_to(peer);
                }
            }

            Ok((index, term))
        }
    }

    // synchronize log entries with peer
    fn send_log_to(&mut self, peer: usize) {
        let mut entries = vec![];
        // this is the starting point of the entries we send
        // to this peer
        let next_index = self.next_index[peer];
        // this is the entry before the starting point, we need to
        // agree with our peer on what this entry is
        let prev_index = next_index - 1;
        let prev_term = self.state.log().term_at_index(prev_index);

        // build entries
        let last_index = self.state.log().last_log_index();
        for i in next_index..=last_index {
            let (term, data) = self.state.log().log_at_index(i);
            entries.push(Entry { term, data });
        }
        let args = AppendEntryArgs {
            term: self.state.term(),
            leader_id: self.me as u64,
            prev_log_term: prev_term,
            prev_log_index: prev_index,
            entries,
            leader_commit: self.commit_index,
        };

        debug!(
            "Raft {} Sync Log With {}: {} -> {}",
            self.me, peer, prev_index, last_index
        );

        self.send_append_entry(peer, args);
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

    fn on_append_entry_reply(&mut self, from: usize, last_log_index: u64, reply: AppendEntryReply) {
        if reply.term > self.state.term() {
            self.become_follower(reply.term);
        }

        // if we still care about this reply?
        if reply.term == self.state.term() && self.role == Some(Role::Leader) {
            if reply.success {
                debug!(
                    "Raft {} agreed with Follower {}, -> {}",
                    self.me, from, last_log_index
                );
                self.next_index[from] = last_log_index + 1;
                self.match_index[from] = last_log_index;
                self.try_commit();
            } else {
                // println!("BBBBB");
                self.next_index[from] -= 1;
                self.send_log_to(from);
            }
        }
    }

    fn on_event(&mut self, from: usize, last_log_index: u64, event: RPCEvent) {
        match event {
            RPCEvent::RequestVote(reply) => self.on_request_vote_reply(from, reply),
            RPCEvent::AppendEntry(reply) => self.on_append_entry_reply(from, last_log_index, reply),
        }
    }

    fn try_commit(&mut self) {
        for n in (self.commit_index + 1)..=self.state.log().last_log_index() {
            let count = self
                .match_index
                .iter()
                .filter(|match_index| **match_index >= n)
                .count();

            if count > self.peers.len() / 2 {
                // more than half of the peers have agreed
                // on this index
                // progress commit index
                debug!("Raft {} commits log index {}", self.me, n);
                self.has_committed_in_term = true;
                self.commit_index = n;
            }
        }
    }

    fn apply(&mut self) {
        for index in (self.apply_index + 1)..=self.commit_index {
            // if a follower disconnected and then returned to
            // group, the group might have committed many entries
            // which it does not have.
            if index > self.state.log().last_log_index() {
                break;
            }

            let (_term, entry) = self.state.log().log_at_index(index);
            let msg = ApplyMsg::Command { data: entry, index };
            self.apply_ch
                .unbounded_send(msg)
                .expect("Cannot Send to Receiving End");
            self.apply_index = index;
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
            for (from, last_log_index, event) in rx.iter() {
                // info!("Raft {} get RPC {:?} from {}", me, event, from);
                {
                    let mut rf_guard = rf.lock().unwrap();

                    if let Some(rf_guard) = rf_guard.as_mut() {
                        rf_guard.on_event(from, last_log_index, event);
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
        let mut raft = self.raft.lock().unwrap();
        if let Some(raft) = raft.as_mut() {
            raft.start(command)
        } else {
            debug!("Start command on closed raft");
            Ok((0, 0))
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
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
        let mut raft = self.raft.lock().unwrap();
        raft.take();
        // raft instance dropped here
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
