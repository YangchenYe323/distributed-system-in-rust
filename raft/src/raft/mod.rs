use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::time::Duration;

use futures::channel::mpsc::UnboundedSender;
use futures::executor::ThreadPool;
// use futures::lock::Mutex;
use futures_timer::Delay;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use rand::Rng;

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

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

const VOTED_FOR_NO_ONE: usize = usize::MAX;

const TICK_TIME_OUT: u64 = 200;

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    voted_for: usize,
    // how many ticks is required to trigger an election
    election_ticks: u64,
    // in how many ticks do we send a hear beat as leader?
    heart_beat_ticks: u64,
    // number of tickes elapsed since last time of receiving message
    election_ticks_elapsed: u64,
    // number of tickes elapsed since last time of sending heart beat
    heart_beat_ticks_elapsed: u64,
    // count how many vote we get
    get_vote_from: usize,
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
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        let mut rng = rand::thread_rng();
        // one tick is 200ms, we want to make election timeout
        // between 800ms and 1200ms
        let election_ticks: u64 = rng.gen_range(4, 6);

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            voted_for: VOTED_FOR_NO_ONE,
            election_ticks,
            election_ticks_elapsed: 0,
            heart_beat_ticks: 1,
            heart_beat_ticks_elapsed: 0,
            get_vote_from: 0,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
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
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.send(res).unwrap();
        });
        rx
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

    fn become_follower(&mut self, term: u64) {
        self.state = Arc::new(State {
            term,
            is_leader: false,
        });
        self.voted_for = VOTED_FOR_NO_ONE;
    }

    fn become_candidate(&mut self) {
        self.state = Arc::new(State {
            term: self.state.term + 1,
            is_leader: false,
        });
    }

    fn become_leader(&mut self) {
        self.state = Arc::new(State {
            term: self.state.term,
            is_leader: true,
        })
    }

    fn heart_beat(&mut self) {}
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
    raft: Arc<std::sync::Mutex<Raft>>,
    pool: ThreadPool,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let raft = Arc::new(std::sync::Mutex::new(raft));

        let node = Node {
            raft,
            pool: ThreadPool::new().unwrap(),
        };

        let node_clone = node.clone();
        node.pool.spawn_ok(node_clone.tick());

        node
    }

    async fn tick(self) {
        loop {
            Delay::new(Duration::from_millis(TICK_TIME_OUT)).await;
            let mut rf = self.raft.lock().unwrap();
            if rf.state.is_leader {
                if rf.heart_beat_ticks_elapsed >= rf.heart_beat_ticks {
                    // heart beat
                    rf.heart_beat();
                    rf.heart_beat_ticks_elapsed = 0;
                } else {
                    rf.heart_beat_ticks_elapsed += 1;
                }
            } else if rf.election_ticks_elapsed >= rf.election_ticks {
                rf.become_candidate();
                rf.election_ticks_elapsed = 0;

                let args = RequestVoteArgs {
                    term: rf.state.term,
                    candidate_id: rf.me as u64,
                    last_log_index: 0,
                    last_log_term: 0,
                };

                for peer in &rf.peers {
                    let peer_clone = peer.clone();
                    let arg = args.clone();
                    let rf_clone = Arc::clone(&self.raft);
                    // send a request-vote rpc
                    self.pool.spawn_ok(async move {
                        let response = peer_clone.request_vote(&arg).await;
                        match response {
                            Ok(RequestVoteReply { term, vote_granted }) => {
                                let mut rf_guard = rf_clone.lock().unwrap();
                                if term > rf_guard.state.term {
                                    rf_guard.become_follower(term);
                                } else if term == rf_guard.state.term && vote_granted {
                                    rf_guard.get_vote_from += 1;
                                    // get most vote
                                    if rf_guard.get_vote_from > rf_guard.peers.len() / 2 {
                                        rf_guard.become_leader();
                                    }
                                }
                            }
                            Err(error) => {
                                println!("{}", error);
                            }
                        }
                    })
                }
            } else {
                rf.election_ticks_elapsed += 1;
            }
            // release the lock and go to another sleep
            drop(rf);
        }
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
        let rf = self.raft.lock().unwrap();
        rf.state.term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        let rf = self.raft.lock().unwrap();
        rf.state.is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
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
        let mut raft = self.raft.lock().unwrap();
        if args.term > raft.state.term {
            raft.become_follower(args.term);
        }

        if args.term < raft.state.term {
            // current term is larger
            Ok(RequestVoteReply {
                term: raft.state.term,
                vote_granted: false,
            })
        } else {
            let candidate_id = args.candidate_id as usize;
            if raft.voted_for == candidate_id || raft.voted_for == VOTED_FOR_NO_ONE {
                raft.voted_for = candidate_id;
                Ok(RequestVoteReply {
                    term: raft.state.term,
                    vote_granted: true,
                })
            } else {
                Ok(RequestVoteReply {
                    term: raft.state.term,
                    vote_granted: false,
                })
            }
        }
    }
}
