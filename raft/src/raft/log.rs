/// A Raft Log
/// It uses a start_index to hide the implementation
/// detail of truncating and snapshoting, providing a
/// unified API of an ever-growing logs.
#[derive(Debug)]
pub struct Log {
    // log starts from start_index,
    // entries before start_index is dropped
    // and replaced by a snapshot
    start_index: usize,

    // log entries: (term, data)
    entries: Vec<(u64, Vec<u8>)>,
}

impl Default for Log {
    fn default() -> Self {
        Log::new()
    }
}

impl Log {
    /// create a new log starting at 0
    pub fn new() -> Log {
        Log {
            start_index: 0,
            // initialize with a dummy entry
            entries: vec![(0, vec![])],
        }
    }

    /// get the log entry stored at index i
    pub fn log_at_index(&self, i: u64) -> (u64, Vec<u8>) {
        let i = i as usize;
        if i < self.start_index {
            debug!(
                "Get log index {} < start index {}, should be a snapshot",
                i, self.start_index
            );
            return (0, vec![]);
        }
        let effective_index = i - self.start_index;
        self.entries[effective_index].clone()
    }

    pub fn term_at_index(&self, i: u64) -> u64 {
        let i = i as usize;
        if i < self.start_index {
            debug!(
                "Get log index {} < start index {}, should be a snapshot",
                i, self.start_index
            );
            return 0;
        }
        let effective_index = i - self.start_index;
        self.entries[effective_index].0
    }

    pub fn last_log_index(&self) -> u64 {
        (self.start_index + self.entries.len() - 1) as u64
    }

    //
    pub fn last_log_term(&self) -> u64 {
        let (term, _) = self.entries[self.entries.len() - 1];
        term
    }

    pub fn append_log(&mut self, new_entry: (u64, Vec<u8>)) -> u64 {
        self.entries.push(new_entry);
        self.last_log_index()
    }

    // delete log entry index..last
    pub fn clear_from_index(&mut self, index: u64) {
        let index = index as usize;
        if index < self.start_index {
            panic!(
                "Start index {}, Try to clear index {}",
                self.start_index, index
            );
        }

        let effective_index = index - self.start_index;

        self.entries.truncate(effective_index);
    }
}
