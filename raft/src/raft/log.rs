use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
/// A Raft Log
/// It uses a start_index to hide the implementation
/// detail of truncating and snapshoting, providing a
/// unified API of an ever-growing logs.
#[derive(Debug, Serialize, Deserialize)]
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
            panic!(
                "Get log index {} < start index {}, should be a snapshot",
                i, self.start_index
            );
        }
        let effective_index = i - self.start_index;
        self.entries[effective_index].clone()
    }

    pub fn term_at_index(&self, i: u64) -> u64 {
        let i = i as usize;
        if i < self.start_index {
            panic!(
                "Get log index {} < start index {}, should be a snapshot",
                i, self.start_index
            );
        }
        let effective_index = i - self.start_index;
        self.entries[effective_index].0
    }

    pub fn first_log_index(&self) -> u64 {
        self.start_index as u64
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

    // find the first index in the log with given term
    // start search from start_index backward
    pub fn first_index_at_term_before(&self, term: u64, start_index: u64) -> u64 {
        // sanity check
        assert_eq!(self.term_at_index(start_index), term);

        let start_index = start_index as usize;
        let effective_index = start_index - self.start_index;

        let (mut l, mut r) = (0, effective_index);
        while l < r {
            let mid = (l + r) / 2;
            let (cur_term, _) = self.entries[mid];
            if cur_term < term {
                l = mid + 1;
            } else {
                r = mid;
            }
        }

        (l + self.start_index) as u64
    }

    // find the last index at the given term
    pub fn last_index_at_term(&self, term: u64) -> Option<u64> {
        let (mut l, mut r) = (0, self.entries.len());

        while l < r {
            let mid = (l + r) / 2;
            let (cur_term, _) = self.entries[mid];
            match cur_term.cmp(&term) {
                Ordering::Less | Ordering::Equal => {
                    l = mid + 1;
                }
                Ordering::Greater => {
                    r = mid;
                }
            }
        }

        if l > 0 && self.entries[l - 1].0 == term {
            Some((l - 1 + self.start_index) as u64)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_log_first_index_at_term() {
		let mut l = Log::new();
		// log entry terms are [1, 1, 2, 2, 2, 3, 3, 3, 4]
		let terms = [1, 1, 2, 2, 2, 3, 3, 3, 4];
		for t in terms {
			l.append_log((t, vec![]));
		}
		assert_eq!(3, l.first_index_at_term_before(2, 3));
		assert_eq!(6, l.first_index_at_term_before(3, 7));
	}

	#[test]
	fn test_log_last_index_at_term_exist() {
		let mut l = Log::new();
		// log entry terms are [1, 1, 2, 2, 2, 3, 3, 3, 4]
		let terms = [1, 1, 2, 2, 2, 3, 3, 3, 4];
		for t in terms {
			l.append_log((t, vec![]));
		}
		assert_eq!(Some(2), l.last_index_at_term(1));
		assert_eq!(Some(5), l.last_index_at_term(2));
		assert_eq!(Some(8), l.last_index_at_term(3));
		assert_eq!(Some(9), l.last_index_at_term(4));
	}

	#[test]
	fn test_log_last_index_at_term_integrated() {
		let mut l = Log::new();
		// log entry terms are [1, 3, 3, 5, 5, 7, 9, 11, 11, 11]
		let terms = [1, 3, 3, 5, 5, 7, 9, 11, 11, 11];
		for t in terms {
			l.append_log((t, vec![]));
		}
		// should not find
		assert_eq!(None, l.last_index_at_term(2));
		assert_eq!(None, l.last_index_at_term(4));
		assert_eq!(None, l.last_index_at_term(6));
		assert_eq!(None, l.last_index_at_term(10));

		// should find
		assert_eq!(Some(1), l.last_index_at_term(1));
		assert_eq!(Some(3), l.last_index_at_term(3));
		assert_eq!(Some(5), l.last_index_at_term(5));
		assert_eq!(Some(6), l.last_index_at_term(7));
		assert_eq!(Some(7), l.last_index_at_term(9));
		assert_eq!(Some(10), l.last_index_at_term(11));
	}
}