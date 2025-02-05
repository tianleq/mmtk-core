use std::sync::atomic::Ordering;

use crate::{
    policy::immix::line::Line,
    util::{
        linear_scan::{Region, RegionIterator},
        Address,
    },
};

use super::block::{Block, BlockState};

#[derive(Debug, Clone, Copy, Eq, Hash)]
pub(crate) struct Hole {
    pub(crate) start: Address,
    pub(crate) size: usize,
}

impl PartialOrd for Hole {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.size.partial_cmp(&other.size)
    }
}

impl Default for Hole {
    fn default() -> Self {
        Self {
            start: Address::ZERO,
            size: 0,
        }
    }
}

impl Ord for Hole {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.size.cmp(&other.size)
    }
}

impl PartialEq for Hole {
    fn eq(&self, other: &Self) -> bool {
        let equal = self.start == other.start;
        debug_assert!(
            !equal || self.size == other.size,
            "left: {:?}, right: {:?}",
            self,
            other
        );
        debug_assert!(self.size % Line::BYTES == 0, "invalid size: {}", self.size);
        equal
    }
}

impl Hole {
    pub fn new(start: Address, size: usize) -> Self {
        Self { start, size }
    }

    pub fn init(&self, copy: bool) {
        let block = Block::from_unaligned_address(self.start);
        // The following is a benign race (multiple holes within the same block will set the same byte value concurrently)
        block.set_state(if copy {
            BlockState::Marked
        } else {
            BlockState::Unmarked
        });

        Block::DEFRAG_STATE_TABLE.store_atomic::<u8>(block.start(), 0, Ordering::SeqCst);
    }

    pub fn mark_all_lines(&self, state: u8) {
        for line in self.lines() {
            #[cfg(debug_assertions)]
            assert!(
                !line.is_marked(state),
                "hole: {:?} has marked line: {:?}",
                self,
                line
            );
            line.mark(state);
        }
    }

    pub fn lines(&self) -> RegionIterator<Line> {
        let start_line = Line::from_aligned_address(self.start);
        let end_line = Line::from_aligned_address(self.start + self.size);
        RegionIterator::<Line>::new(start_line, end_line)
    }

    pub fn start_line(&self) -> Line {
        Line::from_aligned_address(self.start)
    }

    pub fn end_line(&self) -> Line {
        Line::from_aligned_address(self.start + self.size)
    }
}
