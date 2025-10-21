#[cfg(feature = "public_bit")]
pub(crate) mod barrier;
pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

pub use self::global::Immix;
pub use self::global::IMMIX_CONSTRAINTS;

pub(crate) mod concurrent_gc_work;

use bytemuck::NoUninit;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Copy, Clone, NoUninit)]
pub enum Pause {
    Full = 1,
    InitialMark,
    FinalMark,
    Public,
}

unsafe impl bytemuck::ZeroableInOption for Pause {}

unsafe impl bytemuck::PodInOption for Pause {}

impl Default for Pause {
    fn default() -> Self {
        Self::Full
    }
}

lazy_static! {
    pub static ref GLOBAL_REMSET: std::sync::Mutex<std::collections::HashSet<crate::util::ObjectReference>> =
        std::sync::Mutex::new(std::collections::HashSet::new());
}
