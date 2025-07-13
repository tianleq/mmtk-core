#[cfg(feature = "satb")]
pub(super) mod barrier;
#[cfg(feature = "satb")]
pub(super) mod concurrent_marking;

pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

pub use self::global::Immix;
pub use self::global::IMMIX_CONSTRAINTS;

#[cfg(feature = "satb")]
use bytemuck::NoUninit;

#[cfg(feature = "satb")]
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Copy, Clone, NoUninit)]
pub enum Pause {
    Full = 1,
    FullDefrag,
    InitialMark,
    FinalMark,
}
#[cfg(feature = "satb")]
unsafe impl bytemuck::ZeroableInOption for Pause {}
#[cfg(feature = "satb")]
unsafe impl bytemuck::PodInOption for Pause {}

impl Default for Pause {
    fn default() -> Self {
        Self::Full
    }
}
