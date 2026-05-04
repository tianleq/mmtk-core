#[cfg(feature = "public_bit")]
pub(crate) mod barrier;
pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

pub use self::global::Immix;
pub use self::global::IMMIX_CONSTRAINTS;
