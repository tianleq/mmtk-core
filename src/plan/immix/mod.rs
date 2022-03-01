pub(super) mod gc_work;
pub(super) mod global;
pub(super) mod mutator;

pub use self::global::Immix;
pub use self::global::IMMIX_CONSTRAINTS;
pub const MAX_RETRY_TRANSACTION: u8 = 50;
