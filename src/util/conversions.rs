use crate::util::constants::*;
use crate::util::heap::layout::vm_layout::*;
use crate::util::Address;

/// Is the address aligned to word boundary?
pub fn is_address_aligned(addr: Address) -> bool {
    addr.is_aligned_to(BYTES_IN_ADDRESS)
}

/// Align down an address to the nearest page.
pub fn page_align_down(address: Address) -> Address {
    address.align_down(BYTES_IN_PAGE)
}

/// Is the address aligned to page boundary?
pub fn is_page_aligned(address: Address) -> bool {
    address.is_aligned_to(BYTES_IN_PAGE)
}

/// Align up an address to the nearest chunk.
pub const fn chunk_align_up(addr: Address) -> Address {
    addr.align_up(BYTES_IN_CHUNK)
}

/// Align down an address to the nearest chunk.
pub const fn chunk_align_down(addr: Address) -> Address {
    addr.align_down(BYTES_IN_CHUNK)
}

/// Align up an address to the nearest chunk at which granularity we mmap memory.
pub const fn mmap_chunk_align_up(addr: Address) -> Address {
    addr.align_up(MMAP_CHUNK_BYTES)
}

/// Align down an address to the nearest chunk at which granularity we mmap memory.
pub const fn mmap_chunk_align_down(addr: Address) -> Address {
    addr.align_down(MMAP_CHUNK_BYTES)
}

/// Convert size in bytes to the number of chunks (aligned up).
pub fn bytes_to_chunks_up(bytes: usize) -> usize {
    (bytes + BYTES_IN_CHUNK - 1) >> LOG_BYTES_IN_CHUNK
}

/// Convert an address to the chunk index (aligned down).
pub fn address_to_chunk_index(addr: Address) -> usize {
    addr >> LOG_BYTES_IN_CHUNK
}

/// Convert a chunk index to the start address of the chunk.
pub fn chunk_index_to_address(chunk: usize) -> Address {
    unsafe { Address::from_usize(chunk << LOG_BYTES_IN_CHUNK) }
}

/// Align up an integer to the given alignment. `align` must be a power of two.
pub const fn raw_align_up(val: usize, align: usize) -> usize {
    // See https://github.com/rust-lang/rust/blob/e620d0f337d0643c757bab791fc7d88d63217704/src/libcore/alloc.rs#L192
    val.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1)
}

/// Align down an integer to the given alignment. `align` must be a power of two.
pub const fn raw_align_down(val: usize, align: usize) -> usize {
    val & !align.wrapping_sub(1)
}

/// Is the integer aligned to the given alignment? `align` must be a power of two.
pub const fn raw_is_aligned(val: usize, align: usize) -> bool {
    val & align.wrapping_sub(1) == 0
}

/// Convert the number of pages to bytes.
pub fn pages_to_bytes(pages: usize) -> usize {
    pages << LOG_BYTES_IN_PAGE
}

/// Convert size in bytes to the number of pages (aligned up)
pub fn bytes_to_pages_up(bytes: usize) -> usize {
    raw_align_up(bytes, BYTES_IN_PAGE) >> LOG_BYTES_IN_PAGE
}

/// Convert size in bytes to a readable short string, such as 1GB, 2TB, etc. It only keeps the major unit and keeps no fraction.
pub fn bytes_to_formatted_string(bytes: usize) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut i = 0;
    let mut num = bytes;
    while i < UNITS.len() - 1 {
        let new_num = num >> 10;
        if new_num == 0 {
            return format!("{}{}", num, UNITS[i]);
        }
        num = new_num;
        i += 1;
    }
    format!("{}{}", num, UNITS.last().unwrap())
}

/// Shift `num` by `bits` to the right.  Add 1 to the result if any `1` bits are shifted out to the
/// right.  This is equivalent to dividing `num` by 2 to the power of `bits`, rounding up.
///
/// This function has undefined behavior if `bits` is greater or equal to the number of bits in
/// `usize`.
pub const fn rshift_align_up(num: usize, bits: usize) -> usize {
    (num + ((1 << bits) - 1)) >> bits
}

#[cfg(test)]
mod tests {
    use crate::util::conversions::*;
    use crate::util::Address;

    #[test]
    fn test_page_align() {
        let addr = unsafe { Address::from_usize(0x2345_6789) };
        assert_eq!(page_align_down(addr), unsafe {
            Address::from_usize(0x2345_6000)
        });
        assert!(!is_page_aligned(addr));
        assert!(is_page_aligned(page_align_down(addr)));
    }

    #[test]
    fn test_chunk_align() {
        let addr = unsafe { Address::from_usize(0x2345_6789) };
        assert_eq!(chunk_align_down(addr), unsafe {
            Address::from_usize(0x2340_0000)
        });
        assert_eq!(chunk_align_up(addr), unsafe {
            Address::from_usize(0x2380_0000)
        });
    }

    #[test]
    fn test_bytes_to_formatted_string() {
        assert_eq!(bytes_to_formatted_string(0), "0B");
        assert_eq!(bytes_to_formatted_string(1023), "1023B");
        assert_eq!(bytes_to_formatted_string(1024), "1KiB");
        assert_eq!(bytes_to_formatted_string(1025), "1KiB");
        assert_eq!(bytes_to_formatted_string(1 << 20), "1MiB");
        assert_eq!(bytes_to_formatted_string(1 << 30), "1GiB");
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(bytes_to_formatted_string(1 << 40), "1TiB");
            assert_eq!(bytes_to_formatted_string(1 << 50), "1PiB");
            assert_eq!(bytes_to_formatted_string(1 << 60), "1024PiB");
            assert_eq!(bytes_to_formatted_string(1 << 63), "8192PiB");
        }
    }
}
