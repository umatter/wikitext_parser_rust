//! Byte offset to character offset mapping.

/// Maps byte offsets of a text to character indexes.
///
/// Built as a bitset marking the first byte of each char plus a prefix
/// popcount per 64-byte word: about 0.16 bytes of index per text byte, O(n)
/// setup and O(1) lookup (within the O(log n) the contract allows).
pub struct CharIndex {
    starts: Vec<u64>,
    ranks: Vec<u32>,
    len: usize,
    chars: u32,
}

impl CharIndex {
    pub fn new(text: &str) -> CharIndex {
        let len = text.len();
        let mut starts = vec![0u64; len / 64 + 1];
        for (i, b) in text.as_bytes().iter().enumerate() {
            if b & 0xC0 != 0x80 {
                starts[i / 64] |= 1 << (i % 64);
            }
        }
        let mut ranks = Vec::with_capacity(starts.len());
        let mut acc = 0u32;
        for w in &starts {
            ranks.push(acc);
            acc += w.count_ones();
        }
        CharIndex { starts, ranks, len, chars: acc }
    }

    /// Chars before `byte`. A byte inside a multi-byte char gives the index of
    /// the char containing it; offsets past the end give the char count.
    pub fn char_of(&self, byte: usize) -> i32 {
        if byte >= self.len {
            return self.chars as i32;
        }
        let (w, bit) = (byte / 64, byte % 64);
        let below = self.ranks[w] + (self.starts[w] & ((1u64 << bit) - 1)).count_ones();
        if self.starts[w] >> bit & 1 == 1 {
            below as i32
        } else {
            below as i32 - 1
        }
    }
}
