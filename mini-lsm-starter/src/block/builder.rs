use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

fn compute_overlap(first_key: &[u8], key: &[u8]) -> usize {
    let mut i = 0;
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        if first_key[i] != key[i] {
            break;
        }
        i += 1;
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        /* size of data */
        self.data.len() + /* size of offsets */ (self.offsets.len() * SIZEOF_U16) + /* footer */ SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 > self.block_size
            && !self.is_empty()
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        let overlap = compute_overlap(self.first_key.raw_ref(), key.raw_ref());
        // encode key overlap
        self.data.put_u16(overlap as u16);
        // encode key length
        self.data.put_u16((key.len() - overlap) as u16);
        // encode key content
        self.data.put(&key.into_inner()[overlap..]);
        // encode value length
        self.data.put_u16(value.len() as u16);
        // encode value content
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty")
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
