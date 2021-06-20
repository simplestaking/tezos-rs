// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crate::actions::ContextAction;
use bytes::Buf;
use crypto::hash::BlockHash;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use thiserror::Error;

/// Possible errors for storage
#[derive(Debug, Error)]
pub enum ActionFileError {
    #[error("Blocks out of sequence")]
    BlocksOutOfSequence,
    #[error("IOError detected, reason: {error}")]
    IOError { error: std::io::Error },
    #[error("Serialization error, reason: {error}")]
    SerializeError { error: bincode::Error },
}

impl From<std::io::Error> for ActionFileError {
    fn from(error: std::io::Error) -> Self {
        ActionFileError::IOError { error }
    }
}

impl From<bincode::Error> for ActionFileError {
    fn from(error: bincode::Error) -> Self {
        ActionFileError::SerializeError { error }
    }
}

#[derive(Clone, Debug)]
pub struct ActionsFileHeader {
    pub current_block_hash: BlockHash,
    pub block_height: u32,
    pub actions_count: u32,
    pub block_count: u32,
}

pub struct ActionsFileReader {
    cursor: u64,
    reader: BufReader<File>,
}

impl ActionsFileReader {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, ActionFileError> {
        let file = OpenOptions::new()
            .write(false)
            .create(false)
            .read(true)
            .open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(0))?;
        Ok(ActionsFileReader { reader, cursor: 0 })
    }
}

impl Iterator for ActionsFileReader {
    type Item = Vec<ContextAction>;

    /// Return a tuple of a block and list action in the block
    fn next(&mut self) -> Option<Self::Item> {
        self.cursor = match self.reader.seek(SeekFrom::Start(self.cursor)) {
            Ok(c) => c,
            Err(_) => {
                return None;
            }
        };
        let mut h = [0_u8; 4];
        match self.reader.read_exact(&mut h) {
            Ok(_) => {}
            Err(_) => return None,
        }
        let content_len = u32::from_be_bytes(h);
        if content_len == 0 {
            return None;
        }
        let mut b = vec![0; content_len as usize];
        let _ = self.reader.read_exact(&mut b);

        let reader = snap::read::FrameDecoder::new(b.reader());

        let item = match bincode::deserialize_from::<_, Self::Item>(reader) {
            Ok(item) => item,
            Err(_) => {
                return None;
            }
        };
        self.cursor += h.len() as u64 + content_len as u64;
        Some(item)
    }
}

/// # ActionFileWriter
///
/// writes block and list actions to file in `path`
pub struct ActionsFileWriter {
    file: File,
}

impl ActionsFileWriter {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, ActionFileError> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(path)?;

        Ok(ActionsFileWriter { file })
    }
}

impl ActionsFileWriter {
    pub fn update(&mut self, actions: Vec<ContextAction>) -> Result<(), ActionFileError> {
        let mut out = Vec::new();
        let writer = snap::write::FrameEncoder::new(&mut out);
        bincode::serialize_into(writer, &actions)?;

        self._update(&out)?;
        Ok(())
    }

    pub fn _update(&mut self, data: &[u8]) -> Result<(), ActionFileError> {
        self.file.seek(SeekFrom::End(0))?;
        let chunk_size = (data.len() as u32).to_be_bytes();
        let mut dt = vec![];
        dt.extend_from_slice(&chunk_size);
        dt.extend_from_slice(data);
        self.file.write_all(dt.as_slice())?;
        Ok(())
    }
}
