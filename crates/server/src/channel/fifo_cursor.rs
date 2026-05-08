// SPDX-License-Identifier: BSD-3-Clause

//! FIFO head cursor sidecar. The cursor records the next sequence number to
//! be POPped from a FIFO channel and is fsynced to disk before each POP ACK
//! so a crash mid-POP cannot resurrect already-delivered entries.
//!
//! On-disk layout (`<channel_dir>/cursor.bin`):
//!
//! ```text
//! offset  size  field
//! ------  ----  -----
//! 0       8     next_seq      u64 little-endian
//! 8       4     crc32         CRC-32/IEEE over bytes [0..8)
//! 12      4     reserved      zero pad
//! ```
//!
//! Total file size: 16 bytes. Writes go through `atomic_write` (tmp →
//! fsync → rename → fsync parent) so a crash leaves either the previous
//! durable cursor or the new one, never a half-written file.

use std::path::PathBuf;

use crate::util::file::{DirSync, atomic_write};

/// Filename for the FIFO head cursor sidecar inside a channel directory.
pub const CURSOR_FILE: &str = "cursor.bin";
pub const CURSOR_TMP_FILE: &str = "cursor.bin.tmp";

const CURSOR_FILE_SIZE: usize = 16;

/// In-memory handle to a FIFO channel's head cursor on disk.
#[derive(Debug)]
pub struct FifoCursor {
  next_seq: u64,
  channel_dir: PathBuf,
}

impl FifoCursor {
  /// Returns the next sequence number to be POPped.
  #[allow(dead_code)] // used by PR 2 (POP path).
  pub fn next_seq(&self) -> u64 {
    self.next_seq
  }

  /// Atomically writes the initial cursor (called during pub/sub → FIFO
  /// transition). Fsyncs the file and the parent directory so the cursor is
  /// durable before the metadata save advertises the channel as FIFO.
  pub async fn write_initial(channel_dir: PathBuf, next_seq: u64) -> anyhow::Result<Self> {
    compio::fs::create_dir_all(&channel_dir).await?;
    let bytes = encode(next_seq);
    let final_path = channel_dir.join(CURSOR_FILE);
    let tmp_path = channel_dir.join(CURSOR_TMP_FILE);
    let (result, _) = atomic_write(&final_path, &tmp_path, bytes, DirSync::Strict).await;
    result?;
    Ok(Self { next_seq, channel_dir })
  }

  /// Loads the cursor from disk and cross-validates it against the log's
  /// `first_seq` / `last_seq`. The five validation rules implemented here
  /// match the spec at `docs/architecture/channels/fifo-channels.md`:
  ///
  /// 1. File exists and is the expected size.
  /// 2. CRC32 over the encoded `next_seq` matches.
  /// 3. `next_seq >= 1`.
  /// 4. `next_seq <= log.last_seq() + 1`.
  /// 5. If the queue is non-empty (`next_seq <= log.last_seq()`),
  ///    `log.first_seq() <= next_seq`.
  ///
  /// Any failure is returned as an error and the caller surfaces the channel
  /// as `NeedsRecovery` (FIFO data-plane ops then return
  /// `CursorRecoveryRequired`).
  pub async fn load(channel_dir: PathBuf, log_first_seq: u64, log_last_seq: u64) -> anyhow::Result<Self> {
    let path = channel_dir.join(CURSOR_FILE);
    let bytes = compio::fs::read(&path).await?;
    if bytes.len() != CURSOR_FILE_SIZE {
      return Err(anyhow::anyhow!("cursor.bin: unexpected size {}", bytes.len()));
    }
    let next_seq = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let computed = crc32(&bytes[0..8]);
    if stored_crc != computed {
      return Err(anyhow::anyhow!("cursor.bin: crc32 mismatch (stored={stored_crc:x}, computed={computed:x})"));
    }
    if next_seq < 1 {
      return Err(anyhow::anyhow!("cursor.bin: next_seq must be >= 1"));
    }
    if next_seq > log_last_seq.saturating_add(1) {
      return Err(anyhow::anyhow!("cursor.bin: next_seq {next_seq} > log.last_seq + 1 ({log_last_seq})"));
    }
    let queue_non_empty = next_seq <= log_last_seq;
    if queue_non_empty && log_first_seq > next_seq {
      return Err(anyhow::anyhow!("cursor.bin: log.first_seq {log_first_seq} > next_seq {next_seq} (queue non-empty)"));
    }

    Ok(Self { next_seq, channel_dir })
  }

  /// Atomically rewrites the cursor with `next_seq + 1`. On success the
  /// in-memory `next_seq` is advanced to match.
  ///
  /// Caller responsibility: only call after the corresponding entry has been
  /// fsynced to the message log (`log.flush()` if not already durable).
  #[allow(dead_code)] // used by PR 2 (POP path); kept for module symmetry.
  pub async fn advance(&mut self) -> anyhow::Result<()> {
    let new_next_seq = self.next_seq + 1;
    let bytes = encode(new_next_seq);
    let final_path = self.channel_dir.join(CURSOR_FILE);
    let tmp_path = self.channel_dir.join(CURSOR_TMP_FILE);
    let (result, _) = atomic_write(&final_path, &tmp_path, bytes, DirSync::Strict).await;
    result?;
    self.next_seq = new_next_seq;
    Ok(())
  }
}

fn encode(next_seq: u64) -> Vec<u8> {
  let mut buf = vec![0u8; CURSOR_FILE_SIZE];
  buf[0..8].copy_from_slice(&next_seq.to_le_bytes());
  let crc = crc32(&buf[0..8]);
  buf[8..12].copy_from_slice(&crc.to_le_bytes());
  buf
}

fn crc32(data: &[u8]) -> u32 {
  const POLY: u32 = 0xEDB88320;
  let mut crc: u32 = 0xFFFF_FFFF;
  for &b in data {
    crc ^= u32::from(b);
    for _ in 0..8 {
      crc = if crc & 1 != 0 { (crc >> 1) ^ POLY } else { crc >> 1 };
    }
  }
  !crc
}

#[cfg(test)]
mod tests {
  use super::*;

  #[compio::test]
  async fn write_initial_then_load_round_trips() {
    let tmp = tempfile::tempdir().unwrap();
    let cursor = FifoCursor::write_initial(tmp.path().to_path_buf(), 7).await.unwrap();
    assert_eq!(cursor.next_seq(), 7);

    let loaded = FifoCursor::load(tmp.path().to_path_buf(), 1, 7).await.unwrap();
    assert_eq!(loaded.next_seq(), 7);
  }

  #[compio::test]
  async fn advance_persists_new_seq() {
    let tmp = tempfile::tempdir().unwrap();
    let mut cursor = FifoCursor::write_initial(tmp.path().to_path_buf(), 1).await.unwrap();
    cursor.advance().await.unwrap();
    assert_eq!(cursor.next_seq(), 2);

    let loaded = FifoCursor::load(tmp.path().to_path_buf(), 1, 5).await.unwrap();
    assert_eq!(loaded.next_seq(), 2);
  }

  #[compio::test]
  async fn load_missing_file_errors() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(FifoCursor::load(tmp.path().to_path_buf(), 0, 0).await.is_err());
  }

  #[compio::test]
  async fn load_bad_crc_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let cursor_path = tmp.path().join(CURSOR_FILE);
    let mut bytes = encode(5);
    bytes[8] ^= 0xff;
    std::fs::write(&cursor_path, &bytes).unwrap();
    let err = FifoCursor::load(tmp.path().to_path_buf(), 1, 10).await.unwrap_err();
    assert!(err.to_string().contains("crc32"), "{err}");
  }

  #[compio::test]
  async fn load_zero_next_seq_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let cursor_path = tmp.path().join(CURSOR_FILE);
    std::fs::write(&cursor_path, encode(0)).unwrap();
    let err = FifoCursor::load(tmp.path().to_path_buf(), 0, 0).await.unwrap_err();
    assert!(err.to_string().contains(">= 1"), "{err}");
  }

  #[compio::test]
  async fn load_next_seq_beyond_log_last_seq_plus_one_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let cursor_path = tmp.path().join(CURSOR_FILE);
    std::fs::write(&cursor_path, encode(10)).unwrap();
    let err = FifoCursor::load(tmp.path().to_path_buf(), 1, 5).await.unwrap_err();
    assert!(err.to_string().contains("> log.last_seq + 1"), "{err}");
  }

  #[compio::test]
  async fn load_first_seq_above_next_seq_when_non_empty_errors() {
    let tmp = tempfile::tempdir().unwrap();
    let cursor_path = tmp.path().join(CURSOR_FILE);
    // next_seq=3, log range [5..=10] → queue non-empty (3 <= 10) and
    // first_seq=5 > next_seq=3, so the entry the cursor points at is
    // already evicted: this is corruption.
    std::fs::write(&cursor_path, encode(3)).unwrap();
    let err = FifoCursor::load(tmp.path().to_path_buf(), 5, 10).await.unwrap_err();
    assert!(err.to_string().contains("queue non-empty"), "{err}");
  }
}
