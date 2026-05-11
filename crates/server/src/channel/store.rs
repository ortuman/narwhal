// SPDX-License-Identifier: BSD-3-Clause

use std::rc::Rc;
use std::sync::Arc;

use async_trait::async_trait;
use narwhal_protocol::{Message, Nid};
use narwhal_util::pool::PoolBuffer;
use narwhal_util::string_atom::StringAtom;

use super::manager::{ChannelAcl, ChannelConfig};

/// Persisted channel metadata, restored on server startup.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct PersistedChannel {
  pub handler: StringAtom,
  pub owner: Option<Nid>,
  pub config: ChannelConfig,
  pub acl: ChannelAcl,
  #[serde(with = "rc_slice_serde")]
  pub members: Rc<[Nid]>,
  pub channel_type: ChannelType,
}

/// Server-side channel-kind marker, persisted in `metadata.bin` to recover
/// the correct channel mode on restart.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChannelType {
  /// Pub/sub channel: BROADCAST → all members, persistence is opt-in for
  /// HISTORY replay.
  #[default]
  PubSub,
  /// FIFO work-queue channel: PUSH (owner) → POP (any member, destructive).
  Fifo,
}

mod rc_slice_serde {
  use std::rc::Rc;

  use serde::{Deserialize, Deserializer, Serialize, Serializer};

  pub fn serialize<T: Serialize, S: Serializer>(data: &Rc<[T]>, serializer: S) -> Result<S::Ok, S::Error> {
    data.as_ref().serialize(serializer)
  }

  pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(deserializer: D) -> Result<Rc<[T]>, D::Error> {
    let v: Vec<T> = Vec::deserialize(deserializer)?;
    Ok(Rc::from(v))
  }
}

/// Storage backend for persisting channel metadata.
#[async_trait(?Send)]
pub trait ChannelStore: Clone + Send + Sync + 'static {
  /// Persists channel metadata.
  /// Called on channel creation (when persist=true) and on any metadata update.
  /// Returns the storage hash that identifies this channel on disk.
  async fn save_channel(&self, channel: &PersistedChannel) -> anyhow::Result<StringAtom>;

  /// Removes all persisted metadata for the given channel storage hash.
  /// Called when persist is toggled to false or the channel is deleted.
  async fn delete_channel(&self, hash: &StringAtom) -> anyhow::Result<()>;

  /// Returns the storage hashes of all persisted channels.
  async fn load_channel_hashes(&self) -> anyhow::Result<Arc<[StringAtom]>>;

  /// Loads persisted channel metadata by its storage hash.
  async fn load_channel(&self, hash: &StringAtom) -> anyhow::Result<PersistedChannel>;
}

/// Factory for creating per-channel message logs.
#[async_trait(?Send)]
pub trait MessageLogFactory: Clone + Send + Sync + 'static {
  /// The message log type produced by this factory.
  type Log: MessageLog;

  /// Creates a message log for the given channel handler in the requested
  /// mode.
  ///
  /// `mode` selects whether the log is opened in pub/sub mode (count-based
  /// tail eviction on append) or FIFO mode (head-driven eviction via
  /// `evict_below`, with the tail-segment-retention invariant). For
  /// startup-restore the mode is derived from the persisted `channel_type`;
  /// for the in-runtime pub/sub → FIFO transition the manager calls
  /// `switch_to_fifo_mode` on the live log instead.
  ///
  /// Implementations may perform fallible I/O (e.g. opening the channel
  /// directory, recovering on-disk state, memory-mapping index files). Errors
  /// are returned so the caller can refuse to bring the affected channel
  /// online; the rest of the server keeps running.
  async fn create(&self, handler: &StringAtom, mode: LogMode) -> anyhow::Result<Self::Log>;

  /// On-disk directory where the FIFO head-cursor sidecar (`cursor.bin`)
  /// for this channel lives. `None` means the backend is in-memory or
  /// otherwise has no on-disk channel directory; FIFO transitions and FIFO
  /// recovery require `Some`.
  fn channel_dir(&self, _handler: &StringAtom) -> Option<std::path::PathBuf> {
    None
  }
}

/// Mode hint passed to `MessageLogFactory::create`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LogMode {
  /// Pub/sub semantics: tail eviction on append once `max_persist_messages`
  /// is exceeded; no head cursor, no head-driven eviction.
  #[default]
  PubSub,
  /// FIFO semantics: tail eviction is disabled; eviction is driven by
  /// `evict_below` from the manager (called after the head cursor advances
  /// on POP).
  Fifo,
}

/// A single entry read from the message log.
pub struct LogEntry<'a> {
  pub seq: u64,
  pub timestamp: u64,
  pub from: &'a [u8],
  pub payload: &'a [u8],
}

/// Visitor callback for processing log entries during a read.
///
/// Called for each entry, borrowing data directly from the log's internal read
/// buffer. Implementations must not hold references to the entry data after
/// `visit` returns.
#[async_trait(?Send)]
pub trait LogVisitor {
  async fn visit(&mut self, entry: LogEntry<'_>) -> anyhow::Result<()>;
}

/// Append-only log for persisting broadcast messages for a single channel.
#[async_trait(?Send)]
pub trait MessageLog: 'static {
  /// Appends a message to the log, buffering it in memory without flushing to disk.
  /// If `max_messages` is greater than `0` and the number of stored messages exceeds it,
  /// the oldest entries should be evicted. A `max_messages` of `0` disables eviction
  /// entirely (the log grows unbounded).
  /// Call `flush` to persist buffered writes to durable storage.
  async fn append(&self, message: &Message, payload: &PoolBuffer, max_messages: u32) -> anyhow::Result<()>;

  /// Removes all persisted messages.
  async fn delete(&self) -> anyhow::Result<()>;

  /// Flushes any buffered writes to durable storage.
  async fn flush(&self) -> anyhow::Result<()>;

  /// Returns the first retained sequence number, or 0 if the log is empty.
  fn first_seq(&self) -> u64;

  /// Returns the highest sequence number stored in the log, or 0 if the log is empty.
  /// Used during restore to derive the correct starting seq for new broadcasts.
  fn last_seq(&self) -> u64;

  /// Reads entries starting at `from_seq`, up to `limit` entries, calling
  /// `visitor.visit()` for each entry. Returns the number of entries visited.
  ///
  /// Both the read and the visitor are async. Entry data passed to `visit()` is
  /// borrowed from the reader's buffer and is only valid for the duration of
  /// that awaited `visit()` call.
  async fn read(&self, from_seq: u64, limit: u32, visitor: &mut impl LogVisitor) -> anyhow::Result<u32>
  where
    Self: Sized;

  /// Highest sequence number that has been fsynced to disk, or 0 if no
  /// entry has been fsynced yet. Always `<= last_seq()`.
  ///
  /// FIFO POP uses this to skip a redundant `flush()` when the entry at the
  /// cursor is already durable.
  fn last_durable_seq(&self) -> u64;

  /// One-shot transition from pub/sub mode to FIFO mode. After this call:
  ///
  /// - `append` no longer tail-evicts based on `max_messages`.
  /// - `evict_below(seq)` becomes the only eviction path.
  /// - The tail-segment-retention invariant is enforced by `evict_below`.
  ///
  /// No-op if the log is already in FIFO mode.
  fn switch_to_fifo_mode(&self);

  /// Drops sealed segments whose `last_seq < seq`, except the segment that
  /// contains `last_seq()` (tail-segment-retention invariant: the active
  /// segment is never evicted, so there is always a place to append into).
  ///
  /// No-op in pub/sub mode and cheap when no segment qualifies.
  async fn evict_below(&self, seq: u64) -> anyhow::Result<()>;
}

/// A no-op message log that discards all writes and returns empty results.
pub struct NoopMessageLog;

// === impl NoopMessageLog ===

#[async_trait(?Send)]
impl MessageLog for NoopMessageLog {
  async fn append(&self, _message: &Message, _payload: &PoolBuffer, _max_messages: u32) -> anyhow::Result<()> {
    Ok(())
  }

  async fn delete(&self) -> anyhow::Result<()> {
    Ok(())
  }

  async fn flush(&self) -> anyhow::Result<()> {
    Ok(())
  }

  fn first_seq(&self) -> u64 {
    0
  }

  fn last_seq(&self) -> u64 {
    0
  }

  async fn read(&self, _from_seq: u64, _limit: u32, _visitor: &mut impl LogVisitor) -> anyhow::Result<u32>
  where
    Self: Sized,
  {
    Ok(0)
  }

  fn last_durable_seq(&self) -> u64 {
    0
  }

  fn switch_to_fifo_mode(&self) {}

  async fn evict_below(&self, _seq: u64) -> anyhow::Result<()> {
    Ok(())
  }
}

/// A no-op message log factory that produces `NoopMessageLog` instances.
#[derive(Clone)]
pub struct NoopMessageLogFactory;

// === impl NoopMessageLogFactory ===

#[async_trait(?Send)]
impl MessageLogFactory for NoopMessageLogFactory {
  type Log = NoopMessageLog;

  async fn create(&self, _handler: &StringAtom, _mode: LogMode) -> anyhow::Result<NoopMessageLog> {
    Ok(NoopMessageLog)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::channel::manager::{ChannelAcl, ChannelConfig};

  fn sample_channel(channel_type: ChannelType) -> PersistedChannel {
    PersistedChannel {
      handler: StringAtom::from("ch"),
      owner: None,
      config: ChannelConfig::default(),
      acl: ChannelAcl::default(),
      members: Rc::from([]),
      channel_type,
    }
  }

  #[test]
  fn channel_type_postcard_round_trip() {
    for ct in [ChannelType::PubSub, ChannelType::Fifo] {
      let bytes = postcard::to_allocvec(&ct).unwrap();
      let decoded: ChannelType = postcard::from_bytes(&bytes).unwrap();
      assert_eq!(decoded, ct);
    }
  }

  #[test]
  fn persisted_channel_postcard_round_trip() {
    for ct in [ChannelType::PubSub, ChannelType::Fifo] {
      let original = sample_channel(ct);
      let bytes = postcard::to_allocvec(&original).unwrap();
      let decoded: PersistedChannel = postcard::from_bytes(&bytes).unwrap();
      assert_eq!(decoded.channel_type, ct);
      assert_eq!(decoded.handler, original.handler);
    }
  }

  // The pre-FIFO PersistedChannel layout (without `channel_type`).
  #[derive(serde::Serialize)]
  struct LegacyPersistedChannel {
    handler: StringAtom,
    owner: Option<narwhal_protocol::Nid>,
    config: ChannelConfig,
    acl: ChannelAcl,
    members: Vec<narwhal_protocol::Nid>,
  }

  #[test]
  fn legacy_metadata_fails_to_decode() {
    let legacy = LegacyPersistedChannel {
      handler: StringAtom::from("ch"),
      owner: None,
      config: ChannelConfig::default(),
      acl: ChannelAcl::default(),
      members: Vec::new(),
    };
    let bytes = postcard::to_allocvec(&legacy).unwrap();
    let result: Result<PersistedChannel, _> = postcard::from_bytes(&bytes);
    assert!(result.is_err(), "legacy bytes must not deserialize into the new schema");
  }
}
