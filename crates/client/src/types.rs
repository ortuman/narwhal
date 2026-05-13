// SPDX-License-Identifier: BSD-3-Clause

//! Result types returned by the C2S client methods. These give callers
//! ergonomic, allocation-bounded handles on protocol responses without
//! exposing the raw `*Parameters` structs from `narwhal_protocol`.

use narwhal_util::pool::PoolBuffer;
use narwhal_util::string_atom::StringAtom;

/// A paginated list returned by `list_channels`, `list_members`, and
/// `get_channel_acl`. The three protocol acks (`CHANNELS_ACK`, `MEMBERS_ACK`,
/// `CHAN_ACL`) share an identical pagination shape, so the client surfaces
/// them as the same generic type.
///
/// `page`, `page_size`, and `total_count` are echoes of what the server sent;
/// they are `Some` when the server populated them and `None` otherwise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaginatedList<T> {
  pub items: Vec<T>,
  pub page: Option<u32>,
  pub page_size: Option<u32>,
  pub total_count: Option<u32>,
}

/// A single channel-configuration snapshot, returned by `get_channel_config`.
/// Strips the wire-level `id`/`channel` echoes from the protocol struct so
/// the caller sees only the actual configuration fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelConfiguration {
  pub max_clients: u32,
  pub max_payload_size: u32,
  pub max_persist_messages: u32,
  pub persist: bool,
  pub message_flush_interval: u32,
  /// Channel type: `"pubsub"` or `"fifo"`.
  pub r#type: StringAtom,
}

/// A single entry returned by `history`. The channel name and `history_id`
/// are redundant with the request and omitted here; the consumer already
/// knows what it asked for.
#[derive(Clone, Debug)]
pub struct HistoryEntry {
  pub from: StringAtom,
  pub seq: u64,
  pub timestamp: u64,
  pub payload: PoolBuffer,
}
