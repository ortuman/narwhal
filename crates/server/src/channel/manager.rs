// SPDX-License-Identifier: BSD-3-Clause

use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_channel::{Receiver, Sender};
use async_trait::async_trait;

use narwhal_common::core_dispatcher::CoreDispatcher;
use narwhal_protocol::ErrorReason::{
  BadRequest, ChannelIsFull, ChannelNotFound, CursorRecoveryRequired, Forbidden, InternalServerError, NotAllowed,
  NotImplemented, PersistenceNotEnabled, PolicyViolation, QueueEmpty, QueueFull, ResourceLimitReached, UserInChannel,
  UserNotInChannel, UserNotRegistered, WrongType,
};
use narwhal_protocol::{
  AclAction, AclType, BroadcastAckParameters, ChannelAclParameters, ChannelConfigurationParameters,
  ChannelLenParameters, ChannelSeqAckParameters, ClearAckParameters, DeleteChannelAckParameters, HistoryAckParameters,
  JoinChannelAckParameters, LeaveChannelAckParameters, ListChannelsAckParameters, ListMembersAckParameters, Message,
  MessageParameters, PopAckParameters, PushAckParameters, QoS, SetChannelAclAckParameters,
  SetChannelConfigurationAckParameters,
};
use narwhal_protocol::{ChannelId, Nid};
use narwhal_protocol::{Event, EventKind};
use narwhal_util::pool::{Pool, PoolBuffer};
use narwhal_util::string_atom::StringAtom;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use tracing::warn;

use crate::notifier::Notifier;
use crate::router::GlobalRouter;
use crate::transmitter::{Resource, Transmitter};

use super::fifo_cursor::FifoCursor;
use super::membership::Membership;
use super::store::{
  ChannelStore, ChannelType, LogEntry, LogMode, LogVisitor, MessageLog, MessageLogFactory, PersistedChannel,
};

const DEFAULT_MAILBOX_CAPACITY: usize = 16384;

const MAX_CHANNELS_PAGE_SIZE: u32 = 50;

const MAX_MEMBERS_PAGE_SIZE: u32 = 100;

/// Deterministically maps a channel handler to a shard index.
fn shard_for(handler: &StringAtom, shard_count: usize) -> usize {
  let mut hasher = DefaultHasher::new();
  handler.hash(&mut hasher);
  (hasher.finish() as usize) % shard_count
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ResultLabel {
  result: &'static str,
}

const SUCCESS: ResultLabel = ResultLabel { result: "success" };
const FAILURE: ResultLabel = ResultLabel { result: "failure" };

const FIFO_CHANNEL_NOT_FOUND: ResultLabel = ResultLabel { result: "channel_not_found" };
const FIFO_USER_NOT_IN_CHANNEL: ResultLabel = ResultLabel { result: "user_not_in_channel" };
const FIFO_FORBIDDEN: ResultLabel = ResultLabel { result: "forbidden" };
const FIFO_NOT_ALLOWED: ResultLabel = ResultLabel { result: "not_allowed" };
const FIFO_WRONG_TYPE: ResultLabel = ResultLabel { result: "wrong_type" };
const FIFO_QUEUE_EMPTY: ResultLabel = ResultLabel { result: "queue_empty" };
const FIFO_QUEUE_FULL: ResultLabel = ResultLabel { result: "queue_full" };
const FIFO_POLICY_VIOLATION: ResultLabel = ResultLabel { result: "policy_violation" };
const FIFO_CURSOR_RECOVERY: ResultLabel = ResultLabel { result: "cursor_recovery_required" };
const FIFO_NOT_IMPLEMENTED: ResultLabel = ResultLabel { result: "not_implemented" };

/// Metric handles for `ChannelManager`.
#[derive(Clone)]
struct ChannelManagerMetrics {
  channels_active: Gauge,
  channel_joins: Family<ResultLabel, Counter>,
  channel_leaves: Counter,
  channels_deleted: Counter,
  store_saves: Family<ResultLabel, Counter>,
  store_save_duration_seconds: Histogram,
  store_deletes: Family<ResultLabel, Counter>,
  store_loads: Family<ResultLabel, Counter>,
  store_load_duration_seconds: Histogram,
  message_log_flushes: Family<ResultLabel, Counter>,
  message_log_flush_duration_seconds: Histogram,
  message_log_reads: Family<ResultLabel, Counter>,
  message_log_read_duration_seconds: Histogram,
  message_log_entries_returned: Histogram,
  message_log_deletes: Family<ResultLabel, Counter>,
  fifo_pushes: Family<ResultLabel, Counter>,
  fifo_pops: Family<ResultLabel, Counter>,
  fifo_clears: Family<ResultLabel, Counter>,
  fifo_cursor_fsync_seconds: Histogram,
}

impl std::fmt::Debug for ChannelManagerMetrics {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ChannelManagerMetrics").finish_non_exhaustive()
  }
}

impl ChannelManagerMetrics {
  fn register(registry: &mut Registry) -> Self {
    let channels_active = Gauge::default();
    registry.register("channels_active", "Number of currently active channels", channels_active.clone());
    let channel_joins = Family::default();
    registry.register("channel_joins", "Channel join results", channel_joins.clone());
    let channel_leaves = Counter::default();
    registry.register("channel_leaves", "Total channel leave operations", channel_leaves.clone());
    let channels_deleted = Counter::default();
    registry.register("channels_deleted", "Total channels deleted", channels_deleted.clone());

    let store_saves = Family::default();
    registry.register("store_saves", "Channel store save operations", store_saves.clone());
    let store_save_duration_seconds =
      Histogram::new(prometheus_client::metrics::histogram::exponential_buckets(0.0001, 2.0, 16));
    registry.register(
      "store_save_duration_seconds",
      "Duration of channel store save operations in seconds",
      store_save_duration_seconds.clone(),
    );
    let store_deletes = Family::default();
    registry.register("store_deletes", "Channel store delete operations", store_deletes.clone());
    let store_loads = Family::default();
    registry.register("store_loads", "Channel store load operations", store_loads.clone());
    let store_load_duration_seconds =
      Histogram::new(prometheus_client::metrics::histogram::exponential_buckets(0.0001, 2.0, 16));
    registry.register(
      "store_load_duration_seconds",
      "Duration of channel store load operations in seconds",
      store_load_duration_seconds.clone(),
    );
    let message_log_flushes = Family::default();
    registry.register("message_log_flushes", "Message log flush operations", message_log_flushes.clone());
    let message_log_flush_duration_seconds =
      Histogram::new(prometheus_client::metrics::histogram::exponential_buckets(0.0001, 2.0, 16));
    registry.register(
      "message_log_flush_duration_seconds",
      "Duration of message log flush operations in seconds",
      message_log_flush_duration_seconds.clone(),
    );
    let message_log_reads = Family::default();
    registry.register("message_log_reads", "Message log read operations", message_log_reads.clone());
    let message_log_read_duration_seconds =
      Histogram::new(prometheus_client::metrics::histogram::exponential_buckets(0.0001, 2.0, 16));
    registry.register(
      "message_log_read_duration_seconds",
      "Duration of message log read operations in seconds",
      message_log_read_duration_seconds.clone(),
    );
    let message_log_entries_returned =
      Histogram::new(prometheus_client::metrics::histogram::exponential_buckets(1.0, 2.0, 11));
    registry.register(
      "message_log_entries_returned",
      "Number of entries returned by a message log read",
      message_log_entries_returned.clone(),
    );
    let message_log_deletes = Family::default();
    registry.register("message_log_deletes", "Message log delete operations", message_log_deletes.clone());

    let fifo_pushes = Family::default();
    registry.register("fifo_pushes", "FIFO PUSH outcomes", fifo_pushes.clone());
    let fifo_pops = Family::default();
    registry.register("fifo_pops", "FIFO POP outcomes", fifo_pops.clone());
    let fifo_clears = Family::default();
    registry.register("fifo_clears", "FIFO CLEAR outcomes", fifo_clears.clone());
    let fifo_cursor_fsync_seconds =
      Histogram::new(prometheus_client::metrics::histogram::exponential_buckets(0.0001, 2.0, 16));
    registry.register(
      "fifo_cursor_fsync_seconds",
      "Duration of FIFO head-cursor fsync operations in seconds",
      fifo_cursor_fsync_seconds.clone(),
    );

    Self {
      channels_active,
      channel_joins,
      channel_leaves,
      channels_deleted,
      store_saves,
      store_save_duration_seconds,
      store_deletes,
      store_loads,
      store_load_duration_seconds,
      message_log_flushes,
      message_log_flush_duration_seconds,
      message_log_reads,
      message_log_read_duration_seconds,
      message_log_entries_returned,
      message_log_deletes,
      fifo_pushes,
      fifo_pops,
      fifo_clears,
      fifo_cursor_fsync_seconds,
    }
  }

  /// Records the outcome of a message log flush: observes duration and increments the
  /// success/failure counter.
  fn record_flush(&self, start: Instant, result: &anyhow::Result<()>) {
    self.message_log_flush_duration_seconds.observe(start.elapsed().as_secs_f64());
    match result {
      Ok(()) => self.message_log_flushes.get_or_create(&SUCCESS).inc(),
      Err(_) => self.message_log_flushes.get_or_create(&FAILURE).inc(),
    };
  }

  /// Records the outcome of a message log read: observes duration, the number of entries
  /// returned on success, and increments the success/failure counter.
  fn record_read(&self, start: Instant, result: &anyhow::Result<u32>) {
    self.message_log_read_duration_seconds.observe(start.elapsed().as_secs_f64());
    match result {
      Ok(count) => {
        self.message_log_entries_returned.observe(*count as f64);
        self.message_log_reads.get_or_create(&SUCCESS).inc();
      },
      Err(_) => {
        self.message_log_reads.get_or_create(&FAILURE).inc();
      },
    };
  }

  /// Records the outcome of a message log delete.
  fn record_message_log_delete(&self, result: &anyhow::Result<()>) {
    match result {
      Ok(()) => self.message_log_deletes.get_or_create(&SUCCESS).inc(),
      Err(_) => self.message_log_deletes.get_or_create(&FAILURE).inc(),
    };
  }

  /// Records the outcome of a channel store load: observes duration and increments
  /// the success/failure counter.
  fn record_load<T>(&self, start: Instant, result: &anyhow::Result<T>) {
    self.store_load_duration_seconds.observe(start.elapsed().as_secs_f64());
    match result {
      Ok(_) => self.store_loads.get_or_create(&SUCCESS).inc(),
      Err(_) => self.store_loads.get_or_create(&FAILURE).inc(),
    };
  }
}

enum Command {
  JoinChannel {
    channel_id: ChannelId,
    nid: Nid,
    on_behalf_nid: Option<Nid>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<bool>>,
  },
  LeaveChannel {
    channel_id: ChannelId,
    nid: Nid,
    on_behalf_nid: Option<Nid>,
    transmitter: Option<Arc<dyn Transmitter>>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  LeaveChannels {
    nid: Nid,
    handlers: Vec<StringAtom>,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  DeleteChannel {
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  BroadcastPayload {
    payload: PoolBuffer,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    qos: Option<u8>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  GetChannelAcl {
    channel_id: ChannelId,
    nid: Nid,
    acl_type: AclType,
    page: Option<u32>,
    page_size: Option<u32>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  SetChannelAcl {
    channel_id: ChannelId,
    nid: Nid,
    nids: Vec<Nid>,
    acl_type: AclType,
    acl_action: AclAction,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  GetChannelConfiguration {
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  SetChannelConfiguration {
    config: ChannelConfig,
    requested_type: Option<ChannelType>,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  FilterOwnedChannels {
    nid: Nid,
    handlers: Vec<StringAtom>,
    reply_tx: Sender<Vec<StringAtom>>,
  },
  ListMembers {
    channel_id: ChannelId,
    nid: Nid,
    page: Option<u32>,
    count: Option<u32>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  History {
    channel_id: ChannelId,
    nid: Nid,
    history_id: StringAtom,
    from_seq: u64,
    limit: u32,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  ChannelSeq {
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  /// Periodic-flush request sent by a channel's flush task back into its own actor mailbox,
  /// so flushes serialize against the same actor that owns appends. Avoids racing on
  /// `FileMessageLog::inner` (which uses `RefCell` under the single-threaded actor invariant).
  FlushChannel {
    handler: StringAtom,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  Push {
    payload: PoolBuffer,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  Pop {
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  GetChannelLen {
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
  Clear {
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
    reply_tx: Sender<anyhow::Result<()>>,
  },
}

/// In-memory channel kind. Mirrors `store::ChannelType` but carries the
/// FIFO-side state that is *not* persisted (e.g. the head cursor handle).
pub(crate) enum ChannelKind {
  PubSub,
  // The `FifoState` payload is constructed by restore/transition and is
  // read by the PR 2 data plane (PUSH/POP/GET_CHAN_LEN); PR 1 only
  // discriminates the variant.
  Fifo(#[allow(dead_code)] FifoState),
}

/// FIFO runtime state. `Healthy` carries an open cursor; `NeedsRecovery` is
/// the explicit landing pad for FIFO data-plane operations after a failed
/// cursor recovery (PUSH/POP/GET_CHAN_LEN return `CursorRecoveryRequired`
/// while DELETE/JOIN/LEAVE-non-owner/etc still work).
pub(crate) enum FifoState {
  // Cursor is unused in PR 1; PR 2's POP path reads and advances it.
  Healthy(#[allow(dead_code)] super::fifo_cursor::FifoCursor),
  NeedsRecovery,
}

/// A channel.
struct Channel<ML: MessageLog> {
  handler: StringAtom,
  owner: Option<Nid>,
  config: ChannelConfig,
  acl: ChannelAcl,
  members: Rc<[Nid]>,
  allowed_targets: Rc<[Nid]>,
  notifier: Notifier,
  seq: u64,
  message_log: Rc<ML>,
  flush_cancel_tx: Option<Sender<()>>,
  /// The storage hash for this channel. `Some` for persistent channels, `None` for transient.
  store_hash: Option<StringAtom>,
  kind: ChannelKind,
}

// === impl Channel ===

impl<ML: MessageLog> Channel<ML> {
  fn new(handler: StringAtom, config: ChannelConfig, notifier: Notifier, message_log: ML) -> Self {
    Self {
      handler,
      owner: None,
      config,
      acl: ChannelAcl::default(),
      members: Rc::from([]),
      allowed_targets: Rc::from([]),
      notifier,
      seq: 1,
      message_log: Rc::new(message_log),
      flush_cancel_tx: None,
      store_hash: None,
      kind: ChannelKind::PubSub,
    }
  }

  fn is_empty(&self) -> bool {
    self.members.is_empty()
  }

  fn is_persistent(&self) -> bool {
    self.config.persist == Some(true)
  }

  fn is_owner(&self, nid: &Nid) -> bool {
    self.owner == Some(nid.clone())
  }

  fn is_member(&self, nid: &Nid) -> bool {
    self.members.binary_search(nid).is_ok()
  }

  fn member_count(&self) -> usize {
    self.members.len()
  }

  fn remove_member(&mut self, nid: &Nid) -> bool {
    if self.owner.as_ref() == Some(nid) {
      self.owner = None;
    }
    match self.members.binary_search(nid) {
      Ok(pos) => {
        let mut v: Vec<Nid> = self.members.iter().cloned().collect();
        v.remove(pos);
        self.members = Rc::from(v);
        self.update_allowed_targets();
        true
      },
      Err(_) => false,
    }
  }

  fn set_acl(&mut self, acl: Acl, acl_type: AclType) {
    match acl_type {
      AclType::Join => self.acl.join_acl = acl,
      AclType::Publish => self.acl.publish_acl = acl,
      AclType::Read => self.acl.read_acl = acl,
    }
    self.update_allowed_targets();
  }

  fn update_allowed_targets(&mut self) {
    let acl = &self.acl;
    if self.members.iter().all(|m| acl.is_read_allowed(m)) {
      self.allowed_targets = self.members.clone();
      return;
    }
    self.allowed_targets = self.members.iter().filter(|m| acl.is_read_allowed(m)).cloned().collect::<Vec<_>>().into();
  }

  async fn notify_member_joined(
    &self,
    nid: &Nid,
    excluding_resource: Option<Resource>,
    as_owner: bool,
    local_domain: StringAtom,
  ) -> anyhow::Result<()> {
    let channel_id = ChannelId::new_unchecked(self.handler.clone(), local_domain);
    let event =
      Event::new(EventKind::MemberJoined).with_channel(channel_id.into()).with_nid(nid.into()).with_owner(as_owner);
    self.notifier.notify(event, self.members.iter(), excluding_resource).await
  }

  async fn notify_member_left(
    &self,
    nid: &Nid,
    excluding_resource: Option<Resource>,
    as_owner: bool,
    local_domain: StringAtom,
  ) -> anyhow::Result<()> {
    let channel_id = ChannelId::new_unchecked(self.handler.clone(), local_domain);
    let event =
      Event::new(EventKind::MemberLeft).with_channel(channel_id.into()).with_nid(nid.into()).with_owner(as_owner);
    self.notifier.notify(event, self.members.iter(), excluding_resource).await
  }

  async fn notify_channel_deleted(
    &self,
    excluding_resource: Option<Resource>,
    local_domain: StringAtom,
  ) -> anyhow::Result<()> {
    let channel_id = ChannelId::new_unchecked(self.handler.clone(), local_domain);
    let event = Event::new(EventKind::ChannelDeleted).with_channel(channel_id.into());
    self.notifier.notify(event, self.members.iter(), excluding_resource).await
  }

  fn next_seq(&mut self) -> u64 {
    let seq = self.seq;
    self.seq += 1;
    seq
  }

  fn ensure_flush_task(&mut self, interval_ms: u32, mailbox_tx: Sender<Command>) {
    if self.flush_cancel_tx.is_some() {
      return;
    }
    let (cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);
    let handler = self.handler.clone();
    let interval = std::time::Duration::from_millis(interval_ms as u64);

    compio::runtime::spawn(async move {
      use futures::FutureExt;

      loop {
        futures::select! {
          _ = compio::runtime::time::sleep(interval).fuse() => {},
          _ = cancel_rx.recv().fuse() => break,
        }
        let (reply_tx, reply_rx) = async_channel::bounded::<anyhow::Result<()>>(1);
        if mailbox_tx.send(Command::FlushChannel { handler: handler.clone(), reply_tx }).await.is_err() {
          // Actor mailbox closed (shard shutdown); nothing more to do.
          break;
        }
        // Await the reply for natural backpressure: if the actor is slow draining its mailbox,
        // we shouldn't pile up more flush commands behind broadcasts. Outcome is already logged
        // and metricized in `flush_channel`; the receive error case is ignored on purpose.
        let _ = reply_rx.recv().await;
      }
    })
    .detach();

    self.flush_cancel_tx = Some(cancel_tx);
  }

  fn cancel_flush_task(&mut self) {
    if let Some(tx) = self.flush_cancel_tx.take() {
      // Best-effort send; if the receiver is already gone, that's fine.
      let _ = tx.try_send(());
    }
  }

  fn to_persisted(&self) -> PersistedChannel {
    PersistedChannel {
      handler: self.handler.clone(),
      owner: self.owner.clone(),
      config: self.config.clone(),
      acl: self.acl.clone(),
      members: self.members.clone(),
      channel_type: self.channel_type(),
    }
  }

  fn channel_type(&self) -> ChannelType {
    match &self.kind {
      ChannelKind::PubSub => ChannelType::PubSub,
      ChannelKind::Fifo(_) => ChannelType::Fifo,
    }
  }

  fn wire_type_str(&self) -> &'static str {
    match &self.kind {
      ChannelKind::PubSub => "pubsub",
      ChannelKind::Fifo(_) => "fifo",
    }
  }
}

struct ChannelShard<CS: ChannelStore, MLF: MessageLogFactory> {
  channels: HashMap<StringAtom, Channel<MLF::Log>>,
  #[allow(dead_code)]
  store: CS,
  message_log_factory: MLF,
  membership: Membership,
  mailbox: Receiver<Command>,
  /// Send-side handle to this shard's own mailbox, used to dispatch periodic-flush commands
  /// from per-channel flush tasks back into the actor.
  mailbox_tx: Sender<Command>,
  router: GlobalRouter,
  notifier: Notifier,
  local_domain: StringAtom,
  total_channels: Arc<AtomicUsize>,
  limits: ChannelManagerLimits,
  metrics: ChannelManagerMetrics,
  auth_enabled: bool,
  /// Dedicated pool for history replay payload buffers.
  history_pool: Pool,
}

// === impl ChannelShard ===

impl<CS: ChannelStore, MLF: MessageLogFactory> ChannelShard<CS, MLF> {
  async fn restore_and_run(mut self, hashes: Vec<StringAtom>) {
    self.restore(hashes).await;
    while let Ok(cmd) = self.mailbox.recv().await {
      self.handle(cmd).await;
    }

    // Shutdown: cancel all flush tasks and perform final flush on each channel.
    for (handler, channel) in self.channels.iter_mut() {
      channel.cancel_flush_task();
      if channel.config.persist == Some(true) {
        let start = Instant::now();
        let result = channel.message_log.flush().await;
        self.metrics.record_flush(start, &result);
        if let Err(e) = result {
          warn!(channel = %handler, error = %e, "final flush on shutdown failed");
        }
      }
    }
  }

  async fn restore(&mut self, hashes: Vec<StringAtom>) {
    for hash in &hashes {
      let start = Instant::now();
      let result = self.store.load_channel(hash).await;
      self.metrics.record_load(start, &result);
      let persisted = match result {
        Ok(p) => p,
        Err(e) => {
          warn!(hash = %hash, error = %e, "skipping channel restore: failed to load persisted channel");
          continue;
        },
      };

      let handler = persisted.handler.clone();
      let mode = match persisted.channel_type {
        ChannelType::PubSub => LogMode::PubSub,
        ChannelType::Fifo => LogMode::Fifo,
      };
      let message_log = match self.message_log_factory.create(&handler, mode).await {
        Ok(log) => log,
        Err(e) => {
          warn!(handler = %handler, error = %e, "skipping channel restore: failed to create message log");
          continue;
        },
      };

      let kind = match persisted.channel_type {
        ChannelType::PubSub => ChannelKind::PubSub,
        ChannelType::Fifo => {
          let log_first = message_log.first_seq();
          let log_last = message_log.last_seq();
          match self.message_log_factory.channel_dir(&handler) {
            Some(channel_dir) => match FifoCursor::load(channel_dir, log_first, log_last).await {
              Ok(cursor) => ChannelKind::Fifo(FifoState::Healthy(cursor)),
              Err(e) => {
                warn!(handler = %handler, error = %e, "FIFO cursor recovery failed; channel restored in NeedsRecovery state");
                ChannelKind::Fifo(FifoState::NeedsRecovery)
              },
            },
            None => {
              warn!(handler = %handler, "FIFO channel restore: factory has no on-disk channel_dir; restoring as NeedsRecovery");
              ChannelKind::Fifo(FifoState::NeedsRecovery)
            },
          }
        },
      };

      let mut channel = Channel::new(handler.clone(), persisted.config, self.notifier.clone(), message_log);
      channel.owner = persisted.owner;
      channel.acl = persisted.acl;
      channel.members = persisted.members;
      channel.store_hash = Some(hash.clone());
      channel.seq = channel.message_log.last_seq() + 1;
      channel.kind = kind;
      channel.update_allowed_targets();

      // Use u32::MAX to bypass the per-client limit: persisted membership is authoritative
      // and limit changes should not retroactively evict members from their channels.
      for member in channel.members.iter() {
        self.membership.reserve_slot(&member.username, &handler, u32::MAX).await;
      }

      self.channels.insert(handler, channel);
      self.total_channels.fetch_add(1, Ordering::SeqCst);
      self.metrics.channels_active.inc();
    }
  }

  async fn handle(&mut self, cmd: Command) {
    match cmd {
      Command::JoinChannel { channel_id, nid, on_behalf_nid, transmitter, correlation_id, reply_tx } => {
        let result = self.join_channel(channel_id, nid, on_behalf_nid, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::LeaveChannel { channel_id, nid, on_behalf_nid, transmitter, correlation_id, reply_tx } => {
        let result = self.leave_channel(channel_id, nid, on_behalf_nid, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::LeaveChannels { nid, handlers, reply_tx } => {
        let result = self.leave_channels(nid, handlers).await;
        let _ = reply_tx.send(result).await;
      },
      Command::DeleteChannel { channel_id, nid, transmitter, correlation_id, reply_tx } => {
        let result = self.delete_channel(channel_id, nid, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::BroadcastPayload { payload, channel_id, nid, transmitter, qos, correlation_id, reply_tx } => {
        let result = self.broadcast_payload(payload, channel_id, nid, transmitter, qos, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::GetChannelAcl { channel_id, nid, acl_type, page, page_size, transmitter, correlation_id, reply_tx } => {
        let result = self.get_channel_acl(channel_id, nid, acl_type, page, page_size, transmitter, correlation_id);
        let _ = reply_tx.send(result).await;
      },
      Command::SetChannelAcl { channel_id, nid, nids, acl_type, acl_action, transmitter, correlation_id, reply_tx } => {
        let result =
          self.set_channel_acl(channel_id, nid, nids, acl_type, acl_action, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::GetChannelConfiguration { channel_id, nid, transmitter, correlation_id, reply_tx } => {
        let result = self.get_channel_configuration(channel_id, nid, transmitter, correlation_id);
        let _ = reply_tx.send(result).await;
      },
      Command::SetChannelConfiguration {
        config,
        requested_type,
        channel_id,
        nid,
        transmitter,
        correlation_id,
        reply_tx,
      } => {
        let result =
          self.set_channel_configuration(config, requested_type, channel_id, nid, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::FilterOwnedChannels { nid, handlers, reply_tx } => {
        let result = self.filter_owned_channels(&nid, &handlers);
        let _ = reply_tx.send(result).await;
      },
      Command::ListMembers { channel_id, nid, page, count, transmitter, correlation_id, reply_tx } => {
        let result = self.list_members(channel_id, nid, page, count, transmitter, correlation_id);
        let _ = reply_tx.send(result).await;
      },
      Command::History { channel_id, nid, history_id, from_seq, limit, transmitter, correlation_id, reply_tx } => {
        let result = self.history(channel_id, nid, history_id, from_seq, limit, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::ChannelSeq { channel_id, nid, transmitter, correlation_id, reply_tx } => {
        let result = self.channel_seq(channel_id, nid, transmitter, correlation_id);
        let _ = reply_tx.send(result).await;
      },
      Command::FlushChannel { handler, reply_tx } => {
        let result = self.flush_channel(handler).await;
        let _ = reply_tx.send(result).await;
      },
      Command::Push { payload, channel_id, nid, transmitter, correlation_id, reply_tx } => {
        let result = self.push_payload(payload, channel_id, nid, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::Pop { channel_id, nid, transmitter, correlation_id, reply_tx } => {
        let result = self.pop_payload(channel_id, nid, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
      Command::GetChannelLen { channel_id, nid, transmitter, correlation_id, reply_tx } => {
        let result = self.get_channel_len(channel_id, nid, transmitter, correlation_id);
        let _ = reply_tx.send(result).await;
      },
      Command::Clear { channel_id, nid, transmitter, correlation_id, reply_tx } => {
        let result = self.clear(channel_id, nid, transmitter, correlation_id).await;
        let _ = reply_tx.send(result).await;
      },
    }
  }

  /// Flushes the message log of the given channel from within the actor's command loop, so
  /// the operation serializes against any in-flight append on the same shard. Called via
  /// `Command::FlushChannel` posted by the per-channel periodic flush task.
  async fn flush_channel(&mut self, handler: StringAtom) -> anyhow::Result<()> {
    let Some(channel) = self.channels.get(&handler) else {
      // Treat this stale flush tick as a no-op. The periodic flush task is stopped separately
      // via cancellation/config-change handling or when the shard mailbox closes.
      return Ok(());
    };
    let start = Instant::now();
    let result = channel.message_log.flush().await;
    self.metrics.record_flush(start, &result);
    if let Err(ref e) = result {
      warn!(channel = %handler, error = %e, "periodic message log flush failed");
    }
    result
  }

  async fn join_channel(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    on_behalf_nid: Option<Nid>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<bool> {
    if channel_id.domain != self.local_domain {
      self.metrics.channel_joins.get_or_create(&FAILURE).inc();
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }
    let handler = channel_id.handler.clone();
    let as_owner = !self.channels.contains_key(&handler);

    // Create the channel if it doesn't exist.
    if as_owner {
      if self.total_channels.load(Ordering::SeqCst) >= self.limits.max_channels as usize {
        self.metrics.channel_joins.get_or_create(&FAILURE).inc();
        return Err(
          narwhal_protocol::Error::new(ResourceLimitReached)
            .with_id(correlation_id)
            .with_detail("maximum channels reached")
            .into(),
        );
      }

      let config = ChannelConfig {
        max_clients: Some(self.limits.max_clients_per_channel),
        max_payload_size: Some(self.limits.max_payload_size),
        max_persist_messages: Some(self.limits.max_persist_messages),
        persist: Some(false),
        message_flush_interval: Some(0),
      };
      let message_log = match self.message_log_factory.create(&handler, LogMode::PubSub).await {
        Ok(log) => log,
        Err(e) => {
          warn!(handler = %handler, error = %e, "failed to create message log for new channel");
          self.metrics.channel_joins.get_or_create(&FAILURE).inc();
          return Err(narwhal_protocol::Error::new(InternalServerError).with_id(correlation_id).into());
        },
      };
      self.channels.insert(handler.clone(), Channel::new(handler.clone(), config, self.notifier.clone(), message_log));
      self.total_channels.fetch_add(1, Ordering::SeqCst);
    }

    let new_member_nid = match on_behalf_nid {
      Some(behalf_nid) => {
        let channel = self.channels.get(&handler).unwrap();
        if !channel.is_owner(&nid) {
          if as_owner {
            self.channels.remove(&handler);
            self.total_channels.fetch_sub(1, Ordering::SeqCst);
          }
          self.metrics.channel_joins.get_or_create(&FAILURE).inc();
          return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
        }
        if !self.router.c2s_router().has_connection(&behalf_nid.username).await {
          if as_owner {
            self.channels.remove(&handler);
            self.total_channels.fetch_sub(1, Ordering::SeqCst);
          }
          self.metrics.channel_joins.get_or_create(&FAILURE).inc();
          return Err(narwhal_protocol::Error::new(UserNotRegistered).with_id(correlation_id).into());
        }
        behalf_nid
      },
      None => nid.clone(),
    };

    let channel = self.channels.get(&handler).unwrap();

    // Validate before reserving a membership slot.
    if !channel.acl.is_join_allowed(&new_member_nid) {
      if as_owner {
        self.channels.remove(&handler);
        self.total_channels.fetch_sub(1, Ordering::SeqCst);
      }
      self.metrics.channel_joins.get_or_create(&FAILURE).inc();
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    if channel.is_member(&new_member_nid) {
      if as_owner {
        self.channels.remove(&handler);
        self.total_channels.fetch_sub(1, Ordering::SeqCst);
      }
      self.metrics.channel_joins.get_or_create(&FAILURE).inc();
      return Err(narwhal_protocol::Error::new(UserInChannel).with_id(correlation_id).into());
    }

    if channel.member_count() >= channel.config.max_clients.unwrap_or(0) as usize {
      if as_owner {
        self.channels.remove(&handler);
        self.total_channels.fetch_sub(1, Ordering::SeqCst);
      }
      self.metrics.channel_joins.get_or_create(&FAILURE).inc();
      return Err(narwhal_protocol::Error::new(ChannelIsFull).with_id(correlation_id).into());
    }

    // Reserve the membership slot before persisting or mutating in-memory state.
    if !self.membership.reserve_slot(&new_member_nid.username, &handler, self.limits.max_channels_per_client).await {
      if as_owner {
        self.channels.remove(&handler);
        self.total_channels.fetch_sub(1, Ordering::SeqCst);
      }
      self.metrics.channel_joins.get_or_create(&FAILURE).inc();
      return Err(
        narwhal_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("subscription limit reached")
          .into(),
      );
    }

    // Persist the projected membership before any in-memory changes.
    // Build new_members once and share the Rc between the persist and in-memory paths.
    let (new_members, store_hash) = {
      let channel = self.channels.get(&handler).unwrap();
      let pos = channel.members.partition_point(|m| m < &new_member_nid);
      let mut v: Vec<Nid> = channel.members.iter().cloned().collect();
      v.insert(pos, new_member_nid.clone());
      let new_members: Rc<[Nid]> = Rc::from(v);

      let store_hash = if channel.config.persist == Some(true) {
        let mut projected = channel.to_persisted();
        if projected.owner.is_none() {
          projected.owner = Some(new_member_nid.clone());
        }
        projected.members = new_members.clone();
        let start = Instant::now();
        match self.store.save_channel(&projected).await {
          Ok(hash) => {
            self.metrics.store_save_duration_seconds.observe(start.elapsed().as_secs_f64());
            self.metrics.store_saves.get_or_create(&SUCCESS).inc();
            Some(hash)
          },
          Err(e) => {
            self.metrics.store_save_duration_seconds.observe(start.elapsed().as_secs_f64());
            self.metrics.store_saves.get_or_create(&FAILURE).inc();
            self.membership.release_slot(&new_member_nid.username, &handler).await;
            if as_owner {
              self.channels.remove(&handler);
              self.total_channels.fetch_sub(1, Ordering::SeqCst);
            }
            self.metrics.channel_joins.get_or_create(&FAILURE).inc();
            return Err(e);
          },
        }
      } else {
        None
      };

      (new_members, store_hash)
    };

    let channel = self.channels.get_mut(&handler).unwrap();
    if let Some(hash) = store_hash {
      channel.store_hash = Some(hash);
    }
    if channel.owner.is_none() {
      channel.owner = Some(new_member_nid.clone());
    }
    channel.members = new_members;
    channel.update_allowed_targets();
    if let Err(e) = channel
      .notify_member_joined(&new_member_nid, Some(transmitter.resource()), as_owner, self.local_domain.clone())
      .await
    {
      warn!(channel = %handler, error = %e, "failed to notify member joined");
    }

    transmitter.send_message(Message::JoinChannelAck(JoinChannelAckParameters {
      id: correlation_id,
      channel: channel_id.into(),
    }));

    self.metrics.channel_joins.get_or_create(&SUCCESS).inc();
    if as_owner {
      self.metrics.channels_active.inc();
    }

    Ok(as_owner)
  }

  async fn leave_channel(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    on_behalf_nid: Option<Nid>,
    transmitter: Option<Arc<dyn Transmitter>>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    if !self.channels.contains_key(&channel_id.handler) {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    }

    let left_member_nid = match on_behalf_nid {
      Some(behalf_nid) => {
        let channel = self.channels.get(&channel_id.handler).unwrap();
        if !channel.is_owner(&nid) {
          return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
        }
        behalf_nid
      },
      None => nid.clone(),
    };

    {
      let channel = self.channels.get(&channel_id.handler).unwrap();
      if !channel.is_member(&left_member_nid) {
        return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
      }
    }

    let owner_leaving_fifo = {
      let channel = self.channels.get(&channel_id.handler).unwrap();
      matches!(&channel.kind, ChannelKind::Fifo(_)) && channel.is_owner(&left_member_nid)
    };
    if owner_leaving_fifo {
      let Some(tx) = transmitter else {
        return Err(narwhal_protocol::Error::new(InternalServerError).with_id(correlation_id).into());
      };
      self.do_delete_channel(&channel_id, tx.clone()).await?;
      tx.send_message(Message::LeaveChannelAck(LeaveChannelAckParameters { id: correlation_id }));
      self.metrics.channel_leaves.inc();
      return Ok(());
    }

    let excluding_resource = transmitter.as_ref().map(|t| t.resource());
    self.do_leave(&channel_id.handler.clone(), &left_member_nid, excluding_resource).await?;

    // Release the membership slot.
    self.membership.release_slot(&left_member_nid.username, &channel_id.handler).await;

    if let Some(tx) = transmitter {
      tx.send_message(Message::LeaveChannelAck(LeaveChannelAckParameters { id: correlation_id }));
    }

    self.metrics.channel_leaves.inc();

    Ok(())
  }

  async fn leave_channels(&mut self, nid: Nid, handlers: Vec<StringAtom>) -> anyhow::Result<()> {
    let mut removed = 0u64;

    for handler in &handlers {
      // Skip persistent channels when auth is enabled, user stays until explicit leave.
      let is_persistent = self.channels.get(handler).is_some_and(|c| c.config.persist == Some(true));
      if self.auth_enabled && is_persistent {
        continue;
      }

      match self.do_leave(handler, &nid, None).await {
        Ok(true) => {
          self.membership.release_slot(&nid.username, handler).await;
          removed += 1;
        },
        Ok(false) => {
          self.membership.release_slot(&nid.username, handler).await;
        },
        Err(e) => {
          warn!(channel = %handler, nid = %nid, error = %e, "failed to leave channel during batch disconnect");
        },
      }
    }

    self.metrics.channel_leaves.inc_by(removed);

    Ok(())
  }

  async fn delete_channel(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if !channel.is_owner(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    self.do_delete_channel(&channel_id, transmitter.clone()).await?;
    transmitter.send_message(Message::DeleteChannelAck(DeleteChannelAckParameters { id: correlation_id }));
    Ok(())
  }

  /// Runs the full delete-channel machinery: notify members, release
  /// membership slots, cancel flush task, final flush, drop persistent
  /// storage (including the FIFO cursor sidecar), and remove the
  /// in-memory channel. Does **not** send the request ack; callers do
  /// that based on which command initiated the delete (DELETE_ACK,
  /// LEAVE_ACK, etc.).
  async fn do_delete_channel(
    &mut self,
    channel_id: &ChannelId,
    transmitter: Arc<dyn Transmitter>,
  ) -> anyhow::Result<()> {
    {
      let channel = self.channels.get(&channel_id.handler).expect("caller verified channel exists");
      channel.notify_channel_deleted(Some(transmitter.resource()), self.local_domain.clone()).await?;
    }

    // Collect member usernames to release membership slots.
    let member_usernames: Vec<StringAtom> =
      self.channels.get(&channel_id.handler).unwrap().members.iter().map(|m| m.username.clone()).collect();

    for username in &member_usernames {
      self.membership.release_slot(username, &channel_id.handler).await;
    }

    // Cancel flush task and perform final flush before cleanup. The
    // FIFO cursor sidecar (if any) is wiped by `store.delete_channel`
    // along with everything else in the channel directory, so the
    // manager does not need a separate cursor.delete() step.
    let is_persistent = self.channels.get(&channel_id.handler).is_some_and(|c| c.config.persist == Some(true));
    if let Some(channel) = self.channels.get_mut(&channel_id.handler) {
      channel.cancel_flush_task();
      if is_persistent {
        let start = Instant::now();
        let result = channel.message_log.flush().await;
        self.metrics.record_flush(start, &result);
        if let Err(e) = result {
          warn!(channel = %channel_id.handler, error = %e, "final flush on delete failed");
        }
      }
    }

    // Clean up persistent storage. Best effort, must not block channel removal or leak slots.
    if is_persistent {
      self.delete_persistent_storage(&channel_id.handler).await;
    }

    self.channels.remove(&channel_id.handler);

    self.total_channels.fetch_sub(1, Ordering::SeqCst);
    self.metrics.channels_deleted.inc();
    self.metrics.channels_active.dec();

    Ok(())
  }

  async fn broadcast_payload(
    &mut self,
    payload: PoolBuffer,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    qos: Option<u8>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get_mut(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if matches!(&channel.kind, ChannelKind::Fifo(_)) {
      return Err(narwhal_protocol::Error::new(WrongType).with_id(correlation_id).into());
    }

    if !channel.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    if !channel.acl.is_publish_allowed(&nid) {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    let max_payload_size = channel.config.max_payload_size.unwrap_or(0);
    let is_persistent = channel.config.persist == Some(true);
    // Defensive backstop: a persistent channel restored from a pre-validation
    // store could carry max_persist_messages=0; coerce to the server limit so
    // the eviction loophole stays closed for legacy on-disk state.
    let max_persist_messages = match channel.config.max_persist_messages.unwrap_or(0) {
      0 => self.limits.max_persist_messages,
      v => v,
    };
    let allowed_targets = channel.allowed_targets.clone();
    let seq = channel.next_seq();
    let payload_length = payload.as_slice().len() as u32;

    if payload_length > max_payload_size {
      return Err(
        narwhal_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("payload size exceeds channel limit")
          .into(),
      );
    }
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    let qos = qos.map(QoS::try_from).transpose()?.unwrap_or(QoS::default());

    if qos == QoS::AckOnReceived {
      transmitter.send_message(Message::BroadcastAck(BroadcastAckParameters { id: correlation_id, seq }));
    }

    let msg = Message::Message(MessageParameters {
      from: (&nid).into(),
      channel: (&channel_id).into(),
      length: payload_length,
      seq,
      timestamp,
      history_id: None,
    });

    if is_persistent {
      channel.message_log.append(&msg, &payload, max_persist_messages).await?;

      let flush_interval = channel.config.message_flush_interval.unwrap_or(0);
      if flush_interval == 0 {
        let start = Instant::now();
        let result = channel.message_log.flush().await;
        self.metrics.record_flush(start, &result);
        result?;
      } else {
        channel.ensure_flush_task(flush_interval, self.mailbox_tx.clone());
      }
    }

    self.router.route_to_many(msg, Some(payload), allowed_targets.iter(), Some(transmitter.resource())).await?;

    if qos == QoS::AckOnDelivered {
      transmitter.send_message(Message::BroadcastAck(BroadcastAckParameters { id: correlation_id, seq }));
    }

    Ok(())
  }

  #[allow(clippy::too_many_arguments)]
  fn get_channel_acl(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    acl_type: AclType,
    page: Option<u32>,
    page_size: Option<u32>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if !channel.is_owner(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    let acl = match acl_type {
      AclType::Join => channel.acl.join_acl.clone(),
      AclType::Publish => channel.acl.publish_acl.clone(),
      AclType::Read => channel.acl.read_acl.clone(),
    };

    let all_nids: Vec<StringAtom> = acl.allow_list().into_iter().map(|z| z.into()).collect();
    let total_count = all_nids.len() as u32;

    let (nids, response_page, response_page_size, response_total_count) =
      if let (Some(page), Some(page_size)) = (page, page_size) {
        let start = ((page - 1) * page_size) as usize;
        let end = (start + page_size as usize).min(all_nids.len());
        let paginated_nids = if start < all_nids.len() { all_nids[start..end].to_vec() } else { Vec::new() };
        (paginated_nids, Some(page), Some(page_size), Some(total_count))
      } else {
        (all_nids, None, None, None)
      };

    transmitter.send_message(Message::ChannelAcl(ChannelAclParameters {
      id: correlation_id,
      channel: channel_id.into(),
      r#type: acl_type.as_str().into(),
      nids,
      page: response_page,
      page_size: response_page_size,
      total_count: response_total_count,
    }));

    Ok(())
  }

  #[allow(clippy::too_many_arguments)]
  async fn set_channel_acl(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    nids: Vec<Nid>,
    acl_type: AclType,
    acl_action: AclAction,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get_mut(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if !channel.is_owner(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    let mut new_acl = match acl_type {
      AclType::Join => channel.acl.join_acl.clone(),
      AclType::Publish => channel.acl.publish_acl.clone(),
      AclType::Read => channel.acl.read_acl.clone(),
    };
    new_acl.update(nids, acl_action);

    if new_acl.total_entries() > channel.config.max_clients.unwrap_or(0) as usize {
      return Err(
        narwhal_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("ACL allow list exceeds max entries")
          .into(),
      );
    }

    if channel.config.persist == Some(true) {
      let mut projected = channel.to_persisted();
      match acl_type {
        AclType::Join => projected.acl.join_acl = new_acl.clone(),
        AclType::Publish => projected.acl.publish_acl = new_acl.clone(),
        AclType::Read => projected.acl.read_acl = new_acl.clone(),
      }
      let start = Instant::now();
      let result = self.store.save_channel(&projected).await;
      self.metrics.store_save_duration_seconds.observe(start.elapsed().as_secs_f64());
      match result {
        Ok(hash) => {
          self.metrics.store_saves.get_or_create(&SUCCESS).inc();
          channel.store_hash = Some(hash);
        },
        Err(e) => {
          self.metrics.store_saves.get_or_create(&FAILURE).inc();
          return Err(e);
        },
      }
    }

    channel.set_acl(new_acl, acl_type);

    transmitter.send_message(Message::SetChannelAclAck(SetChannelAclAckParameters { id: correlation_id }));

    Ok(())
  }

  fn get_channel_configuration(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let Some(channel) = self.channels.get(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if !channel.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    let config = channel.config.clone();

    transmitter.send_message(Message::ChannelConfiguration(ChannelConfigurationParameters {
      id: correlation_id,
      channel: channel_id.into(),
      max_clients: config.max_clients.unwrap_or(0),
      max_payload_size: config.max_payload_size.unwrap_or(0),
      max_persist_messages: config.max_persist_messages.unwrap_or(0),
      persist: config.persist.unwrap_or(false),
      message_flush_interval: config.message_flush_interval.unwrap_or(0),
      r#type: StringAtom::from(channel.wire_type_str()),
    }));

    Ok(())
  }

  #[allow(clippy::too_many_arguments)]
  async fn set_channel_configuration(
    &mut self,
    config: ChannelConfig,
    requested_type: Option<ChannelType>,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    if let Some(v) = config.max_clients
      && v > self.limits.max_clients_per_channel
    {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("max_clients exceeds server established limit")
          .into(),
      );
    }

    if let Some(v) = config.max_payload_size
      && v > self.limits.max_payload_size
    {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("max_payload_size exceeds server established limit")
          .into(),
      );
    }

    if let Some(v) = config.max_persist_messages
      && v > self.limits.max_persist_messages
    {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("max_persist_messages exceeds server established limit")
          .into(),
      );
    }

    if let Some(v) = config.message_flush_interval
      && v > self.limits.max_message_flush_interval
    {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("message_flush_interval exceeds server established limit")
          .into(),
      );
    }
    let Some(channel) = self.channels.get_mut(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if !channel.is_owner(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    let current_type = channel.channel_type();
    // FIFO is one-way: pub/sub → fifo is allowed, fifo → pub/sub is not.
    if let Some(req) = requested_type
      && current_type == ChannelType::Fifo
      && req == ChannelType::PubSub
    {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("FIFO is one-way; cannot revert to pubsub")
          .into(),
      );
    }
    let new_type = requested_type.unwrap_or(current_type);
    let transitioning_to_fifo = current_type == ChannelType::PubSub && new_type == ChannelType::Fifo;

    let was_persistent = channel.config.persist == Some(true);
    let flush_interval_changed =
      config.message_flush_interval.is_some() && config.message_flush_interval != channel.config.message_flush_interval;
    let new_config = channel.config.merge(&config);
    let is_persistent = new_config.persist == Some(true);

    if is_persistent && new_config.max_persist_messages.unwrap_or(0) == 0 {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("max_persist_messages must be greater than 0 for persistent channels")
          .into(),
      );
    }

    // FIFO requires durable storage with a non-zero retention.
    if new_type == ChannelType::Fifo && (!is_persistent || new_config.max_persist_messages.unwrap_or(0) == 0) {
      return Err(
        narwhal_protocol::Error::new(BadRequest)
          .with_id(correlation_id)
          .with_detail("FIFO requires persist=true and max_persist_messages > 0")
          .into(),
      );
    }

    let config_unchanged = new_config_equals(&channel.config, &new_config);
    let no_op = config_unchanged && !transitioning_to_fifo;

    if transitioning_to_fifo {
      // Step 1: flush so log.last_seq() is durable.
      let start = Instant::now();
      let flush_result = channel.message_log.flush().await;
      self.metrics.record_flush(start, &flush_result);
      flush_result?;

      // Step 2: derive the new starting seq.
      let new_next_seq = channel.message_log.last_seq() + 1;

      // Step 3: atomic-write cursor.bin (durable) before publishing the
      // FIFO type via metadata.bin.
      let channel_dir = match self.message_log_factory.channel_dir(&channel_id.handler) {
        Some(dir) => dir,
        None => {
          return Err(
            narwhal_protocol::Error::new(InternalServerError)
              .with_id(correlation_id)
              .with_detail("FIFO transition not supported by this storage backend")
              .into(),
          );
        },
      };
      let cursor = match FifoCursor::write_initial(channel_dir, new_next_seq).await {
        Ok(cursor) => cursor,
        Err(e) => {
          warn!(channel = %channel_id.handler, error = %e, "FIFO cursor write failed; transition aborted");
          return Err(narwhal_protocol::Error::new(InternalServerError).with_id(correlation_id).into());
        },
      };

      // Step 4: persist metadata with channel_type=Fifo. After this point
      // the transition is durable on disk.
      let mut projected = channel.to_persisted();
      projected.config = new_config.clone();
      projected.channel_type = ChannelType::Fifo;
      let store_start = Instant::now();
      let save_result = self.store.save_channel(&projected).await;
      self.metrics.store_save_duration_seconds.observe(store_start.elapsed().as_secs_f64());
      match save_result {
        Ok(hash) => {
          self.metrics.store_saves.get_or_create(&SUCCESS).inc();
          channel.store_hash = Some(hash);
        },
        Err(e) => {
          self.metrics.store_saves.get_or_create(&FAILURE).inc();
          // Cursor is stranded on disk. `load_channel_hashes` cleans up
          // directories whose metadata.bin is absent on next restart, and
          // the next successful retry of this same transition will
          // overwrite it. No in-memory state changed yet, so leave the
          // channel as pub/sub and surface the error.
          return Err(e);
        },
      }

      // Step 5: in-memory mutations (infallible from here).
      channel.message_log.switch_to_fifo_mode();
      channel.seq = new_next_seq;
      channel.kind = ChannelKind::Fifo(FifoState::Healthy(cursor));
      channel.config = new_config.clone();

      // Reconcile the periodic flush task against the new interval. FIFO is
      // always persistent so the persistence-toggle branch is a no-op here,
      // but a `flush_interval_changed` request still needs the cancel +
      // optional restart logic that the non-transition path runs below.
      Self::reconcile_flush_task_after_config_change(
        channel,
        &channel_id.handler,
        false,
        flush_interval_changed,
        true,
        &self.metrics,
        self.mailbox_tx.clone(),
      )
      .await;

      // Steps 6/7: emit reconfigured event + ack.
      Self::emit_channel_reconfigured(channel, &channel_id, transmitter.clone(), self.local_domain.clone()).await;
      transmitter
        .send_message(Message::SetChannelConfigurationAck(SetChannelConfigurationAckParameters { id: correlation_id }));
      return Ok(());
    }

    // Non-transition path (unchanged from pre-FIFO behavior, plus event
    // emission on any successful change).
    if is_persistent {
      let mut projected = channel.to_persisted();
      projected.config = new_config.clone();
      let start = Instant::now();
      let result = self.store.save_channel(&projected).await;
      self.metrics.store_save_duration_seconds.observe(start.elapsed().as_secs_f64());
      match result {
        Ok(hash) => {
          self.metrics.store_saves.get_or_create(&SUCCESS).inc();
          channel.store_hash = Some(hash);
        },
        Err(e) => {
          self.metrics.store_saves.get_or_create(&FAILURE).inc();
          return Err(e);
        },
      }
    }

    channel.config = new_config;

    Self::reconcile_flush_task_after_config_change(
      channel,
      &channel_id.handler,
      !is_persistent && was_persistent,
      flush_interval_changed,
      is_persistent,
      &self.metrics,
      self.mailbox_tx.clone(),
    )
    .await;

    if !is_persistent && was_persistent {
      self.delete_persistent_storage(&channel_id.handler).await;
      let channel = self.channels.get_mut(&channel_id.handler).unwrap();
      channel.store_hash = None;
    }

    if !no_op {
      let channel = self.channels.get(&channel_id.handler).unwrap();
      Self::emit_channel_reconfigured(channel, &channel_id, transmitter.clone(), self.local_domain.clone()).await;
    }

    transmitter
      .send_message(Message::SetChannelConfigurationAck(SetChannelConfigurationAckParameters { id: correlation_id }));

    Ok(())
  }

  /// Reconciles the per-channel periodic flush task with the post-change
  /// config. Used by both the FIFO transition and non-transition branches of
  /// `set_channel_configuration` so that toggling persistence or changing
  /// `message_flush_interval` always cancels the running task and restarts
  /// a new one when the new interval is non-zero and the channel is still
  /// persistent.
  ///
  /// `channel.config` must already reflect the new configuration when this
  /// is called.
  async fn reconcile_flush_task_after_config_change(
    channel: &mut Channel<MLF::Log>,
    channel_handler: &StringAtom,
    persistence_disabled: bool,
    flush_interval_changed: bool,
    is_persistent: bool,
    metrics: &ChannelManagerMetrics,
    mailbox_tx: Sender<Command>,
  ) {
    if !(persistence_disabled || flush_interval_changed) {
      return;
    }
    channel.cancel_flush_task();

    // When the flush interval changed but persistence is still enabled, flush any buffered
    // messages and restart the periodic task immediately so they are not left pending
    // indefinitely waiting for the next broadcast.
    if flush_interval_changed && is_persistent {
      let start = Instant::now();
      let result = channel.message_log.flush().await;
      metrics.record_flush(start, &result);
      if let Err(e) = result {
        warn!(channel = %channel_handler, error = %e, "flush on interval change failed");
      }
      if let Some(interval) = channel.config.message_flush_interval
        && interval > 0
      {
        channel.ensure_flush_task(interval, mailbox_tx);
      }
    }
  }

  /// Notifies all members of a successful CHAN_CONFIG change. The event
  /// excludes the caller's resource (the issuing client receives the ACK,
  /// not the event).
  async fn emit_channel_reconfigured(
    channel: &Channel<MLF::Log>,
    channel_id: &ChannelId,
    transmitter: Arc<dyn Transmitter>,
    local_domain: StringAtom,
  ) {
    let event_channel_id = ChannelId::new_unchecked(channel_id.handler.clone(), local_domain);
    let event = Event::new(EventKind::ChannelReconfigured).with_channel(event_channel_id.into());
    if let Err(e) = channel.notifier.notify(event, channel.members.iter(), Some(transmitter.resource())).await {
      warn!(channel = %channel_id.handler, error = %e, "failed to notify CHANNEL_RECONFIGURED");
    }
  }

  fn filter_owned_channels(&self, nid: &Nid, handlers: &[StringAtom]) -> Vec<StringAtom> {
    handlers
      .iter()
      .filter_map(|handler| {
        self.channels.get(handler).and_then(|channel| {
          if channel.is_owner(nid) {
            Some(ChannelId::new_unchecked(handler.clone(), self.local_domain.clone()).into())
          } else {
            None
          }
        })
      })
      .collect()
  }

  fn list_members(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    page: Option<u32>,
    count: Option<u32>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if !channel.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }

    let member_list: Vec<StringAtom> = channel.members.iter().map(|m| m.into()).collect();

    let page = page.unwrap_or(1);
    let page_size = count.unwrap_or(20).min(MAX_MEMBERS_PAGE_SIZE);
    let start = ((page - 1) * page_size) as usize;
    let end = (page * page_size) as usize;

    let paginated_members =
      if start < member_list.len() { member_list[start..end.min(member_list.len())].to_vec() } else { Vec::new() };
    let include_pagination_info = paginated_members.len() < member_list.len();
    let (page, page_size, total_count) = if include_pagination_info {
      (Some(page), Some(page_size), Some(member_list.len() as u32))
    } else {
      (None, None, None)
    };

    transmitter.send_message(Message::ListMembersAck(ListMembersAckParameters {
      id: correlation_id,
      channel: channel_id.into(),
      members: paginated_members,
      page,
      page_size,
      total_count,
    }));

    Ok(())
  }

  #[allow(clippy::too_many_arguments)]
  async fn history(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    history_id: StringAtom,
    from_seq: u64,
    limit: u32,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    let limit = limit.min(self.limits.max_history_limit);
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if matches!(&channel.kind, ChannelKind::Fifo(_)) {
      return Err(narwhal_protocol::Error::new(WrongType).with_id(correlation_id).into());
    }

    if !channel.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }

    if !channel.acl.is_read_allowed(&nid) {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    if !channel.is_persistent() {
      return Err(narwhal_protocol::Error::new(PersistenceNotEnabled).with_id(correlation_id).into());
    }

    let channel_atom: StringAtom = (&channel_id).into();
    let mut visitor = HistoryVisitor {
      transmitter: &transmitter,
      channel: &channel_atom,
      history_id: &history_id,
      pool: &self.history_pool,
    };

    let start = Instant::now();
    let result = channel.message_log.read(from_seq, limit, &mut visitor).await;
    self.metrics.record_read(start, &result);
    let count = result?;

    transmitter.send_message(Message::HistoryAck(HistoryAckParameters {
      id: correlation_id,
      history_id,
      channel: channel_atom,
      count,
    }));

    Ok(())
  }

  fn channel_seq(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if matches!(&channel.kind, ChannelKind::Fifo(_)) {
      return Err(narwhal_protocol::Error::new(WrongType).with_id(correlation_id).into());
    }

    if !channel.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }

    if !channel.acl.is_read_allowed(&nid) {
      return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
    }

    if !channel.is_persistent() {
      return Err(narwhal_protocol::Error::new(PersistenceNotEnabled).with_id(correlation_id).into());
    }

    transmitter.send_message(Message::ChannelSeqAck(ChannelSeqAckParameters {
      id: correlation_id,
      channel: channel_id.into(),
      first_seq: channel.message_log.first_seq(),
      last_seq: channel.message_log.last_seq(),
    }));

    Ok(())
  }

  /// PUSH handler for FIFO channels. Owner-only append. Returns:
  /// `CHANNEL_NOT_FOUND`, `USER_NOT_IN_CHANNEL`, `FORBIDDEN`, `WRONG_TYPE`,
  /// `CURSOR_RECOVERY_REQUIRED`, `POLICY_VIOLATION`, or `QUEUE_FULL`.
  async fn push_payload(
    &mut self,
    payload: PoolBuffer,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      self.metrics.fifo_pushes.get_or_create(&FIFO_NOT_IMPLEMENTED).inc();
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get_mut(&channel_id.handler) else {
      self.metrics.fifo_pushes.get_or_create(&FIFO_CHANNEL_NOT_FOUND).inc();
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    match &channel.kind {
      ChannelKind::PubSub => {
        self.metrics.fifo_pushes.get_or_create(&FIFO_WRONG_TYPE).inc();
        return Err(narwhal_protocol::Error::new(WrongType).with_id(correlation_id).into());
      },
      ChannelKind::Fifo(FifoState::NeedsRecovery) => {
        self.metrics.fifo_pushes.get_or_create(&FIFO_CURSOR_RECOVERY).inc();
        return Err(narwhal_protocol::Error::new(CursorRecoveryRequired).with_id(correlation_id).into());
      },
      ChannelKind::Fifo(FifoState::Healthy(_)) => {},
    }

    if !channel.is_member(&nid) {
      self.metrics.fifo_pushes.get_or_create(&FIFO_USER_NOT_IN_CHANNEL).inc();
      return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }

    if !channel.is_owner(&nid) {
      self.metrics.fifo_pushes.get_or_create(&FIFO_FORBIDDEN).inc();
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    let max_payload_size = channel.config.max_payload_size.unwrap_or(0);
    // FIFO transition validation guarantees `max_persist_messages > 0`. Coerce
    // 0 to the server limit defensively (matches the broadcast path).
    let max_persist_messages = match channel.config.max_persist_messages.unwrap_or(0) {
      0 => self.limits.max_persist_messages,
      v => v,
    };
    let payload_length = payload.as_slice().len() as u32;

    if payload_length > max_payload_size {
      self.metrics.fifo_pushes.get_or_create(&FIFO_POLICY_VIOLATION).inc();
      return Err(
        narwhal_protocol::Error::new(PolicyViolation)
          .with_id(correlation_id)
          .with_detail("payload size exceeds channel limit")
          .into(),
      );
    }

    // Logical queue depth using the healthy cursor and the log's last_seq.
    let cursor_next_seq = match &channel.kind {
      ChannelKind::Fifo(FifoState::Healthy(c)) => c.next_seq(),
      _ => unreachable!("kind already validated as Fifo(Healthy)"),
    };
    let log_last_seq = channel.message_log.last_seq();
    let depth = if cursor_next_seq > log_last_seq { 0 } else { log_last_seq - cursor_next_seq + 1 };
    if depth >= max_persist_messages as u64 {
      self.metrics.fifo_pushes.get_or_create(&FIFO_QUEUE_FULL).inc();
      return Err(narwhal_protocol::Error::new(QueueFull).with_id(correlation_id).into());
    }

    // Peek the next seq WITHOUT bumping the counter. If append fails the
    // counter must stay put: otherwise the next successful PUSH leaves a gap
    // in the log between the previous tail and the new tail, and POP would
    // read past the gap and double-deliver the later entry (cursor advances
    // by one, but `MessageLog::read(from_seq, limit, visitor)` returns the
    // first entry with `seq >= from_seq`, not strictly at `from_seq`).
    let seq = channel.seq;
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    let msg = Message::Message(MessageParameters {
      from: (&nid).into(),
      channel: (&channel_id).into(),
      length: payload_length,
      seq,
      timestamp,
      history_id: None,
    });

    // FIFO mode disables tail-eviction inside the log; pass 0 to make the
    // intent explicit (no `max_messages`-driven eviction on PUSH).
    channel.message_log.append(&msg, &payload, 0).await?;
    // Append committed: claim the seq by bumping the channel counter.
    channel.seq = seq + 1;

    let flush_interval = channel.config.message_flush_interval.unwrap_or(0);
    if flush_interval == 0 {
      let start = Instant::now();
      let result = channel.message_log.flush().await;
      self.metrics.record_flush(start, &result);
      result?;
    } else {
      channel.ensure_flush_task(flush_interval, self.mailbox_tx.clone());
    }

    transmitter.send_message(Message::PushAck(PushAckParameters { id: correlation_id }));
    self.metrics.fifo_pushes.get_or_create(&SUCCESS).inc();
    Ok(())
  }

  /// POP handler for FIFO channels. Reads the entry at the head cursor, forces
  /// a synchronous log flush when the entry is not yet durable, advances and
  /// fsyncs the cursor sidecar, and then sends `POP_ACK` with the entry's
  /// PUSH timestamp. A crash between cursor fsync and `POP_ACK` socket write
  /// loses the entry consumer-side (no duplication).
  async fn pop_payload(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      self.metrics.fifo_pops.get_or_create(&FIFO_NOT_IMPLEMENTED).inc();
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let handler = channel_id.handler.clone();

    let (cursor_next_seq, captured) = {
      let Some(channel) = self.channels.get(&handler) else {
        self.metrics.fifo_pops.get_or_create(&FIFO_CHANNEL_NOT_FOUND).inc();
        return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
      };

      let cursor_next_seq = match &channel.kind {
        ChannelKind::PubSub => {
          self.metrics.fifo_pops.get_or_create(&FIFO_WRONG_TYPE).inc();
          return Err(narwhal_protocol::Error::new(WrongType).with_id(correlation_id).into());
        },
        ChannelKind::Fifo(FifoState::NeedsRecovery) => {
          self.metrics.fifo_pops.get_or_create(&FIFO_CURSOR_RECOVERY).inc();
          return Err(narwhal_protocol::Error::new(CursorRecoveryRequired).with_id(correlation_id).into());
        },
        ChannelKind::Fifo(FifoState::Healthy(c)) => c.next_seq(),
      };

      if !channel.is_member(&nid) {
        self.metrics.fifo_pops.get_or_create(&FIFO_USER_NOT_IN_CHANNEL).inc();
        return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
      }

      if !channel.acl.is_read_allowed(&nid) {
        self.metrics.fifo_pops.get_or_create(&FIFO_NOT_ALLOWED).inc();
        return Err(narwhal_protocol::Error::new(NotAllowed).with_id(correlation_id).into());
      }

      let log_last_seq = channel.message_log.last_seq();
      if cursor_next_seq > log_last_seq {
        self.metrics.fifo_pops.get_or_create(&FIFO_QUEUE_EMPTY).inc();
        return Err(narwhal_protocol::Error::new(QueueEmpty).with_id(correlation_id).into());
      }

      if cursor_next_seq > channel.message_log.last_durable_seq() {
        // Entry is buffered but not yet fsynced. Force a sync flush before
        // we commit the consumption via the cursor advance; otherwise a
        // crash between the cursor fsync and the next log flush would leave
        // `cursor.next_seq > log.last_seq() + 1` on disk (corruption).
        let start = Instant::now();
        let result = channel.message_log.flush().await;
        self.metrics.record_flush(start, &result);
        result?;
      }

      let mut visitor = PopVisitor { pool: &self.history_pool, captured: None };
      channel.message_log.read(cursor_next_seq, 1, &mut visitor).await?;
      let entry = visitor
        .captured
        .ok_or_else(|| anyhow::anyhow!("fifo pop: log.read returned no entry at seq {cursor_next_seq}"))?;
      (cursor_next_seq, entry)
    };

    // `MessageLog::read(from_seq, limit, visitor)` returns the first entry
    // with `seq >= from_seq`, NOT strictly at `from_seq`. If the log has a
    // gap at the cursor (e.g. a prior failed append left `channel.seq` ahead
    // of the log tail before that path was fixed, or external corruption),
    // the read would silently return a later entry. Advancing the cursor by
    // one in that case would re-read the same later entry on the next POP,
    // causing duplicate delivery. Refuse to advance and bubble up an
    // internal error instead; an operator-driven path (DELETE / CLEAR in
    // PR 3) can heal it.
    if captured.seq != cursor_next_seq {
      let entry_seq = captured.seq;
      tracing::error!(
        channel = %handler,
        cursor_next_seq,
        entry_seq,
        "fifo pop: log returned an entry at a higher seq than the cursor; refusing to advance to avoid duplicate delivery"
      );
      return Err(
        narwhal_protocol::Error::new(InternalServerError)
          .with_id(correlation_id)
          .with_detail("fifo log has a gap at the head cursor")
          .into(),
      );
    }

    let PopEntry { payload, length, timestamp, .. } = captured;

    let new_next_seq = {
      let channel = self.channels.get_mut(&handler).expect("channel disappeared between pop phases");
      let cursor = match &mut channel.kind {
        ChannelKind::Fifo(FifoState::Healthy(c)) => c,
        _ => unreachable!("channel kind mutated between pop phases"),
      };
      let start = Instant::now();
      let advance_result = cursor.advance().await;
      self.metrics.fifo_cursor_fsync_seconds.observe(start.elapsed().as_secs_f64());
      advance_result?;
      cursor.next_seq()
    };

    transmitter.send_message_with_payload(
      Message::PopAck(PopAckParameters { id: correlation_id, length, timestamp }),
      Some(payload),
    );
    self.metrics.fifo_pops.get_or_create(&SUCCESS).inc();

    // Best-effort lazy head-eviction. Failures must not propagate: the POP
    // already committed (cursor fsynced, ACK sent). Cheap when no segment
    // qualifies.
    let log = {
      let channel = self.channels.get(&handler).expect("channel disappeared after pop");
      channel.message_log.clone()
    };
    if let Err(e) = log.evict_below(new_next_seq).await {
      warn!(channel = %handler, error = %e, "FIFO head-eviction failed (best-effort)");
    }

    Ok(())
  }

  /// GET_CHAN_LEN handler. Owner-only queue depth query.
  fn get_channel_len(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get(&channel_id.handler) else {
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    let cursor_next_seq = match &channel.kind {
      ChannelKind::PubSub => {
        return Err(narwhal_protocol::Error::new(WrongType).with_id(correlation_id).into());
      },
      ChannelKind::Fifo(FifoState::NeedsRecovery) => {
        return Err(narwhal_protocol::Error::new(CursorRecoveryRequired).with_id(correlation_id).into());
      },
      ChannelKind::Fifo(FifoState::Healthy(c)) => c.next_seq(),
    };

    if !channel.is_member(&nid) {
      return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }

    if !channel.is_owner(&nid) {
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    let log_last_seq = channel.message_log.last_seq();
    let depth = if cursor_next_seq > log_last_seq { 0 } else { log_last_seq - cursor_next_seq + 1 };
    // Logical queue depth is bounded by `max_persist_messages` (u32) by the
    // QUEUE_FULL gate on PUSH; saturate to u32::MAX for theoretical safety.
    let count = depth.min(u32::MAX as u64) as u32;

    transmitter.send_message(Message::ChannelLen(ChannelLenParameters {
      id: correlation_id,
      channel: channel_id.into(),
      count,
    }));

    Ok(())
  }

  /// CLEAR handler. Owner-only, idempotent, and the recovery path out of
  /// `NeedsRecovery`. Discards every queued element by flushing the log and
  /// writing a fresh `cursor.bin` whose `next_seq = log.last_seq() + 1`.
  /// Membership, ACLs, configuration, and seq monotonicity are preserved
  /// (no reset to 1). Exempt from `CURSOR_RECOVERY_REQUIRED` since this is
  /// the operator-less heal for that state.
  async fn clear(
    &mut self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    if channel_id.domain != self.local_domain {
      self.metrics.fifo_clears.get_or_create(&FIFO_NOT_IMPLEMENTED).inc();
      return Err(narwhal_protocol::Error::new(NotImplemented).with_id(correlation_id).into());
    }

    let Some(channel) = self.channels.get_mut(&channel_id.handler) else {
      self.metrics.fifo_clears.get_or_create(&FIFO_CHANNEL_NOT_FOUND).inc();
      return Err(narwhal_protocol::Error::new(ChannelNotFound).with_id(correlation_id).into());
    };

    if matches!(&channel.kind, ChannelKind::PubSub) {
      self.metrics.fifo_clears.get_or_create(&FIFO_WRONG_TYPE).inc();
      return Err(narwhal_protocol::Error::new(WrongType).with_id(correlation_id).into());
    }

    if !channel.is_member(&nid) {
      self.metrics.fifo_clears.get_or_create(&FIFO_USER_NOT_IN_CHANNEL).inc();
      return Err(narwhal_protocol::Error::new(UserNotInChannel).with_id(correlation_id).into());
    }

    if !channel.is_owner(&nid) {
      self.metrics.fifo_clears.get_or_create(&FIFO_FORBIDDEN).inc();
      return Err(narwhal_protocol::Error::new(Forbidden).with_id(correlation_id).into());
    }

    // Step 1: synchronously flush + fsync the log so `log.last_seq()` is
    // anchored on disk. The cursor we are about to write must point past
    // a durable tail; otherwise a crash between cursor.bin fsync and the
    // next log flush would leave `cursor.next_seq > log.last_seq() + 1`,
    // which the recovery model treats as corruption.
    let start = Instant::now();
    let result = channel.message_log.flush().await;
    self.metrics.record_flush(start, &result);
    result?;

    // Step 2: atomic-write `cursor.bin` with the new head. Uses the same
    // crash-safe path as the type-transition write (tmp -> fsync -> rename
    // -> fsync parent).
    let new_next_seq = channel.message_log.last_seq().saturating_add(1);
    let Some(channel_dir) = self.message_log_factory.channel_dir(&channel_id.handler) else {
      // FIFO requires a persistent message log factory; the absence of a
      // channel_dir is a configuration error.
      return Err(narwhal_protocol::Error::new(InternalServerError).with_id(correlation_id).into());
    };
    let start = Instant::now();
    let new_cursor = FifoCursor::write_initial(channel_dir, new_next_seq).await;
    self.metrics.fifo_cursor_fsync_seconds.observe(start.elapsed().as_secs_f64());
    let new_cursor = new_cursor?;

    // Step 3: install the fresh cursor and align channel.seq. `NeedsRecovery`
    // transitions back to `Healthy` here; an already-`Healthy` channel
    // replaces its in-memory cursor handle with the freshly written one.
    channel.kind = ChannelKind::Fifo(FifoState::Healthy(new_cursor));
    channel.seq = new_next_seq;

    // Step 4: ACK. The client can now resume PUSH/POP on the cleared queue.
    transmitter.send_message(Message::ClearAck(ClearAckParameters { id: correlation_id }));
    self.metrics.fifo_clears.get_or_create(&SUCCESS).inc();

    // Step 5: best-effort lazy head-eviction. The cursor now sits past every
    // sealed segment, so head-eviction can reclaim them all (the tail-segment
    // retention invariant keeps the active segment around so `log.last_seq()`
    // stays recoverable). Failures must not propagate: CLEAR already
    // committed (cursor durable, ACK sent).
    let log = channel.message_log.clone();
    let handler = channel_id.handler.clone();
    if let Err(e) = log.evict_below(new_next_seq).await {
      warn!(channel = %handler, error = %e, "FIFO head-eviction after CLEAR failed (best-effort)");
    }

    Ok(())
  }

  /// Best-effort cleanup of persistent storage for a channel (message log + store record).
  /// Errors are logged but never propagated. Callers must not depend on storage cleanup
  /// succeeding for in-memory consistency.
  async fn delete_persistent_storage(&mut self, handler: &StringAtom) {
    if let Some(channel) = self.channels.get_mut(handler) {
      let result = channel.message_log.delete().await;
      self.metrics.record_message_log_delete(&result);
      if let Err(e) = result {
        warn!(channel = %handler, error = %e, "failed to delete persisted message log");
      }
    }
    if let Some(hash) = self.channels.get(handler).and_then(|c| c.store_hash.clone()) {
      match self.store.delete_channel(&hash).await {
        Ok(()) => {
          self.metrics.store_deletes.get_or_create(&SUCCESS).inc();
        },
        Err(e) => {
          self.metrics.store_deletes.get_or_create(&FAILURE).inc();
          warn!(channel = %handler, error = %e, "failed to delete persisted channel");
        },
      }
    }
  }

  /// Performs the core leave logic: notify members, remove from channel, handle owner change
  /// or channel deletion. Returns `true` if the member was actually removed.
  async fn do_leave(
    &mut self,
    handler: &StringAtom,
    nid: &Nid,
    excluding_resource: Option<Resource>,
  ) -> anyhow::Result<bool> {
    let Some(channel) = self.channels.get(handler) else {
      return Ok(false);
    };

    if !channel.is_member(nid) {
      return Ok(false);
    }

    let as_owner = channel.is_owner(nid);
    let will_be_empty = channel.member_count() == 1;

    // Pick the new owner once (if needed) so the same value is used for both
    // the persisted projection and the in-memory update.
    let new_owner =
      if as_owner && !will_be_empty { Some(channel.members.iter().find(|m| *m != nid).unwrap().clone()) } else { None };

    // Build new_members once (only when the channel will survive) so it can be
    // shared between the persist and in-memory paths without a second O(n) rebuild.
    let new_members: Option<Rc<[Nid]>> = if !will_be_empty {
      channel.members.binary_search(nid).ok().map(|pos| {
        let mut v: Vec<Nid> = channel.members.iter().cloned().collect();
        v.remove(pos);
        Rc::from(v)
      })
    } else {
      None
    };

    // Persist the projected membership before any in-memory changes or notifications.
    // Skip when the channel will be empty.
    let store_hash = if channel.config.persist == Some(true) && !will_be_empty {
      let mut projected = channel.to_persisted();
      if let Some(ref members) = new_members {
        projected.members = members.clone();
      }
      projected.owner = new_owner.clone().or(projected.owner);
      let start = Instant::now();
      let result = self.store.save_channel(&projected).await;
      self.metrics.store_save_duration_seconds.observe(start.elapsed().as_secs_f64());
      match result {
        Ok(hash) => {
          self.metrics.store_saves.get_or_create(&SUCCESS).inc();
          Some(hash)
        },
        Err(e) => {
          self.metrics.store_saves.get_or_create(&FAILURE).inc();
          return Err(e);
        },
      }
    } else {
      None
    };

    if let Err(e) = channel.notify_member_left(nid, excluding_resource, as_owner, self.local_domain.clone()).await {
      warn!(channel = %handler, error = %e, "failed to notify member left");
    }

    let channel = self.channels.get_mut(handler).unwrap();
    if let Some(hash) = store_hash {
      channel.store_hash = Some(hash);
    }
    if let Some(members) = new_members {
      if channel.owner.as_ref() == Some(nid) {
        channel.owner = None;
      }
      channel.members = members;
      channel.update_allowed_targets();
    } else {
      channel.remove_member(nid);
    }

    if channel.is_empty() {
      let is_persistent = channel.config.persist == Some(true);
      channel.cancel_flush_task();
      if is_persistent {
        let start = Instant::now();
        let result = channel.message_log.flush().await;
        self.metrics.record_flush(start, &result);
        if let Err(e) = result {
          warn!(channel = %handler, error = %e, "final flush on empty channel failed");
        }
        self.delete_persistent_storage(handler).await;
      }
      self.channels.remove(handler);
      self.total_channels.fetch_sub(1, Ordering::SeqCst);
      self.metrics.channels_active.dec();
      return Ok(true);
    }

    if let Some(new_owner) = new_owner {
      let channel = self.channels.get_mut(handler).unwrap();
      channel.owner = Some(new_owner.clone());
      if let Err(e) = channel.notify_member_joined(&new_owner, None, true, self.local_domain.clone()).await {
        warn!(channel = %handler, error = %e, "failed to notify new owner");
      }
    }

    Ok(true)
  }
}

/// Field-by-field equality on `ChannelConfig` used to detect a no-op
/// `SET_CHAN_CONFIG` (so `CHANNEL_RECONFIGURED` does not fire for an
/// identical re-submission). `ChannelConfig` does not derive `PartialEq`
/// because it is shared with `PersistedChannel` via serde and adding the
/// derive there would lock us into pre-prod schema decisions; a small
/// local helper is cheaper than that.
fn new_config_equals(a: &ChannelConfig, b: &ChannelConfig) -> bool {
  a.max_clients == b.max_clients
    && a.max_payload_size == b.max_payload_size
    && a.max_persist_messages == b.max_persist_messages
    && a.persist == b.persist
    && a.message_flush_interval == b.message_flush_interval
}

/// Limits for the channel manager.
#[derive(Clone, Debug)]
pub struct ChannelManagerLimits {
  pub max_channels: u32,
  pub max_clients_per_channel: u32,
  pub max_channels_per_client: u32,
  pub max_payload_size: u32,
  pub max_persist_messages: u32,
  pub max_message_flush_interval: u32,
  pub max_history_limit: u32,
}

/// The channel manager.
pub struct ChannelManager<CS: ChannelStore, MLF: MessageLogFactory> {
  store: CS,
  message_log_factory: MLF,
  mailboxes: Arc<[Sender<Command>]>,
  membership: Membership,
  router: GlobalRouter,
  notifier: Notifier,
  total_channels: Arc<AtomicUsize>,
  limits: ChannelManagerLimits,
  metrics: ChannelManagerMetrics,
  mailbox_capacity: usize,
}

impl<CS: ChannelStore, MLF: MessageLogFactory> Clone for ChannelManager<CS, MLF> {
  fn clone(&self) -> Self {
    Self {
      store: self.store.clone(),
      message_log_factory: self.message_log_factory.clone(),
      mailboxes: self.mailboxes.clone(),
      membership: self.membership.clone(),
      router: self.router.clone(),
      notifier: self.notifier.clone(),
      total_channels: self.total_channels.clone(),
      limits: self.limits.clone(),
      metrics: self.metrics.clone(),
      mailbox_capacity: self.mailbox_capacity,
    }
  }
}

impl<CS: ChannelStore, MLF: MessageLogFactory> std::fmt::Debug for ChannelManager<CS, MLF> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ChannelManager").finish_non_exhaustive()
  }
}

// === impl ChannelManager ===

impl<CS: ChannelStore, MLF: MessageLogFactory> ChannelManager<CS, MLF> {
  /// Creates a new channel manager with the specified configuration.
  pub fn new(
    router: GlobalRouter,
    notifier: Notifier,
    limits: ChannelManagerLimits,
    store: CS,
    message_log_factory: MLF,
    registry: &mut Registry,
  ) -> Self {
    Self {
      store,
      message_log_factory,
      mailboxes: Arc::from([]),
      membership: Membership::new(),
      router,
      notifier,
      total_channels: Arc::new(AtomicUsize::new(0)),
      limits,
      metrics: ChannelManagerMetrics::register(registry),
      mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
    }
  }

  /// Spawns one shard actor per core on the given dispatcher.
  ///
  /// When `restore_channels` is `true`, persisted channels are loaded from the
  /// store and restored into the appropriate shards. Pass `false` when auth is
  /// disabled; without auth, memberships are ephemeral, so restoring them at
  /// startup would leave orphaned channels.
  pub async fn bootstrap(&mut self, core_dispatcher: &CoreDispatcher, restore_channels: bool) -> anyhow::Result<()> {
    self.membership.bootstrap(core_dispatcher).await?;

    let shard_count = core_dispatcher.shard_count();
    let mut mailboxes = Vec::with_capacity(shard_count);
    let local_domain = self.router.c2s_router().local_domain();

    // Load persisted channel hashes and determine shard assignment.
    // Each hash's channel is loaded to extract the handler for shard routing,
    // then the shard re-loads the full metadata on its own thread during restore.
    let channel_hashes: Arc<[StringAtom]> = if restore_channels {
      let start = Instant::now();
      let result = self.store.load_channel_hashes().await;
      self.metrics.record_load(start, &result);
      result?
    } else {
      Arc::from([])
    };

    let mut shard_hashes: Vec<Vec<StringAtom>> = vec![Vec::new(); shard_count];
    for hash in channel_hashes.iter() {
      let start = Instant::now();
      let result = self.store.load_channel(hash).await;
      self.metrics.record_load(start, &result);
      match result {
        Ok(persisted) => {
          let shard_id = shard_for(&persisted.handler, shard_count);
          shard_hashes[shard_id].push(hash.clone());
        },
        Err(e) => {
          warn!(hash = %hash, error = %e, "skipping channel restore: failed to load persisted channel");
        },
      }
    }

    for (shard_id, hashes_for_shard) in shard_hashes.into_iter().enumerate() {
      let (tx, rx) = async_channel::bounded(self.mailbox_capacity);
      let mailbox_tx = tx.clone();
      mailboxes.push(tx);

      let store = self.store.clone();
      let message_log_factory = self.message_log_factory.clone();
      let membership = self.membership.clone();
      let router = self.router.clone();
      let notifier = self.notifier.clone();
      let local_domain = local_domain.clone();
      let total_channels = self.total_channels.clone();
      let limits = self.limits.clone();
      let metrics = self.metrics.clone();

      // 2× max_history_limit: one replay's worth of buffers can still be draining
      // through the outbound queue when the next replay starts.
      let history_pool_cap = (limits.max_history_limit as usize).saturating_mul(2).max(1);
      let history_pool = Pool::new(history_pool_cap, limits.max_payload_size as usize);

      core_dispatcher
        .dispatch_at_shard(shard_id, move || async move {
          let shard = ChannelShard {
            channels: HashMap::new(),
            store,
            message_log_factory,
            membership,
            mailbox: rx,
            mailbox_tx,
            router,
            notifier,
            local_domain,
            total_channels,
            limits,
            metrics,
            auth_enabled: restore_channels,
            history_pool,
          };
          shard.restore_and_run(hashes_for_shard).await;
        })
        .await?;
    }

    self.mailboxes = Arc::from(mailboxes);

    Ok(())
  }

  /// Shuts down the channel manager.
  pub fn shutdown(&self) {
    for tx in self.mailboxes.iter() {
      tx.close();
    }
    self.membership.shutdown();
  }

  /// Returns the total number of active channels across all shards.
  pub fn total_channels(&self) -> usize {
    self.total_channels.load(Ordering::SeqCst)
  }

  /// Joins a channel.
  pub async fn join_channel(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    on_behalf_nid: Option<Nid>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<bool> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::JoinChannel { channel_id, nid, on_behalf_nid, transmitter, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// Leaves a channel.
  pub async fn leave_channel(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    on_behalf_nid: Option<Nid>,
    transmitter: Option<Arc<dyn Transmitter>>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::LeaveChannel { channel_id, nid, on_behalf_nid, transmitter, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// Removes a user from all channels they are a member of.
  pub async fn leave_all_channels(&self, nid: Nid) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    // Get channels first.
    let handlers = self.membership.get_channels(&nid.username).await;
    if handlers.is_empty() {
      return Ok(());
    }

    // Group handlers by channel shard.
    let mut by_shard: HashMap<usize, Vec<StringAtom>> = HashMap::new();
    for handler in handlers.iter() {
      let shard = shard_for(handler, self.mailboxes.len());
      by_shard.entry(shard).or_default().push(handler.clone());
    }

    // Send batch leave to each relevant shard.
    let mut reply_rxs = Vec::with_capacity(by_shard.len());

    for (shard, handlers) in by_shard {
      let (reply_tx, reply_rx) = async_channel::bounded(1);
      self.mailboxes[shard].send(Command::LeaveChannels { nid: nid.clone(), handlers, reply_tx }).await?;
      reply_rxs.push(reply_rx);
    }
    for reply_rx in reply_rxs {
      reply_rx.recv().await??;
    }

    Ok(())
  }

  /// Deletes a channel.
  pub async fn delete_channel(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::DeleteChannel { channel_id, nid, transmitter, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// Broadcasts a payload to all members of a channel.
  pub async fn broadcast_payload(
    &self,
    payload: PoolBuffer,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    qos: Option<u8>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::BroadcastPayload { payload, channel_id, nid, transmitter, qos, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// Gets the ACL for a channel.
  #[allow(clippy::too_many_arguments)]
  pub async fn get_channel_acl(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    acl_type: AclType,
    page: Option<u32>,
    page_size: Option<u32>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::GetChannelAcl {
        channel_id,
        nid,
        acl_type,
        page,
        page_size,
        transmitter,
        correlation_id,
        reply_tx,
      })
      .await?;

    reply_rx.recv().await?
  }

  /// Sets the ACL for a channel.
  #[allow(clippy::too_many_arguments)]
  pub async fn set_channel_acl(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    nids: Vec<Nid>,
    acl_type: AclType,
    acl_action: AclAction,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::SetChannelAcl {
        channel_id,
        nid,
        nids,
        acl_type,
        acl_action,
        transmitter,
        correlation_id,
        reply_tx,
      })
      .await?;

    reply_rx.recv().await?
  }

  /// Gets the configuration for a channel.
  pub async fn get_channel_configuration(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::GetChannelConfiguration { channel_id, nid, transmitter, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// Sets the configuration for a channel.
  #[allow(clippy::too_many_arguments)]
  pub async fn set_channel_configuration(
    &self,
    config: ChannelConfig,
    requested_type: Option<ChannelType>,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::SetChannelConfiguration {
        config,
        requested_type,
        channel_id,
        nid,
        transmitter,
        correlation_id,
        reply_tx,
      })
      .await?;

    reply_rx.recv().await?
  }

  /// Lists all channels a user is a member of.
  pub async fn list_channels(
    &self,
    nid: Nid,
    page: Option<u32>,
    count: Option<u32>,
    as_owner: bool,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let all_handlers = self.membership.get_channels(&nid.username).await;
    let local_domain = self.router.c2s_router().local_domain();

    let mut channel_list: Vec<StringAtom> = if as_owner {
      // Group by shard, fan-out to check ownership.
      let mut by_shard: HashMap<usize, Vec<StringAtom>> = HashMap::new();
      for handler in all_handlers.iter() {
        by_shard.entry(shard_for(handler, self.mailboxes.len())).or_default().push(handler.clone());
      }
      let mut reply_rxs = Vec::with_capacity(by_shard.len());
      for (shard, handlers) in by_shard {
        let (reply_tx, reply_rx) = async_channel::bounded(1);
        self.mailboxes[shard].send(Command::FilterOwnedChannels { nid: nid.clone(), handlers, reply_tx }).await?;
        reply_rxs.push(reply_rx);
      }
      let mut result = Vec::new();
      for reply_rx in reply_rxs {
        if let Ok(partial) = reply_rx.recv().await {
          result.extend(partial);
        }
      }
      result
    } else {
      all_handlers.iter().map(|h| ChannelId::new_unchecked(h.clone(), local_domain.clone()).into()).collect()
    };

    channel_list.sort();

    let page = page.unwrap_or(1);
    let page_size = count.unwrap_or(20).min(MAX_CHANNELS_PAGE_SIZE);
    let start = ((page - 1) * page_size) as usize;
    let end = (page * page_size) as usize;

    let paginated_channels =
      if start < channel_list.len() { channel_list[start..end.min(channel_list.len())].to_vec() } else { Vec::new() };
    let include_pagination_info = paginated_channels.len() < channel_list.len();
    let (page, page_size, total_count) = if include_pagination_info {
      (Some(page), Some(page_size), Some(channel_list.len() as u32))
    } else {
      (None, None, None)
    };

    transmitter.send_message(Message::ListChannelsAck(ListChannelsAckParameters {
      id: correlation_id,
      channels: paginated_channels,
      page,
      page_size,
      total_count,
    }));

    Ok(())
  }

  /// Lists all members of a channel.
  pub async fn list_members(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    page: Option<u32>,
    count: Option<u32>,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::ListMembers { channel_id, nid, page, count, transmitter, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// Requests historical messages from a channel.
  #[allow(clippy::too_many_arguments)]
  pub async fn history(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    history_id: StringAtom,
    from_seq: u64,
    limit: u32,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::History { channel_id, nid, history_id, from_seq, limit, transmitter, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// Queries the available sequence range of a channel's message log.
  pub async fn channel_seq(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard].send(Command::ChannelSeq { channel_id, nid, transmitter, correlation_id, reply_tx }).await?;

    reply_rx.recv().await?
  }

  /// PUSH an element onto a FIFO channel. Owner-only. The payload's size is
  /// gated by `max_payload_size`; the logical queue depth is gated by
  /// `max_persist_messages` (`QUEUE_FULL`).
  pub async fn push_payload(
    &self,
    payload: PoolBuffer,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::Push { payload, channel_id, nid, transmitter, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// POP an element from a FIFO channel. Members subject to the read ACL.
  /// Advances the head cursor durably before replying so a crash mid-POP
  /// cannot deliver the element twice.
  pub async fn pop_payload(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard].send(Command::Pop { channel_id, nid, transmitter, correlation_id, reply_tx }).await?;

    reply_rx.recv().await?
  }

  /// Queries the logical queue depth of a FIFO channel. Owner-only.
  pub async fn get_channel_len(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard]
      .send(Command::GetChannelLen { channel_id, nid, transmitter, correlation_id, reply_tx })
      .await?;

    reply_rx.recv().await?
  }

  /// CLEAR a FIFO channel. Owner-only, idempotent. Discards every queued
  /// element without destroying the channel, preserving membership, ACLs,
  /// configuration, and seq monotonicity. Also the recovery path for a
  /// channel in `NeedsRecovery` state (exempt from `CURSOR_RECOVERY_REQUIRED`).
  pub async fn clear(
    &self,
    channel_id: ChannelId,
    nid: Nid,
    transmitter: Arc<dyn Transmitter>,
    correlation_id: u32,
  ) -> anyhow::Result<()> {
    self.assert_bootstrapped();

    let shard = shard_for(&channel_id.handler, self.mailboxes.len());
    let (reply_tx, reply_rx) = async_channel::bounded(1);

    self.mailboxes[shard].send(Command::Clear { channel_id, nid, transmitter, correlation_id, reply_tx }).await?;

    reply_rx.recv().await?
  }

  /// Asserts that `bootstrap()` has been called.
  fn assert_bootstrapped(&self) {
    debug_assert!(!self.mailboxes.is_empty(), "ChannelManager::bootstrap() must be called before use");
  }
}

/// Per-domain ACLs.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct Acl {
  allow_lists: HashMap<StringAtom, HashSet<StringAtom>>,
}

// === impl Acl ===

impl Acl {
  /// Updates the ACL based on action.
  fn update(&mut self, nids: Vec<Nid>, action: AclAction) {
    match action {
      AclAction::Add => {
        for nid in nids {
          let domain = nid.domain.clone();
          let username = nid.username.clone();
          let domain_users = self.allow_lists.entry(domain).or_default();
          if !nid.is_server() {
            domain_users.insert(username);
          }
        }
      },
      AclAction::Remove => {
        for nid in nids {
          let domain = nid.domain.clone();
          let username = nid.username.clone();
          if let Some(domain_users) = self.allow_lists.get_mut(&domain) {
            domain_users.remove(&username);
            if domain_users.is_empty() {
              self.allow_lists.remove(&domain);
            }
          }
        }
      },
    }
  }

  fn is_allowed(&self, nid: &Nid) -> bool {
    if self.allow_lists.is_empty() {
      return true;
    }
    let domain = nid.domain.clone();
    let username = nid.username.clone();
    if let Some(allowed_users) = self.allow_lists.get(&domain) {
      if allowed_users.is_empty() {
        return true;
      }
      return allowed_users.contains(&username);
    }
    false
  }

  pub fn allow_list(&self) -> Vec<Nid> {
    let mut allow_list: Vec<Nid> = Vec::with_capacity(self.allow_lists.len());
    for (domain, allowed_users) in self.allow_lists.iter() {
      if !allowed_users.is_empty() {
        for username in allowed_users.iter() {
          allow_list.push(Nid::new_unchecked(username.clone(), domain.clone()));
        }
      } else {
        allow_list.push(Nid::new_unchecked(StringAtom::default(), domain.clone()));
      }
    }
    allow_list.sort();
    allow_list
  }

  pub fn total_entries(&self) -> usize {
    let mut total = 0;
    for (_, allowed_users) in self.allow_lists.iter() {
      total += allowed_users.len();
    }
    total
  }
}

/// The channel ACL.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ChannelAcl {
  join_acl: Acl,
  publish_acl: Acl,
  read_acl: Acl,
}

// === impl ChannelAcl ===

impl ChannelAcl {
  pub fn is_join_allowed(&self, nid: &Nid) -> bool {
    self.join_acl.is_allowed(nid)
  }

  pub fn is_publish_allowed(&self, nid: &Nid) -> bool {
    self.publish_acl.is_allowed(nid)
  }

  pub fn is_read_allowed(&self, nid: &Nid) -> bool {
    self.read_acl.is_allowed(nid)
  }

  pub fn update(&mut self, nids: Vec<Nid>, acl_type: AclType, action: AclAction) {
    match acl_type {
      AclType::Join => self.join_acl.update(nids, action),
      AclType::Publish => self.publish_acl.update(nids, action),
      AclType::Read => self.read_acl.update(nids, action),
    }
  }
}

/// Visitor that captures a single log entry's seq, payload, and timestamp for
/// the FIFO `POP` path. The visitor copies the payload into a pooled buffer
/// so the entry borrow ends with `visit()`, leaving the captured tuple owned
/// and usable past the read. The `seq` is kept so the POP handler can verify
/// the entry it actually got matches the seq it asked for: a mismatch means
/// the log has a gap below the tail and we must refuse to advance the
/// cursor (would otherwise lead to duplicate delivery of the later entry).
struct PopVisitor<'a> {
  pool: &'a Pool,
  captured: Option<PopEntry>,
}

struct PopEntry {
  seq: u64,
  payload: PoolBuffer,
  length: u32,
  timestamp: u64,
}

#[async_trait(?Send)]
impl LogVisitor for PopVisitor<'_> {
  async fn visit(&mut self, entry: LogEntry<'_>) -> anyhow::Result<()> {
    let len = entry.payload.len();
    let mut buf = self.pool.acquire_buffer().await;
    buf.as_mut_slice()[..len].copy_from_slice(entry.payload);
    let payload = buf.freeze(len);
    self.captured = Some(PopEntry { seq: entry.seq, payload, length: len as u32, timestamp: entry.timestamp });
    Ok(())
  }
}

/// Visitor that streams log entries to a client as MESSAGE frames with a `history_id`.
struct HistoryVisitor<'a> {
  transmitter: &'a Arc<dyn Transmitter>,
  channel: &'a StringAtom,
  history_id: &'a StringAtom,
  pool: &'a Pool,
}

#[async_trait(?Send)]
impl LogVisitor for HistoryVisitor<'_> {
  async fn visit(&mut self, entry: LogEntry<'_>) -> anyhow::Result<()> {
    let msg = Message::Message(MessageParameters {
      from: StringAtom::from(std::str::from_utf8(entry.from)?),
      channel: self.channel.clone(),
      length: entry.payload.len() as u32,
      seq: entry.seq,
      timestamp: entry.timestamp,
      history_id: Some(self.history_id.clone()),
    });

    let mut buf = self.pool.acquire_buffer().await;
    buf.as_mut_slice()[..entry.payload.len()].copy_from_slice(entry.payload);
    let payload = buf.freeze(entry.payload.len());
    self.transmitter.send_message_with_payload(msg, Some(payload));

    Ok(())
  }
}

/// The channel configuration.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ChannelConfig {
  pub max_clients: Option<u32>,
  pub max_payload_size: Option<u32>,
  pub max_persist_messages: Option<u32>,
  pub persist: Option<bool>,
  pub message_flush_interval: Option<u32>,
}

// === impl ChannelConfig ===

impl ChannelConfig {
  pub fn merge(&self, other: &Self) -> Self {
    let mut config = self.clone();
    if let Some(v) = other.max_clients {
      config.max_clients = Some(v);
    }
    if let Some(v) = other.max_payload_size {
      config.max_payload_size = Some(v);
    }
    if let Some(v) = other.max_persist_messages {
      config.max_persist_messages = Some(v);
    }
    if let Some(v) = other.persist {
      config.persist = Some(v);
    }
    if let Some(v) = other.message_flush_interval {
      config.message_flush_interval = Some(v);
    }
    config
  }
}
