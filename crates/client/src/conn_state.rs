// SPDX-License-Identifier: BSD-3-Clause

use std::collections::HashMap;
use std::sync::Arc;

use async_channel::Sender as AcSender;
use async_lock::SemaphoreGuardArc;
use futures_channel::oneshot;
use parking_lot::Mutex as PlMutex;
use parking_lot::RwLock as PlRwLock;

use narwhal_protocol::Message;
use narwhal_util::pool::PoolBuffer;
use narwhal_util::string_atom::StringAtom;

use crate::types::HistoryEntry;

pub(crate) const OUTBOUND_QUEUE_SIZE: usize = 4 * 1024;

pub(crate) const INBOUND_QUEUE_SIZE: usize = 16 * 1024;

/// Type alias for the oneshot sender used to deliver a response back to
/// the caller of `send_message`.
pub(crate) type ResponseSender = oneshot::Sender<anyhow::Result<(Message, Option<PoolBuffer>)>>;

/// Thread-safe, cloneable slot that records the first connection error.
///
/// Used by reader / writer tasks to signal that the connection has become
/// unhealthy, and by the pool's recycle logic to detect stale connections.
#[derive(Clone, Debug)]
pub(crate) struct ErrorState(Arc<PlMutex<Option<anyhow::Error>>>);

impl ErrorState {
  pub fn new() -> Self {
    Self(Arc::new(PlMutex::new(None)))
  }

  #[inline]
  pub fn set_error(&self, error: anyhow::Error) {
    self.0.lock().replace(error);
  }

  #[inline]
  pub fn has_error(&self) -> bool {
    self.0.lock().is_some()
  }

  #[inline]
  pub fn take_error(&self) -> Option<anyhow::Error> {
    self.0.lock().take()
  }
}

/// A single in-flight request awaiting a correlated server response.
pub(crate) struct PendingRequest {
  /// Oneshot sender for the response.  Taken (set to `None`) once the
  /// response arrives.
  pub sender: Option<ResponseSender>,

  /// Owned semaphore permit, released when this entry is removed,
  /// allowing another request to be submitted.
  pub _permit: SemaphoreGuardArc,
}

/// Thread-safe map of correlation-ID → [`PendingRequest`].
///
/// Both the reader task (which dispatches responses) and the request
/// submission path (which inserts entries) access this concurrently,
/// hence the `RwLock`.
#[derive(Clone)]
pub(crate) struct PendingRequests(Arc<PlRwLock<HashMap<u32, PendingRequest>>>);

impl PendingRequests {
  pub fn new() -> Self {
    Self(Arc::new(PlRwLock::new(HashMap::new())))
  }

  #[inline]
  pub fn insert(&self, correlation_id: u32, request: PendingRequest) {
    self.0.write().insert(correlation_id, request);
  }

  /// Takes the [`ResponseSender`] for the given correlation ID, leaving
  /// the entry in place (the permit is still held until [`remove`] is
  /// called).
  #[inline]
  pub fn take_response_sender(&self, correlation_id: u32) -> Option<ResponseSender> {
    self.0.write().get_mut(&correlation_id).and_then(|r| r.sender.take())
  }

  #[inline]
  pub fn remove(&self, correlation_id: &u32) -> Option<PendingRequest> {
    self.0.write().remove(correlation_id)
  }
}

/// Thread-safe map of `history_id` → `Sender<HistoryEntry>` for in-flight
/// `HISTORY` requests. The reader task routes inbound `MESSAGE` frames whose
/// `history_id` is registered here to the per-request channel instead of the
/// general `inbound_stream`, so callers of `history()` see a coherent reply
/// even while concurrent broadcasts are flowing.
#[derive(Clone)]
pub(crate) struct PendingHistories(Arc<PlRwLock<HashMap<StringAtom, AcSender<HistoryEntry>>>>);

impl PendingHistories {
  pub fn new() -> Self {
    Self(Arc::new(PlRwLock::new(HashMap::new())))
  }

  #[inline]
  pub fn register(&self, history_id: StringAtom, sender: AcSender<HistoryEntry>) {
    self.0.write().insert(history_id, sender);
  }

  /// Returns a clone of the `Sender` registered for `history_id`, if any.
  /// The reader task uses this to route a single MESSAGE frame; the entry is
  /// not removed from the map (multiple frames share the same collector).
  #[inline]
  pub fn get_sender(&self, history_id: &StringAtom) -> Option<AcSender<HistoryEntry>> {
    self.0.read().get(history_id).cloned()
  }

  #[inline]
  pub fn unregister(&self, history_id: &StringAtom) {
    self.0.write().remove(history_id);
  }
}
