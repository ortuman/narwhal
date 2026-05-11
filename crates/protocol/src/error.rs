// SPDX-License-Identifier: BSD-3-Clause

use std::str::FromStr;

use narwhal_util::string_atom::StringAtom;

use crate::{ErrorParameters, Message};

/// Represents the various error reasons that can occur in the Narwhal protocol.
///
/// `ErrorReason` is an enumeration of all possible error codes that can be
/// returned by the protocol. Each variant corresponds to a specific error
/// condition with a unique string representation for protocol communication.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ErrorReason {
  /// The request is malformed or contains invalid parameters.
  BadRequest,
  /// The specified channel does not exist.
  ChannelNotFound,
  /// The channel has reached its maximum capacity and cannot accept new members.
  ChannelIsFull,
  /// The request is not allowed due to lack of permissions or privileges.
  Forbidden,
  /// An unexpected error occurred on the server side.
  InternalServerError,
  /// No error - used as a placeholder or default value.
  None,
  /// The request violates server policies or rules.
  PolicyViolation,
  /// The server is overloaded and cannot handle the request at this time.
  ServerOverloaded,
  /// The server is in the process of shutting down and not accepting new requests.
  ServerShuttingDown,
  /// The message channel buffer is full and cannot accept new messages.
  OutboundQueueIsFull,
  /// The requested operation is not allowed for the current user or context.
  NotAllowed,
  /// The channel does not have persistence enabled.
  PersistenceNotEnabled,
  /// The requested feature or operation is not yet implemented.
  NotImplemented,
  /// The operation timed out before completion.
  Timeout,
  /// The user/server is not authorized to perform the requested operation.
  Unauthorized,
  /// Received a message that was not expected in the current protocol state.
  UnexpectedMessage,
  /// The client's protocol version is not supported by the server.
  UnsupportedProtocolVersion,
  /// The user is already a member of the specified channel.
  UserInChannel,
  /// The user is not a member of the specified channel.
  UserNotInChannel,
  /// The requested username is already taken by another user.
  UsernameInUse,
  /// The user has not completed the registration process.
  UserNotRegistered,
  /// A server-wide resource limit has been reached (e.g., maximum number of channels).
  ResourceLimitReached,
  /// The response exceeds the maximum message size limit.
  ResponseTooLarge,
  /// POP attempted on an empty FIFO channel queue.
  QueueEmpty,
  /// PUSH attempted on a FIFO channel whose queue is at `max_persist_messages`.
  QueueFull,
  /// Operation requested on a channel whose declared type does not support it
  /// (e.g. BROADCAST on a FIFO channel, PUSH on a pub/sub channel).
  WrongType,
  /// FIFO data-plane operation on a channel whose head cursor failed recovery.
  /// Operator must restore `cursor.bin`, write a fresh sidecar, or DELETE the
  /// channel before normal operation can resume.
  CursorRecoveryRequired,
}

impl From<ErrorReason> for StringAtom {
  fn from(val: ErrorReason) -> Self {
    let s: &str = val.into();
    StringAtom::from(s)
  }
}

impl From<ErrorReason> for &str {
  fn from(val: ErrorReason) -> Self {
    match val {
      ErrorReason::BadRequest => "BAD_REQUEST",
      ErrorReason::ChannelNotFound => "CHANNEL_NOT_FOUND",
      ErrorReason::ChannelIsFull => "CHANNEL_IS_FULL",
      ErrorReason::Forbidden => "FORBIDDEN",
      ErrorReason::InternalServerError => "INTERNAL_SERVER_ERROR",
      ErrorReason::None => "NONE",
      ErrorReason::PolicyViolation => "POLICY_VIOLATION",
      ErrorReason::ServerOverloaded => "SERVER_OVERLOADED",
      ErrorReason::ServerShuttingDown => "SERVER_SHUTTING_DOWN",
      ErrorReason::OutboundQueueIsFull => "OUTBOUND_QUEUE_FULL",
      ErrorReason::NotAllowed => "NOT_ALLOWED",
      ErrorReason::PersistenceNotEnabled => "PERSISTENCE_NOT_ENABLED",
      ErrorReason::NotImplemented => "NOT_IMPLEMENTED",
      ErrorReason::Timeout => "TIMEOUT",
      ErrorReason::Unauthorized => "UNAUTHORIZED",
      ErrorReason::UnexpectedMessage => "UNEXPECTED_MESSAGE",
      ErrorReason::UnsupportedProtocolVersion => "UNSUPPORTED_PROTOCOL_VERSION",
      ErrorReason::UserInChannel => "USER_IN_CHANNEL",
      ErrorReason::UserNotInChannel => "USER_NOT_IN_CHANNEL",
      ErrorReason::UsernameInUse => "USERNAME_IN_USE",
      ErrorReason::UserNotRegistered => "USER_NOT_REGISTERED",
      ErrorReason::ResourceLimitReached => "RESOURCE_LIMIT_REACHED",
      ErrorReason::ResponseTooLarge => "RESPONSE_TOO_LARGE",
      ErrorReason::QueueEmpty => "QUEUE_EMPTY",
      ErrorReason::QueueFull => "QUEUE_FULL",
      ErrorReason::WrongType => "WRONG_TYPE",
      ErrorReason::CursorRecoveryRequired => "CURSOR_RECOVERY_REQUIRED",
    }
  }
}

impl TryFrom<StringAtom> for ErrorReason {
  type Error = anyhow::Error;

  fn try_from(atom: StringAtom) -> Result<Self, Self::Error> {
    ErrorReason::from_str(atom.as_ref())
  }
}

impl FromStr for ErrorReason {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "BAD_REQUEST" => Ok(ErrorReason::BadRequest),
      "CHANNEL_NOT_FOUND" => Ok(ErrorReason::ChannelNotFound),
      "CHANNEL_IS_FULL" => Ok(ErrorReason::ChannelIsFull),
      "INTERNAL_SERVER_ERROR" => Ok(ErrorReason::InternalServerError),
      "FORBIDDEN" => Ok(ErrorReason::Forbidden),
      "OUTBOUND_QUEUE_FULL" => Ok(ErrorReason::OutboundQueueIsFull),
      "NONE" => Ok(ErrorReason::None),
      "NOT_ALLOWED" => Ok(ErrorReason::NotAllowed),
      "PERSISTENCE_NOT_ENABLED" => Ok(ErrorReason::PersistenceNotEnabled),
      "NOT_IMPLEMENTED" => Ok(ErrorReason::NotImplemented),
      "POLICY_VIOLATION" => Ok(ErrorReason::PolicyViolation),
      "SERVER_OVERLOADED" => Ok(ErrorReason::ServerOverloaded),
      "SERVER_SHUTTING_DOWN" => Ok(ErrorReason::ServerShuttingDown),
      "SEND_CHANNEL_FULL" => Ok(ErrorReason::OutboundQueueIsFull),
      "TIMEOUT" => Ok(ErrorReason::Timeout),
      "UNAUTHORIZED" => Ok(ErrorReason::Unauthorized),
      "UNEXPECTED_MESSAGE" => Ok(ErrorReason::UnexpectedMessage),
      "UNSUPPORTED_PROTOCOL_VERSION" => Ok(ErrorReason::UnsupportedProtocolVersion),
      "USER_IN_CHANNEL" => Ok(ErrorReason::UserInChannel),
      "USER_NOT_IN_CHANNEL" => Ok(ErrorReason::UserNotInChannel),
      "USERNAME_IN_USE" => Ok(ErrorReason::UsernameInUse),
      "USER_NOT_REGISTERED" => Ok(ErrorReason::UserNotRegistered),
      "RESOURCE_LIMIT_REACHED" => Ok(ErrorReason::ResourceLimitReached),
      "RESPONSE_TOO_LARGE" => Ok(ErrorReason::ResponseTooLarge),
      "QUEUE_EMPTY" => Ok(ErrorReason::QueueEmpty),
      "QUEUE_FULL" => Ok(ErrorReason::QueueFull),
      "WRONG_TYPE" => Ok(ErrorReason::WrongType),
      "CURSOR_RECOVERY_REQUIRED" => Ok(ErrorReason::CursorRecoveryRequired),
      _ => anyhow::bail!("unknown error reason: {}", s),
    }
  }
}

impl std::fmt::Display for ErrorReason {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let s: &str = (*self).into();
    write!(f, "{}", s)
  }
}

/// Represents a protocol error with an optional correlation ID and detailed information.
#[derive(Clone, Debug)]
pub struct Error {
  /// The error correlation id.
  pub id: Option<u32>,

  /// The error reason.
  pub reason: ErrorReason,

  /// Optional detail information about the error.
  pub detail: Option<StringAtom>,
}

impl Error {
  /// Creates a new Error with the given reason and no ID or detail.
  pub fn new(reason: ErrorReason) -> Self {
    Self { id: None, reason, detail: None }
  }

  /// Sets the error correlation ID.
  pub fn with_id(mut self, id: u32) -> Self {
    self.id = Some(id);
    self
  }

  /// Sets the error detail message.
  pub fn with_detail(mut self, detail: impl Into<StringAtom>) -> Self {
    self.detail = Some(detail.into());
    self
  }

  /// Tells whether the error is recoverable.
  pub fn is_recoverable(&self) -> bool {
    matches!(
      self.reason,
      ErrorReason::ChannelIsFull
        | ErrorReason::ChannelNotFound
        | ErrorReason::Forbidden
        | ErrorReason::NotAllowed
        | ErrorReason::PersistenceNotEnabled
        | ErrorReason::NotImplemented
        | ErrorReason::UserInChannel
        | ErrorReason::UserNotInChannel
        | ErrorReason::UsernameInUse
        | ErrorReason::UserNotRegistered
        | ErrorReason::ServerOverloaded
        | ErrorReason::ResourceLimitReached
        | ErrorReason::ResponseTooLarge
        | ErrorReason::QueueEmpty
        | ErrorReason::QueueFull
        | ErrorReason::WrongType
        | ErrorReason::CursorRecoveryRequired
    )
  }
}

impl From<Error> for Message {
  fn from(val: Error) -> Self {
    Message::Error(ErrorParameters { id: val.id, reason: val.reason.into(), detail: val.detail.clone() })
  }
}

impl From<&Error> for Message {
  fn from(val: &Error) -> Self {
    Message::Error(ErrorParameters { id: val.id, reason: val.reason.into(), detail: val.detail.clone() })
  }
}

impl std::fmt::Display for Error {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let reason_str: &str = self.reason.into();

    if let Some(id) = self.id {
      write!(f, "[{}] {}", id, reason_str)?;
    } else {
      write!(f, "{}", reason_str)?;
    }

    if let Some(detail) = &self.detail {
      write!(f, ": {}", detail)?;
    }

    Ok(())
  }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
  use super::*;

  fn round_trip(reason: ErrorReason) {
    let s: &str = reason.into();
    let parsed = ErrorReason::from_str(s).unwrap();
    assert_eq!(parsed, reason);
  }

  #[test]
  fn round_trip_queue_empty() {
    round_trip(ErrorReason::QueueEmpty);
    let s: &str = ErrorReason::QueueEmpty.into();
    assert_eq!(s, "QUEUE_EMPTY");
  }

  #[test]
  fn round_trip_queue_full() {
    round_trip(ErrorReason::QueueFull);
    let s: &str = ErrorReason::QueueFull.into();
    assert_eq!(s, "QUEUE_FULL");
  }

  #[test]
  fn round_trip_wrong_type() {
    round_trip(ErrorReason::WrongType);
    let s: &str = ErrorReason::WrongType.into();
    assert_eq!(s, "WRONG_TYPE");
  }

  #[test]
  fn round_trip_cursor_recovery_required() {
    round_trip(ErrorReason::CursorRecoveryRequired);
    let s: &str = ErrorReason::CursorRecoveryRequired.into();
    assert_eq!(s, "CURSOR_RECOVERY_REQUIRED");
  }

  #[test]
  fn fifo_errors_are_recoverable() {
    for reason in
      [ErrorReason::QueueEmpty, ErrorReason::QueueFull, ErrorReason::WrongType, ErrorReason::CursorRecoveryRequired]
    {
      assert!(Error::new(reason).is_recoverable(), "{reason} should be recoverable");
    }
  }
}
