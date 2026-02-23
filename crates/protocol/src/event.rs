// SPDX-License-Identifier: BSD-3-Clause

use std::str::FromStr;

use crate::{EventParameters, Message};
use narwhal_util::string_atom::StringAtom;

/// Represents the type of event that occurred in the system.
///
/// `EventKind` is used to categorize different types of events that can occur
/// within channels or the broader system. Each variant corresponds to a specific
/// type of activity or state change.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EventKind {
  /// Indicates that a member has joined a channel.
  ///
  /// This event is triggered when a user successfully joins a channel,
  /// either through direct action or being added by another member with
  /// appropriate permissions.
  MemberJoined,

  /// Indicates that a member has left a channel.
  ///
  /// This event is triggered when a user leaves a channel, either voluntarily
  /// or due to being removed by another member with appropriate permissions.
  MemberLeft,

  /// Indicates that a channel has been deleted.
  ///
  /// This event is triggered when the channel owner explicitly deletes the
  /// channel. All members are removed and the channel is destroyed.
  ChannelDeleted,
}

impl std::fmt::Display for EventKind {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let s: &str = (*self).into();
    write!(f, "{}", s)
  }
}

impl From<EventKind> for StringAtom {
  fn from(val: EventKind) -> Self {
    let s: &str = val.into();
    StringAtom::from(s)
  }
}

impl From<EventKind> for &str {
  fn from(val: EventKind) -> Self {
    match val {
      EventKind::MemberJoined => "MEMBER_JOINED",
      EventKind::MemberLeft => "MEMBER_LEFT",
      EventKind::ChannelDeleted => "CHANNEL_DELETED",
    }
  }
}

impl TryFrom<StringAtom> for EventKind {
  type Error = anyhow::Error;

  fn try_from(atom: StringAtom) -> Result<Self, Self::Error> {
    EventKind::from_str(atom.as_ref())
  }
}

impl FromStr for EventKind {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "MEMBER_JOINED" => Ok(EventKind::MemberJoined),
      "MEMBER_LEFT" => Ok(EventKind::MemberLeft),
      "CHANNEL_DELETED" => Ok(EventKind::ChannelDeleted),
      _ => anyhow::bail!("unknown event kind: {}", s),
    }
  }
}

/// Represents an event that can be processed by a modulator.
///
/// Events are used to communicate information about various activities or state changes
/// that occur within the system. Each event contains metadata about the event type,
/// associated entities, and contextual information.
#[derive(Clone, Debug)]
pub struct Event {
  /// The event kind.
  pub kind: EventKind,

  /// The channel associated with the event, if any.
  pub channel: Option<StringAtom>,

  /// The nid associated with the event, if any.
  pub nid: Option<StringAtom>,

  /// Tells whether the nid is the channel owner.
  pub owner: Option<bool>,
}

impl Event {
  /// Creates a new event with the given kind.
  pub fn new(kind: EventKind) -> Self {
    Event { kind, channel: None, nid: None, owner: None }
  }

  /// Sets the channel for the event.
  pub fn with_channel(mut self, channel: StringAtom) -> Self {
    self.channel = Some(channel);
    self
  }

  /// Sets the nid for the event.
  pub fn with_nid(mut self, nid: StringAtom) -> Self {
    self.nid = Some(nid);
    self
  }

  /// Sets the owner flag for the event.
  pub fn with_owner(mut self, owner: bool) -> Self {
    self.owner = Some(owner);
    self
  }
}

impl From<Event> for Message {
  fn from(val: Event) -> Self {
    Message::Event(EventParameters {
      kind: val.kind.into(),
      channel: val.channel.clone(),
      nid: val.nid.clone(),
      owner: val.owner,
    })
  }
}
