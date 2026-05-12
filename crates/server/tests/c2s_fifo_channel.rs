// SPDX-License-Identifier: BSD-3-Clause

//! Integration tests for the FIFO Channels transition slice (PR 1).
//!
//! Tests the in-runtime pub/sub → FIFO transition, restart-preserves-type,
//! type-aware WRONG_TYPE branches, owner-LEAVE-deletes, and the
//! CHANNEL_RECONFIGURED event. The data plane (PUSH/POP/GET_CHAN_LEN)
//! ships in PR 2.

use narwhal_client::S2mConfig;
use narwhal_client::compio::s2m::S2mClient;
use narwhal_common::core_dispatcher::CoreDispatcher;
use narwhal_modulator::create_s2m_listener;
use narwhal_modulator::modulator::AuthResult;
use narwhal_protocol::{
  AclAction, AclType, BroadcastParameters, ChannelSeqParameters, ErrorReason, EventKind,
  GetChannelConfigurationParameters, GetChannelLenParameters, HistoryParameters, LeaveChannelParameters,
  ListChannelsParameters, Message, PopParameters, PushParameters, SetChannelAclParameters,
  SetChannelConfigurationParameters,
};
use narwhal_server::channel::file_message_log::{FileMessageLogFactory, MessageLogMetrics};
use narwhal_server::channel::file_store::FileChannelStore;
use narwhal_server::channel::store::{ChannelStore, MessageLogFactory};
use narwhal_test_util::{C2sSuite, TestModulator, default_c2s_config, default_s2m_config};
use narwhal_util::string_atom::StringAtom;
use prometheus_client::registry::Registry;

const TEST_USER_1: &str = "test_user_1";
const TEST_USER_2: &str = "test_user_2";
const SHARED_SECRET: &str = "test_secret";
const CHANNEL: &str = "!fifotest@localhost";

fn make_auth_modulator() -> TestModulator {
  TestModulator::new()
    .with_auth_handler(|token| async move { Ok(AuthResult::Success { username: StringAtom::from(token.as_ref()) }) })
}

async fn bootstrap_s2m(
  modulator: TestModulator,
) -> anyhow::Result<(S2mClient, narwhal_modulator::S2mListener<TestModulator>, CoreDispatcher)> {
  let mut core_dispatcher = CoreDispatcher::new(1);
  core_dispatcher.bootstrap().await?;

  let mut s2m_ln = create_s2m_listener(
    default_s2m_config(SHARED_SECRET),
    modulator,
    core_dispatcher.clone(),
    &mut Registry::default(),
  )
  .await?;
  s2m_ln.bootstrap().await?;

  let s2m_client = S2mClient::new(S2mConfig {
    address: s2m_ln.local_address().unwrap().to_string(),
    shared_secret: SHARED_SECRET.to_string(),
    ..Default::default()
  })?;

  Ok((s2m_client, s2m_ln, core_dispatcher))
}

fn set_config(
  id: u32,
  channel: &str,
  persist: Option<bool>,
  max_persist_messages: Option<u32>,
  r#type: Option<&str>,
) -> Message {
  Message::SetChannelConfiguration(SetChannelConfigurationParameters {
    id,
    channel: StringAtom::from(channel),
    persist,
    max_persist_messages,
    r#type: r#type.map(StringAtom::from),
    ..Default::default()
  })
}

/// Transition validation: persist=false must reject the FIFO request.
#[compio::test]
async fn test_fifo_transition_rejects_non_persistent() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;

  // Try to declare FIFO without persist=true.
  suite.write_message(TEST_USER_1, set_config(99, CHANNEL, None, Some(10), Some("fifo"))).await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(99));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::BadRequest));
      assert_eq!(p.detail.as_deref(), Some("FIFO requires persist=true and max_persist_messages > 0"));
    },
    other => panic!("expected Error, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// Transition validation: max_persist_messages=0 must reject. The generic
/// `max_persist_messages > 0` rule fires before the FIFO-specific check
/// when persist=true is also requested.
#[compio::test]
async fn test_fifo_transition_rejects_zero_max_persist_messages() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;

  suite.write_message(TEST_USER_1, set_config(11, CHANNEL, Some(true), Some(0), Some("fifo"))).await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(11));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::BadRequest));
    },
    other => panic!("expected Error, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// FIFO is one-way: rejecting type=pubsub on a channel that is already FIFO.
#[compio::test]
async fn test_fifo_to_pubsub_transition_rejected() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;

  // Become FIFO.
  suite.write_message(TEST_USER_1, set_config(1, CHANNEL, Some(true), Some(10), Some("fifo"))).await?;
  let reply = suite.read_message(TEST_USER_1).await?;
  assert!(matches!(reply, Message::SetChannelConfigurationAck { .. }), "unexpected reply: {:?}", reply);

  // Try to revert.
  suite.write_message(TEST_USER_1, set_config(2, CHANNEL, None, None, Some("pubsub"))).await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(2));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::BadRequest));
      assert_eq!(p.detail.as_deref(), Some("FIFO is one-way; cannot revert to pubsub"));
    },
    other => panic!("expected Error, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// Restart preserves channel_type=Fifo: after a transition, restart the
/// server and verify GET_CHAN_CONFIG returns type=fifo.
#[compio::test]
async fn test_fifo_transition_persists_across_restart() -> anyhow::Result<()> {
  let tmp = tempfile::tempdir()?;
  let data_dir = tmp.path().to_path_buf();

  // First boot: transition to FIFO.
  {
    let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
    let store = FileChannelStore::new(data_dir.clone()).await?;
    let mlf = FileMessageLogFactory::new(
      data_dir.clone(),
      default_c2s_config().limits.max_payload_size,
      MessageLogMetrics::noop(),
    );

    let mut suite =
      C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
    suite.setup().await?;

    suite.auth(TEST_USER_1, TEST_USER_1).await?;
    suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
    suite.write_message(TEST_USER_1, set_config(1, CHANNEL, Some(true), Some(10), Some("fifo"))).await?;
    assert!(matches!(suite.read_message(TEST_USER_1).await?, Message::SetChannelConfigurationAck { .. }));

    suite.teardown().await?;
    s2m_ln.shutdown().await?;
    s2m_dispatcher.shutdown().await?;
  }

  // Second boot: type=fifo on GET_CHAN_CONFIG.
  {
    let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
    let store = FileChannelStore::new(data_dir.clone()).await?;
    let mlf = FileMessageLogFactory::new(
      data_dir.clone(),
      default_c2s_config().limits.max_payload_size,
      MessageLogMetrics::noop(),
    );

    let mut suite =
      C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
    suite.setup().await?;

    suite.auth(TEST_USER_1, TEST_USER_1).await?;
    suite
      .write_message(
        TEST_USER_1,
        Message::GetChannelConfiguration(GetChannelConfigurationParameters {
          id: 5,
          channel: StringAtom::from(CHANNEL),
        }),
      )
      .await?;

    let reply = suite.read_message(TEST_USER_1).await?;
    if let Message::ChannelConfiguration(p) = reply {
      assert_eq!(p.r#type.as_ref(), "fifo");
      assert!(p.persist);
    } else {
      panic!("expected ChannelConfiguration, got {:?}", reply);
    }

    suite.teardown().await?;
    s2m_ln.shutdown().await?;
    s2m_dispatcher.shutdown().await?;
  }

  Ok(())
}

/// BROADCAST on a FIFO channel returns WRONG_TYPE.
#[compio::test]
async fn test_broadcast_on_fifo_returns_wrong_type() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
  suite.write_message(TEST_USER_1, set_config(1, CHANNEL, Some(true), Some(10), Some("fifo"))).await?;
  let reply = suite.read_message(TEST_USER_1).await?;
  assert!(matches!(reply, Message::SetChannelConfigurationAck { .. }), "unexpected reply: {:?}", reply);

  // Try BROADCAST.
  suite
    .write_message(
      TEST_USER_1,
      Message::Broadcast(BroadcastParameters { id: 7, channel: StringAtom::from(CHANNEL), qos: None, length: 1 }),
    )
    .await?;
  // The codec frames the payload as `bytes + \n` after the BROADCAST header.
  suite.write_raw_bytes(TEST_USER_1, b"x").await?;
  suite.write_raw_bytes(TEST_USER_1, b"\n").await?;

  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(7));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::WrongType));
    },
    other => panic!("expected Error, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// HISTORY on a FIFO channel returns WRONG_TYPE.
#[compio::test]
async fn test_history_on_fifo_returns_wrong_type() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
  suite.write_message(TEST_USER_1, set_config(1, CHANNEL, Some(true), Some(10), Some("fifo"))).await?;
  let reply = suite.read_message(TEST_USER_1).await?;
  assert!(matches!(reply, Message::SetChannelConfigurationAck { .. }), "unexpected reply: {:?}", reply);

  suite
    .write_message(
      TEST_USER_1,
      Message::History(HistoryParameters {
        id: 8,
        history_id: StringAtom::from("h1"),
        channel: StringAtom::from(CHANNEL),
        from_seq: 1,
        limit: 10,
      }),
    )
    .await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(8));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::WrongType));
    },
    other => panic!("expected Error, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// CHAN_SEQ on a FIFO channel returns WRONG_TYPE.
#[compio::test]
async fn test_chan_seq_on_fifo_returns_wrong_type() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
  suite.write_message(TEST_USER_1, set_config(1, CHANNEL, Some(true), Some(10), Some("fifo"))).await?;
  let reply = suite.read_message(TEST_USER_1).await?;
  assert!(matches!(reply, Message::SetChannelConfigurationAck { .. }), "unexpected reply: {:?}", reply);

  suite
    .write_message(TEST_USER_1, Message::ChannelSeq(ChannelSeqParameters { id: 9, channel: StringAtom::from(CHANNEL) }))
    .await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(9));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::WrongType));
    },
    other => panic!("expected Error, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// Owner LEAVE on a FIFO channel auto-deletes: other members receive
/// CHANNEL_DELETED, the leaver receives LEAVE_ACK, and the channel
/// disappears from `CHANNELS`.
#[compio::test]
async fn test_owner_leave_on_fifo_auto_deletes() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
  suite.join_channel(TEST_USER_2, CHANNEL, None).await?;
  // user_1 sees user_2 join.
  suite.ignore_reply(TEST_USER_1).await?;

  suite.write_message(TEST_USER_1, set_config(1, CHANNEL, Some(true), Some(10), Some("fifo"))).await?;
  let reply = suite.read_message(TEST_USER_1).await?;
  assert!(matches!(reply, Message::SetChannelConfigurationAck { .. }), "unexpected reply: {:?}", reply);
  // user_2 receives CHANNEL_RECONFIGURED.
  suite.ignore_reply(TEST_USER_2).await?;

  // Owner LEAVEs.
  suite
    .write_message(
      TEST_USER_1,
      Message::LeaveChannel(LeaveChannelParameters { id: 22, channel: StringAtom::from(CHANNEL), on_behalf: None }),
    )
    .await?;

  // user_1 receives LEAVE_ACK (matches request shape, not DELETE_ACK).
  let reply = suite.read_message(TEST_USER_1).await?;
  match reply {
    Message::LeaveChannelAck(p) => assert_eq!(p.id, 22),
    other => panic!("expected LeaveChannelAck, got {:?}", other),
  }

  // user_2 receives CHANNEL_DELETED.
  match suite.read_message(TEST_USER_2).await? {
    Message::Event(p) => {
      assert_eq!(p.kind, StringAtom::from(EventKind::ChannelDeleted));
      assert_eq!(p.channel.as_deref(), Some(CHANNEL));
    },
    other => panic!("expected Event, got {:?}", other),
  }

  // Channel is gone from user_1's listing.
  suite
    .write_message(
      TEST_USER_1,
      Message::ListChannels(ListChannelsParameters { id: 30, page: None, page_size: None, owner: false }),
    )
    .await?;
  let listing = suite.read_message(TEST_USER_1).await?;
  match listing {
    Message::ListChannelsAck(p) => {
      assert!(!p.channels.iter().any(|c| c.as_ref() == CHANNEL), "channel should be gone, got: {:?}", p.channels);
    },
    other => panic!("expected ListChannelsAck, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// CHANNEL_RECONFIGURED fires on the type transition.
#[compio::test]
async fn test_channel_reconfigured_fires_on_type_change() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
  suite.join_channel(TEST_USER_2, CHANNEL, None).await?;
  // user_1 sees user_2 join.
  suite.ignore_reply(TEST_USER_1).await?;

  suite.write_message(TEST_USER_1, set_config(40, CHANNEL, Some(true), Some(10), Some("fifo"))).await?;
  // user_1 receives ACK.
  assert!(matches!(suite.read_message(TEST_USER_1).await?, Message::SetChannelConfigurationAck { .. }));

  // user_2 receives CHANNEL_RECONFIGURED (and not the ACK or anything else).
  match suite.read_message(TEST_USER_2).await? {
    Message::Event(p) => {
      assert_eq!(p.kind, StringAtom::from(EventKind::ChannelReconfigured));
      assert_eq!(p.channel.as_deref(), Some(CHANNEL));
    },
    other => panic!("expected Event, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// CHANNEL_RECONFIGURED fires on a config-only change (e.g. max_clients).
#[compio::test]
async fn test_channel_reconfigured_fires_on_config_only_change() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
  suite.join_channel(TEST_USER_2, CHANNEL, None).await?;
  suite.ignore_reply(TEST_USER_1).await?;

  // Change max_clients only.
  suite
    .write_message(
      TEST_USER_1,
      Message::SetChannelConfiguration(SetChannelConfigurationParameters {
        id: 50,
        channel: StringAtom::from(CHANNEL),
        max_clients: Some(33),
        ..Default::default()
      }),
    )
    .await?;
  assert!(matches!(suite.read_message(TEST_USER_1).await?, Message::SetChannelConfigurationAck { .. }));

  match suite.read_message(TEST_USER_2).await? {
    Message::Event(p) => {
      assert_eq!(p.kind, StringAtom::from(EventKind::ChannelReconfigured));
      assert_eq!(p.channel.as_deref(), Some(CHANNEL));
    },
    other => panic!("expected Event, got {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// CHANNEL_RECONFIGURED is suppressed on a true no-op (identical config
/// re-submission with no type change).
#[compio::test]
async fn test_channel_reconfigured_suppressed_on_no_op() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
  suite.join_channel(TEST_USER_2, CHANNEL, None).await?;
  suite.ignore_reply(TEST_USER_1).await?;

  // First change: bumps max_clients to 33; user_2 gets CHANNEL_RECONFIGURED.
  suite
    .write_message(
      TEST_USER_1,
      Message::SetChannelConfiguration(SetChannelConfigurationParameters {
        id: 60,
        channel: StringAtom::from(CHANNEL),
        max_clients: Some(33),
        ..Default::default()
      }),
    )
    .await?;
  assert!(matches!(suite.read_message(TEST_USER_1).await?, Message::SetChannelConfigurationAck { .. }));
  suite.ignore_reply(TEST_USER_2).await?;

  // Second submission: same max_clients, so it's a no-op.
  suite
    .write_message(
      TEST_USER_1,
      Message::SetChannelConfiguration(SetChannelConfigurationParameters {
        id: 61,
        channel: StringAtom::from(CHANNEL),
        max_clients: Some(33),
        ..Default::default()
      }),
    )
    .await?;
  assert!(matches!(suite.read_message(TEST_USER_1).await?, Message::SetChannelConfigurationAck { .. }));

  // user_2 must NOT receive a second CHANNEL_RECONFIGURED.
  suite.expect_read_timeout(TEST_USER_2, std::time::Duration::from_millis(500)).await?;

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

// ---------- PR 2 data-plane tests ----------

/// Writes `PUSH id length` + payload + trailing newline. Caller awaits the
/// `PUSH_ACK` separately (so test code can also assert errors).
async fn write_push<CS: ChannelStore, MLF: MessageLogFactory>(
  suite: &mut C2sSuite<CS, MLF>,
  user: &str,
  channel: &str,
  id: u32,
  payload: &[u8],
) -> anyhow::Result<()> {
  suite
    .write_message(
      user,
      Message::Push(PushParameters { id, channel: StringAtom::from(channel), length: payload.len() as u32 }),
    )
    .await?;
  suite.write_raw_bytes(user, payload).await?;
  suite.write_raw_bytes(user, b"\n").await?;
  Ok(())
}

/// Sends a PUSH and asserts a PUSH_ACK with the same correlation id.
async fn push_ok<CS: ChannelStore, MLF: MessageLogFactory>(
  suite: &mut C2sSuite<CS, MLF>,
  user: &str,
  channel: &str,
  id: u32,
  payload: &[u8],
) -> anyhow::Result<()> {
  write_push(suite, user, channel, id, payload).await?;
  match suite.read_message(user).await? {
    Message::PushAck(p) => {
      assert_eq!(p.id, id);
      Ok(())
    },
    other => panic!("expected PushAck, got: {:?}", other),
  }
}

/// Sends a POP and reads back the POP_ACK + payload + trailing newline.
/// Returns the payload and the entry's PUSH timestamp.
async fn pop_ok<CS: ChannelStore, MLF: MessageLogFactory>(
  suite: &mut C2sSuite<CS, MLF>,
  user: &str,
  channel: &str,
  id: u32,
) -> anyhow::Result<(Vec<u8>, u64)> {
  suite.write_message(user, Message::Pop(PopParameters { id, channel: StringAtom::from(channel) })).await?;
  match suite.read_message(user).await? {
    Message::PopAck(p) => {
      assert_eq!(p.id, id);
      let mut buf = vec![0u8; p.length as usize];
      suite.read_raw_bytes(user, &mut buf).await?;
      let mut nl = [0u8; 1];
      suite.read_raw_bytes(user, &mut nl).await?;
      assert_eq!(nl[0], b'\n', "payload not followed by newline");
      Ok((buf, p.timestamp))
    },
    other => panic!("expected PopAck, got: {:?}", other),
  }
}

/// Sends a GET_CHAN_LEN and reads back the CHAN_LEN count.
async fn get_chan_len_ok<CS: ChannelStore, MLF: MessageLogFactory>(
  suite: &mut C2sSuite<CS, MLF>,
  user: &str,
  channel: &str,
  id: u32,
) -> anyhow::Result<u32> {
  suite
    .write_message(user, Message::GetChannelLen(GetChannelLenParameters { id, channel: StringAtom::from(channel) }))
    .await?;
  match suite.read_message(user).await? {
    Message::ChannelLen(p) => {
      assert_eq!(p.id, id);
      assert_eq!(p.channel.as_ref(), channel);
      Ok(p.count)
    },
    other => panic!("expected ChannelLen, got: {:?}", other),
  }
}

/// Auths user_1, JOINs `CHANNEL`, transitions to FIFO with the given
/// `max_persist_messages` and optional `message_flush_interval`. Drains
/// the SetChannelConfigurationAck.
async fn bootstrap_fifo<CS: ChannelStore, MLF: MessageLogFactory>(
  suite: &mut C2sSuite<CS, MLF>,
  max_persist_messages: u32,
  flush_interval: Option<u32>,
) -> anyhow::Result<()> {
  suite.auth(TEST_USER_1, TEST_USER_1).await?;
  suite.join_channel(TEST_USER_1, CHANNEL, None).await?;
  suite
    .write_message(
      TEST_USER_1,
      Message::SetChannelConfiguration(SetChannelConfigurationParameters {
        id: 555,
        channel: StringAtom::from(CHANNEL),
        persist: Some(true),
        max_persist_messages: Some(max_persist_messages),
        message_flush_interval: flush_interval,
        r#type: Some(StringAtom::from("fifo")),
        ..Default::default()
      }),
    )
    .await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::SetChannelConfigurationAck(p) => assert_eq!(p.id, 555),
    other => panic!("expected SetChannelConfigurationAck, got: {:?}", other),
  }
  Ok(())
}

/// PUSH then POP roundtrip: payload and timestamp survive intact.
#[compio::test]
async fn test_push_pop_roundtrip() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 10, None).await?;

  let before_push = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64;
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 1, b"hello fifo").await?;
  let (payload, timestamp) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 2).await?;
  assert_eq!(payload, b"hello fifo");
  assert!(timestamp >= before_push, "POP timestamp ({}) must be >= pre-push wall time ({})", timestamp, before_push);

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// POP against an empty FIFO returns `QUEUE_EMPTY`.
#[compio::test]
async fn test_pop_empty_queue_returns_queue_empty() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 10, None).await?;

  suite.write_message(TEST_USER_1, Message::Pop(PopParameters { id: 7, channel: StringAtom::from(CHANNEL) })).await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(7));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::QueueEmpty));
    },
    other => panic!("expected Error(QUEUE_EMPTY), got: {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// Queue depth is computed from the cursor and the log's tail, not from the
/// physical entry count. PUSH N up to the cap, POP one, PUSH one more must
/// succeed; PUSH again must fail with `QUEUE_FULL`. This locks the
/// logical-depth contract that survives once head-eviction kicks in.
#[compio::test]
async fn test_push_queue_full_logical_depth() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 3, None).await?;

  push_ok(&mut suite, TEST_USER_1, CHANNEL, 1, b"a").await?;
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 2, b"b").await?;
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 3, b"c").await?;

  // Fourth push must be QUEUE_FULL.
  write_push(&mut suite, TEST_USER_1, CHANNEL, 4, b"d").await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(4));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::QueueFull));
    },
    other => panic!("expected Error(QUEUE_FULL), got: {:?}", other),
  }

  // Drain one and verify PUSH succeeds again under the cap. depth=2 < 3.
  let (got, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 5).await?;
  assert_eq!(got, b"a");
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 6, b"d").await?;

  // depth back to 3 (b, c, d); another push must again be QUEUE_FULL.
  write_push(&mut suite, TEST_USER_1, CHANNEL, 7, b"e").await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(7));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::QueueFull));
    },
    other => panic!("expected Error(QUEUE_FULL), got: {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// PUSH from a non-owner member returns `FORBIDDEN`.
#[compio::test]
async fn test_push_non_owner_returns_forbidden() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 10, None).await?;
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.join_channel(TEST_USER_2, CHANNEL, None).await?;
  // owner drains MEMBER_JOINED.
  suite.ignore_reply(TEST_USER_1).await?;

  write_push(&mut suite, TEST_USER_2, CHANNEL, 1, b"unauthorized").await?;
  match suite.read_message(TEST_USER_2).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(1));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::Forbidden));
    },
    other => panic!("expected Error(FORBIDDEN), got: {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// POP from an authenticated user that has not JOINed the channel returns
/// `USER_NOT_IN_CHANNEL`.
#[compio::test]
async fn test_pop_non_member_returns_user_not_in_channel() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 10, None).await?;
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 1, b"data").await?;

  // user_2 auths but never JOINs.
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.write_message(TEST_USER_2, Message::Pop(PopParameters { id: 9, channel: StringAtom::from(CHANNEL) })).await?;
  match suite.read_message(TEST_USER_2).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(9));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::UserNotInChannel));
    },
    other => panic!("expected Error(USER_NOT_IN_CHANNEL), got: {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// POP from a JOINed member that the read ACL excludes returns `NOT_ALLOWED`.
#[compio::test]
async fn test_pop_read_acl_denied_returns_not_allowed() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 10, None).await?;
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.join_channel(TEST_USER_2, CHANNEL, None).await?;
  // owner drains MEMBER_JOINED.
  suite.ignore_reply(TEST_USER_1).await?;

  // Owner sets the read ACL to allow only itself.
  suite
    .write_message(
      TEST_USER_1,
      Message::SetChannelAcl(SetChannelAclParameters {
        id: 100,
        channel: StringAtom::from(CHANNEL),
        r#type: StringAtom::from(AclType::Read.as_str()),
        action: StringAtom::from(AclAction::Add.as_str()),
        nids: vec![StringAtom::from(format!("{}@localhost", TEST_USER_1))],
      }),
    )
    .await?;
  match suite.read_message(TEST_USER_1).await? {
    Message::SetChannelAclAck(p) => assert_eq!(p.id, 100),
    other => panic!("expected SetChannelAclAck, got: {:?}", other),
  }

  push_ok(&mut suite, TEST_USER_1, CHANNEL, 1, b"secret").await?;

  // user_2 attempts POP and gets denied.
  suite.write_message(TEST_USER_2, Message::Pop(PopParameters { id: 9, channel: StringAtom::from(CHANNEL) })).await?;
  match suite.read_message(TEST_USER_2).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(9));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::NotAllowed));
    },
    other => panic!("expected Error(NOT_ALLOWED), got: {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// GET_CHAN_LEN reflects PUSH/POP activity: starts at 0, grows with PUSH,
/// shrinks with POP.
#[compio::test]
async fn test_get_chan_len_returns_queue_depth() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 10, None).await?;
  assert_eq!(get_chan_len_ok(&mut suite, TEST_USER_1, CHANNEL, 10).await?, 0);

  push_ok(&mut suite, TEST_USER_1, CHANNEL, 11, b"x").await?;
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 12, b"y").await?;
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 13, b"z").await?;
  assert_eq!(get_chan_len_ok(&mut suite, TEST_USER_1, CHANNEL, 14).await?, 3);

  let (_, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 15).await?;
  assert_eq!(get_chan_len_ok(&mut suite, TEST_USER_1, CHANNEL, 16).await?, 2);

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// GET_CHAN_LEN from a non-owner member returns `FORBIDDEN`.
#[compio::test]
async fn test_get_chan_len_non_owner_returns_forbidden() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 10, None).await?;
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.join_channel(TEST_USER_2, CHANNEL, None).await?;
  // owner drains MEMBER_JOINED.
  suite.ignore_reply(TEST_USER_1).await?;

  suite
    .write_message(
      TEST_USER_2,
      Message::GetChannelLen(GetChannelLenParameters { id: 9, channel: StringAtom::from(CHANNEL) }),
    )
    .await?;
  match suite.read_message(TEST_USER_2).await? {
    Message::Error(p) => {
      assert_eq!(p.id, Some(9));
      assert_eq!(p.reason, StringAtom::from(ErrorReason::Forbidden));
    },
    other => panic!("expected Error(FORBIDDEN), got: {:?}", other),
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// Competing consumers: two clients POPing the same FIFO each receive a
/// distinct element. The per-channel actor serializes POPs and the cursor
/// advances exactly once per success, so no element is delivered twice.
#[compio::test]
async fn test_competing_consumers_each_get_unique_element() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  bootstrap_fifo(&mut suite, 10, None).await?;
  suite.auth(TEST_USER_2, TEST_USER_2).await?;
  suite.join_channel(TEST_USER_2, CHANNEL, None).await?;
  suite.ignore_reply(TEST_USER_1).await?;

  push_ok(&mut suite, TEST_USER_1, CHANNEL, 1, b"alpha").await?;
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 2, b"beta").await?;
  push_ok(&mut suite, TEST_USER_1, CHANNEL, 3, b"gamma").await?;

  // Each consumer pops once; collect what they got.
  let (a, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 10).await?;
  let (b, _) = pop_ok(&mut suite, TEST_USER_2, CHANNEL, 20).await?;
  let (c, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 11).await?;

  let mut seen = vec![a, b, c];
  seen.sort();
  assert_eq!(seen, vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]);

  // No element remains; both consumers see QUEUE_EMPTY now.
  for user in [TEST_USER_1, TEST_USER_2] {
    suite.write_message(user, Message::Pop(PopParameters { id: 99, channel: StringAtom::from(CHANNEL) })).await?;
    match suite.read_message(user).await? {
      Message::Error(p) => {
        assert_eq!(p.reason, StringAtom::from(ErrorReason::QueueEmpty));
      },
      other => panic!("expected Error(QUEUE_EMPTY), got: {:?}", other),
    }
  }

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// Cursor advance survives restart: after PUSH N and POP K, restart and verify
/// POPs deliver the remaining N - K entries in order. Combined with the
/// per-POP cursor fsync, this exercises the no-duplication contract: an
/// already-consumed entry must not surface again.
#[compio::test]
async fn test_pop_durable_across_restart() -> anyhow::Result<()> {
  let tmp = tempfile::tempdir()?;
  let data_dir = tmp.path().to_path_buf();

  // First boot: PUSH 3, POP 1.
  {
    let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
    let store = FileChannelStore::new(data_dir.clone()).await?;
    let mlf = FileMessageLogFactory::new(
      data_dir.clone(),
      default_c2s_config().limits.max_payload_size,
      MessageLogMetrics::noop(),
    );

    let mut suite =
      C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
    suite.setup().await?;

    bootstrap_fifo(&mut suite, 10, None).await?;
    push_ok(&mut suite, TEST_USER_1, CHANNEL, 1, b"one").await?;
    push_ok(&mut suite, TEST_USER_1, CHANNEL, 2, b"two").await?;
    push_ok(&mut suite, TEST_USER_1, CHANNEL, 3, b"three").await?;

    let (first, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 10).await?;
    assert_eq!(first, b"one");

    suite.teardown().await?;
    s2m_ln.shutdown().await?;
    s2m_dispatcher.shutdown().await?;
  }

  // Second boot: re-auth and POP twice. The consumed "one" must NOT
  // resurface; we must receive "two" and "three" in order.
  {
    let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
    let store = FileChannelStore::new(data_dir.clone()).await?;
    let mlf = FileMessageLogFactory::new(
      data_dir.clone(),
      default_c2s_config().limits.max_payload_size,
      MessageLogMetrics::noop(),
    );

    let mut suite =
      C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
    suite.setup().await?;

    suite.auth(TEST_USER_1, TEST_USER_1).await?;

    let (second, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 20).await?;
    assert_eq!(second, b"two", "restart must skip consumed entry; cursor advance was not durable");
    let (third, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 21).await?;
    assert_eq!(third, b"three");

    // Queue is drained.
    suite
      .write_message(TEST_USER_1, Message::Pop(PopParameters { id: 22, channel: StringAtom::from(CHANNEL) }))
      .await?;
    match suite.read_message(TEST_USER_1).await? {
      Message::Error(p) => assert_eq!(p.reason, StringAtom::from(ErrorReason::QueueEmpty)),
      other => panic!("expected Error(QUEUE_EMPTY), got: {:?}", other),
    }

    suite.teardown().await?;
    s2m_ln.shutdown().await?;
    s2m_dispatcher.shutdown().await?;
  }

  Ok(())
}

/// POP forces a synchronous log flush when the entry is not yet durable.
/// With a long `message_flush_interval` (60s), a PUSH followed by an
/// immediate POP must complete well below the flush interval, proving that
/// POP triggered the flush rather than waiting on the periodic flusher.
#[compio::test]
async fn test_pop_force_flushes_log_when_not_durable() -> anyhow::Result<()> {
  let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  // 60s flush interval: only a force-flush on POP will land the entry.
  bootstrap_fifo(&mut suite, 10, Some(60_000)).await?;

  push_ok(&mut suite, TEST_USER_1, CHANNEL, 1, b"durable-on-pop").await?;

  let start = std::time::Instant::now();
  let (payload, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 2).await?;
  let elapsed = start.elapsed();

  assert_eq!(payload, b"durable-on-pop");
  assert!(
    elapsed < std::time::Duration::from_secs(5),
    "POP did not force-flush; took {:?} with a 60s flush interval",
    elapsed
  );

  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

/// With the default `message_flush_interval=0`, PUSH is durable before
/// PUSH_ACK is sent. Verify by PUSHing on the first boot and POPing on the
/// second boot without any explicit flush between them.
#[compio::test]
async fn test_push_durable_with_zero_flush_interval() -> anyhow::Result<()> {
  let tmp = tempfile::tempdir()?;
  let data_dir = tmp.path().to_path_buf();

  // First boot: PUSH only.
  {
    let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
    let store = FileChannelStore::new(data_dir.clone()).await?;
    let mlf = FileMessageLogFactory::new(
      data_dir.clone(),
      default_c2s_config().limits.max_payload_size,
      MessageLogMetrics::noop(),
    );

    let mut suite =
      C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
    suite.setup().await?;

    bootstrap_fifo(&mut suite, 10, Some(0)).await?;
    push_ok(&mut suite, TEST_USER_1, CHANNEL, 1, b"pre-restart").await?;

    suite.teardown().await?;
    s2m_ln.shutdown().await?;
    s2m_dispatcher.shutdown().await?;
  }

  // Second boot: POP must return the pre-restart payload.
  {
    let (s2m_client, mut s2m_ln, mut s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
    let store = FileChannelStore::new(data_dir.clone()).await?;
    let mlf = FileMessageLogFactory::new(
      data_dir.clone(),
      default_c2s_config().limits.max_payload_size,
      MessageLogMetrics::noop(),
    );

    let mut suite =
      C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
    suite.setup().await?;

    suite.auth(TEST_USER_1, TEST_USER_1).await?;
    let (payload, _) = pop_ok(&mut suite, TEST_USER_1, CHANNEL, 10).await?;
    assert_eq!(payload, b"pre-restart");

    suite.teardown().await?;
    s2m_ln.shutdown().await?;
    s2m_dispatcher.shutdown().await?;
  }

  Ok(())
}
