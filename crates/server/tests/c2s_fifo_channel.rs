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
  BroadcastParameters, ChannelSeqParameters, ErrorReason, EventKind, GetChannelConfigurationParameters,
  HistoryParameters, LeaveChannelParameters, ListChannelsParameters, Message, SetChannelConfigurationParameters,
};
use narwhal_server::channel::file_message_log::{FileMessageLogFactory, MessageLogMetrics};
use narwhal_server::channel::file_store::FileChannelStore;
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
