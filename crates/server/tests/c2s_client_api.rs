// SPDX-License-Identifier: BSD-3-Clause

//! Happy-path coverage for the C2S client crate's public methods, exercised
//! end-to-end against a real `narwhal-server` instance bootstrapped via
//! `narwhal-test-util::C2sSuite`. The suite's raw test connections are not
//! used here; instead each test spins up a `narwhal_client::compio::c2s::C2sClient`
//! that connects to the suite-managed listener address.
//!
//! Scope: each test verifies that the client correctly encodes the request
//! and parses the ACK / response. Error-path coverage is provided by the
//! server-side handler tests in `crates/server/tests/c2s_tests.rs` and friends.

use std::sync::Arc;
use std::time::Duration;

use narwhal_client::S2mConfig;
use narwhal_client::compio::c2s::C2sClient;
use narwhal_client::compio::s2m::S2mClient;
use narwhal_client::{AuthMethod, Authenticator, AuthenticatorFactory, C2sConfig};
use narwhal_common::core_dispatcher::CoreDispatcher;
use narwhal_modulator::create_s2m_listener;
use narwhal_modulator::modulator::AuthResult;
use narwhal_protocol::{AclAction, AclType};
use narwhal_server::channel::file_message_log::{FileMessageLogFactory, MessageLogMetrics};
use narwhal_server::channel::file_store::FileChannelStore;
use narwhal_test_util::{C2sSuite, TestModulator, default_c2s_config, default_s2m_config};
use narwhal_util::pool::Pool;
use narwhal_util::string_atom::StringAtom;
use prometheus_client::registry::Registry;

const TEST_USER: &str = "test_user_client";
const SECOND_USER: &str = "test_user_client_2";
const SHARED_SECRET: &str = "test_secret";

/// Trivial Authenticator that sends a fixed username as the AUTH token and
/// expects the server to accept it immediately. Paired with a TestModulator
/// whose auth handler returns `Success { username: token }`, this provides a
/// real AUTH flow without the complexity of challenge/response.
struct UsernameAuthenticator {
  username: String,
}

#[async_trait::async_trait]
impl Authenticator for UsernameAuthenticator {
  async fn start(&mut self) -> anyhow::Result<String> {
    Ok(self.username.clone())
  }
  async fn next(&mut self, _challenge: String) -> anyhow::Result<String> {
    Err(anyhow::anyhow!("unexpected challenge"))
  }
}

struct UsernameAuthFactory {
  username: String,
}

impl AuthenticatorFactory for UsernameAuthFactory {
  fn create(&self) -> Box<dyn Authenticator> {
    Box::new(UsernameAuthenticator { username: self.username.clone() })
  }
}

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

fn client_config_for(addr: std::net::SocketAddr) -> C2sConfig {
  C2sConfig {
    address: addr.to_string(),
    heartbeat_interval: Duration::from_secs(60),
    connect_timeout: Duration::from_secs(5),
    timeout: Duration::from_secs(5),
    payload_read_timeout: Duration::from_secs(5),
    backoff_initial_delay: Duration::from_millis(100),
    backoff_max_delay: Duration::from_secs(30),
    backoff_max_retries: 5,
  }
}

/// Boot the server and return a connected client + the live suite (so it
/// stays alive until the test scope ends and tears down cleanly).
async fn boot_with_client(username: &str) -> anyhow::Result<(C2sSuite, C2sClient)> {
  let mut suite = C2sSuite::new(default_c2s_config()).await?;
  suite.setup().await?;

  let addr = suite.local_address().expect("listener address not set");
  let client =
    C2sClient::new_with_insecure_tls(client_config_for(addr), AuthMethod::Identify { username: username.to_string() })?;
  // Trigger the connection handshake by querying session info.
  let _ = client.session_info().await?;
  Ok((suite, client))
}

#[compio::test]
async fn test_client_delete_channel() -> anyhow::Result<()> {
  let (mut suite, client) = boot_with_client(TEST_USER).await?;
  let channel = StringAtom::from("!del@localhost");
  client.join_channel(channel.clone()).await?;
  client.delete_channel(channel.clone()).await?;
  // After delete, list_channels should not include it.
  let listing = client.list_channels(None, None, true).await?;
  assert!(!listing.items.iter().any(|c| c.as_ref() == channel.as_ref()), "deleted channel still listed");
  client.shutdown().await?;
  suite.teardown().await?;
  Ok(())
}

#[compio::test]
async fn test_client_list_channels() -> anyhow::Result<()> {
  let (mut suite, client) = boot_with_client(TEST_USER).await?;
  let a = StringAtom::from("!listA@localhost");
  let b = StringAtom::from("!listB@localhost");
  client.join_channel(a.clone()).await?;
  client.join_channel(b.clone()).await?;
  let listing = client.list_channels(None, None, false).await?;
  let names: Vec<&str> = listing.items.iter().map(|c| c.as_ref()).collect();
  assert!(names.iter().any(|n| n.contains("listA")), "channel a missing: {:?}", names);
  assert!(names.iter().any(|n| n.contains("listB")), "channel b missing: {:?}", names);
  client.shutdown().await?;
  suite.teardown().await?;
  Ok(())
}

#[compio::test]
async fn test_client_list_members() -> anyhow::Result<()> {
  let (mut suite, owner) = boot_with_client(TEST_USER).await?;
  let channel = StringAtom::from("!members@localhost");
  owner.join_channel(channel.clone()).await?;

  // Second client joins via raw suite connection so list_members has > 1 entry.
  let addr = suite.local_address().expect("listener");
  let other = C2sClient::new_with_insecure_tls(
    client_config_for(addr),
    AuthMethod::Identify { username: SECOND_USER.to_string() },
  )?;
  let _ = other.session_info().await?;
  other.join_channel(channel.clone()).await?;

  let listing = owner.list_members(channel.clone(), None, None).await?;
  let nids: Vec<&str> = listing.items.iter().map(|n| n.as_ref()).collect();
  assert!(nids.iter().any(|n| n.starts_with(TEST_USER)), "owner missing from members: {:?}", nids);
  assert!(nids.iter().any(|n| n.starts_with(SECOND_USER)), "second member missing: {:?}", nids);

  other.shutdown().await?;
  owner.shutdown().await?;
  suite.teardown().await?;
  Ok(())
}

#[compio::test]
async fn test_client_get_channel_config() -> anyhow::Result<()> {
  let (mut suite, client) = boot_with_client(TEST_USER).await?;
  let channel = StringAtom::from("!config@localhost");
  client.join_channel(channel.clone()).await?;
  let cfg = client.get_channel_config(channel.clone()).await?;
  // Auto-created channel defaults are non-zero for max_clients / payload limits.
  assert!(cfg.max_clients > 0, "max_clients was 0: {:?}", cfg);
  assert!(cfg.max_payload_size > 0, "max_payload_size was 0: {:?}", cfg);
  assert_eq!(cfg.r#type.as_ref(), "pubsub");
  client.shutdown().await?;
  suite.teardown().await?;
  Ok(())
}

#[compio::test]
async fn test_client_get_channel_acl() -> anyhow::Result<()> {
  let (mut suite, client) = boot_with_client(TEST_USER).await?;
  let channel = StringAtom::from("!acl@localhost");
  client.join_channel(channel.clone()).await?;

  // Seed an ACL entry so the response carries something non-empty.
  let nid = StringAtom::from(format!("{}@localhost", SECOND_USER));
  client.set_channel_acl(channel.clone(), AclType::Read, AclAction::Add, vec![nid.parse()?]).await?;

  let listing = client.get_channel_acl(channel.clone(), AclType::Read, None, None).await?;
  assert!(listing.items.iter().any(|n| n.as_ref().starts_with(SECOND_USER)), "seeded nid missing: {:?}", listing.items);

  client.shutdown().await?;
  suite.teardown().await?;
  Ok(())
}

/// Boots the server with an auth modulator (so `persist=true` is allowed)
/// and a file-backed channel store + message log (so persistent channels can
/// actually accept appends). Returns the client plus the live suite +
/// modulator handles to keep them alive for the test scope.
async fn boot_with_auth_client(
  username: &str,
) -> anyhow::Result<(
  C2sSuite<FileChannelStore, FileMessageLogFactory>,
  narwhal_modulator::S2mListener<TestModulator>,
  CoreDispatcher,
  tempfile::TempDir,
  C2sClient,
)> {
  let (s2m_client, s2m_ln, s2m_dispatcher) = bootstrap_s2m(make_auth_modulator()).await?;
  let tmp = tempfile::tempdir()?;
  let store = FileChannelStore::new(tmp.path().to_path_buf()).await?;
  let mlf = FileMessageLogFactory::new(
    tmp.path().to_path_buf(),
    default_c2s_config().limits.max_payload_size,
    MessageLogMetrics::noop(),
  );

  let mut suite = C2sSuite::with_modulator_and_stores(default_c2s_config(), Some(s2m_client), None, store, mlf).await?;
  suite.setup().await?;

  let addr = suite.local_address().expect("listener address not set");
  let auth_method =
    AuthMethod::Auth { authenticator_factory: Arc::new(UsernameAuthFactory { username: username.to_string() }) };
  let client = C2sClient::new_with_insecure_tls(client_config_for(addr), auth_method)?;
  let _ = client.session_info().await?;

  Ok((suite, s2m_ln, s2m_dispatcher, tmp, client))
}

#[compio::test]
async fn test_client_channel_seq() -> anyhow::Result<()> {
  let (mut suite, mut s2m_ln, mut s2m_dispatcher, _tmp, client) = boot_with_auth_client(TEST_USER).await?;
  let channel = StringAtom::from("!seq@localhost");
  client.join_channel(channel.clone()).await?;
  // Enable persistence so CHAN_SEQ is valid.
  client.configure_channel(channel.clone(), None, None, Some(8), Some(true), Some(0)).await?;

  // Broadcast a payload to advance the log.
  let pool = Pool::new(1, 1024);
  let mut buf = pool.acquire_buffer().await;
  buf.as_mut_slice()[..5].copy_from_slice(b"hello");
  let payload = buf.freeze(5);
  client.broadcast(channel.clone(), None, payload).await?;

  let (first_seq, last_seq) = client.channel_seq(channel.clone()).await?;
  assert_eq!(first_seq, 1);
  assert_eq!(last_seq, 1);

  client.shutdown().await?;
  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

#[compio::test]
async fn test_client_history() -> anyhow::Result<()> {
  let (mut suite, mut s2m_ln, mut s2m_dispatcher, _tmp, client) = boot_with_auth_client(TEST_USER).await?;
  let channel = StringAtom::from("!hist@localhost");
  client.join_channel(channel.clone()).await?;
  client.configure_channel(channel.clone(), None, None, Some(8), Some(true), Some(0)).await?;

  // Broadcast three payloads.
  let pool = Pool::new(8, 1024);
  for body in [&b"alpha"[..], &b"beta"[..], &b"gamma"[..]] {
    let mut buf = pool.acquire_buffer().await;
    buf.as_mut_slice()[..body.len()].copy_from_slice(body);
    let payload = buf.freeze(body.len());
    client.broadcast(channel.clone(), None, payload).await?;
  }

  let entries = client.history(channel.clone(), 1, 10).await?;
  assert_eq!(entries.len(), 3, "expected 3 entries, got {}", entries.len());
  let bodies: Vec<&[u8]> = entries.iter().map(|e| e.payload.as_slice()).collect();
  assert_eq!(bodies, vec![&b"alpha"[..], &b"beta"[..], &b"gamma"[..]]);
  // Seq is monotonically increasing and 1-based.
  assert_eq!(entries.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2, 3]);

  client.shutdown().await?;
  suite.teardown().await?;
  s2m_ln.shutdown().await?;
  s2m_dispatcher.shutdown().await?;
  Ok(())
}

#[compio::test]
async fn test_client_mod_direct_without_modulator_returns_error() -> anyhow::Result<()> {
  // The test suite is not booted with a modulator, so a MOD_DIRECT request
  // must surface an error from the client rather than hang or panic. This
  // verifies the client correctly parses the ERROR response.
  let (mut suite, client) = boot_with_client(TEST_USER).await?;

  let pool = Pool::new(1, 1024);
  let mut buf = pool.acquire_buffer().await;
  buf.as_mut_slice()[..4].copy_from_slice(b"ping");
  let payload = buf.freeze(4);

  let result = client.mod_direct(payload).await;
  assert!(result.is_err(), "expected ERROR from server when no modulator is attached");

  client.shutdown().await?;
  suite.teardown().await?;
  Ok(())
}
