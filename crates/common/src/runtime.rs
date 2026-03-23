// SPDX-License-Identifier: BSD-3-Clause

use std::future::Future;
use std::time::Duration;

/// Trait for an asynchronous runtime.
pub trait Runtime: Clone + Send + Sync + 'static {
  /// A handle to a spawned task.
  type JoinHandle<T: 'static>: Future<Output = T> + 'static;

  /// Spawns a task onto the runtime.
  fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
  where
    F: Future + 'static,
    F::Output: 'static;

  /// Sleeps for the specified duration.
  fn sleep(&self, duration: Duration) -> impl Future<Output = ()>;

  /// Runs a future to completion with a timeout.
  fn timeout<F>(&self, duration: Duration, future: F) -> impl Future<Output = Result<F::Output, ()>>
  where
    F: Future;

  /// Runs a future on a new runtime instance.
  fn block_on<F>(&self, future: F) -> F::Output
  where
    F: Future;

  /// Returns the current runtime.
  fn current() -> Self;
}

#[cfg(feature = "runtime-monoio")]
/// A implementation of the `Runtime` trait for `monoio`.
#[derive(Clone, Copy, Debug, Default)]
pub struct MonoioRuntime;

#[cfg(feature = "runtime-monoio")]
impl Runtime for MonoioRuntime {
  type JoinHandle<T: 'static> = monoio::task::JoinHandle<T>;

  fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
  where
    F: Future + 'static,
    F::Output: 'static,
  {
    monoio::spawn(future)
  }

  fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
    monoio::time::sleep(duration)
  }

  async fn timeout<F>(&self, duration: Duration, future: F) -> Result<F::Output, ()>
  where
    F: Future,
  {
    monoio::time::timeout(duration, future).await.map_err(|_| ())
  }

  fn block_on<F>(&self, future: F) -> F::Output
  where
    F: Future,
  {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
      .enable_all()
      .build()
      .expect("failed to create monoio runtime");
    rt.block_on(future)
  }

  fn current() -> Self {
    MonoioRuntime
  }
}

#[cfg(not(feature = "runtime-monoio"))]
compile_error!("Feature 'runtime-monoio' must be enabled for narwhal-common.");

/// The currently configured runtime.
#[cfg(feature = "runtime-monoio")]
pub type CurrentRuntime = MonoioRuntime;

/// Spawns a task onto the current runtime.
pub fn spawn<F>(future: F) -> <CurrentRuntime as Runtime>::JoinHandle<F::Output>
where
  F: Future + 'static,
  F::Output: 'static,
{
  #[cfg(feature = "runtime-monoio")]
  {
    MonoioRuntime.spawn(future)
  }
}

/// Sleeps for the specified duration using the current runtime.
pub fn sleep(duration: Duration) -> impl Future<Output = ()> {
  #[cfg(feature = "runtime-monoio")]
  {
    MonoioRuntime.sleep(duration)
  }
}

/// Runs a future to completion with a timeout using the current runtime.
pub fn timeout<F>(duration: Duration, future: F) -> impl Future<Output = Result<F::Output, ()>>
where
  F: Future,
{
  #[cfg(feature = "runtime-monoio")]
  {
    MonoioRuntime.timeout(duration, future)
  }
}

/// Runs a future on a new runtime instance using the current runtime type.
pub fn block_on<F>(future: F) -> F::Output
where
  F: Future,
{
  #[cfg(feature = "runtime-monoio")]
  {
    MonoioRuntime.block_on(future)
  }
}
