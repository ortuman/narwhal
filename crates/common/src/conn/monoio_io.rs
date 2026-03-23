// SPDX-License-Identifier: BSD-3-Clause

use std::sync::atomic::Ordering;

use anyhow::anyhow;
use async_channel::bounded;
use monoio::BufResult;
use monoio::buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt};
use tracing::{trace, warn};

use narwhal_protocol::ErrorReason::BadRequest;
use narwhal_util::codec_monoio::{StreamReader, StreamReaderError};
use narwhal_util::pool::{BucketedPool, Pool, PoolBuffer};
use narwhal_util::string_atom::StringAtom;

use crate::service::Service;

use super::{
  Conn, ConnRuntime, ConnStreamReader, ConnTx, ConnWriter, Dispatcher, DispatcherFactory, LocalStream,
  MAX_BUFFERS_PER_BATCH, ReadLineError, SERVER_OVERLOADED_ERROR,
};

impl<S: AsyncReadRent> AsyncReadRent for LocalStream<S> {
  async fn read<B: IoBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
    // SAFETY: single-threaded and cooperative – only one task
    // executes at any point, so no concurrent mutable access can occur.
    let stream = unsafe { &mut *self.0.get() };
    stream.read(buf).await
  }

  async fn readv<B: IoVecBufMut>(&mut self, buf: B) -> BufResult<usize, B> {
    let stream = unsafe { &mut *self.0.get() };
    stream.readv(buf).await
  }
}

impl<S: AsyncWriteRent> AsyncWriteRent for LocalStream<S> {
  async fn write<B: IoBuf>(&mut self, buf: B) -> BufResult<usize, B> {
    let stream = unsafe { &mut *self.0.get() };
    stream.write(buf).await
  }

  async fn writev<B: IoVecBuf>(&mut self, buf: B) -> BufResult<usize, B> {
    let stream = unsafe { &mut *self.0.get() };
    stream.writev(buf).await
  }

  async fn flush(&mut self) -> std::io::Result<()> {
    let stream = unsafe { &mut *self.0.get() };
    stream.flush().await
  }

  async fn shutdown(&mut self) -> std::io::Result<()> {
    let stream = unsafe { &mut *self.0.get() };
    stream.shutdown().await
  }
}

struct MonoioStreamReader<T: AsyncReadRent + 'static> {
  stream_reader: StreamReader<LocalStream<T>>,
  delimiter_buf: Box<[u8; 1]>,
}

impl<T: AsyncReadRent + 'static> MonoioStreamReader<T> {
  async fn new(reader: LocalStream<T>, message_buffer_pool: &Pool) -> Self {
    let read_pool_buffer = message_buffer_pool.acquire_buffer().await;
    let stream_reader = StreamReader::with_pool_buffer(reader, read_pool_buffer);
    Self { stream_reader, delimiter_buf: Box::new([0u8; 1]) }
  }
}

impl<T: AsyncReadRent + 'static> ConnStreamReader for MonoioStreamReader<T> {
  async fn next(&mut self) -> Result<bool, ReadLineError> {
    self.stream_reader.next().await.map_err(|e| match e {
      StreamReaderError::MaxLineLengthExceeded => ReadLineError::MaxLineLengthExceeded,
      StreamReaderError::IoError(e) => ReadLineError::Io(e),
    })
  }

  fn line(&mut self) -> Option<&[u8]> {
    self.stream_reader.get_line()
  }

  async fn read_payload(
    &mut self,
    pool: &BucketedPool,
    len: usize,
    correlation_id: Option<u32>,
  ) -> anyhow::Result<PoolBuffer> {
    let pool_buff = pool.acquire_buffer(len).await.unwrap();
    let mut buf = self.stream_reader.read_raw(pool_buff, len).await?;

    // Take delimiter_buf for ownership-based I/O, then put it back.
    let delimiter = std::mem::replace(&mut self.delimiter_buf, Box::new([0u8; 1]));
    let delimiter = self.stream_reader.read_raw(delimiter, 1).await?;

    if delimiter[0] != b'\n' {
      self.delimiter_buf = delimiter;
      let mut error = narwhal_protocol::Error::new(BadRequest).with_detail(StringAtom::from("invalid payload format"));
      if let Some(id) = correlation_id {
        error = error.with_id(id);
      }
      return Err(error.into());
    }

    self.delimiter_buf = delimiter;
    Ok(buf.freeze(len))
  }
}

struct MonoioConnWriter<W> {
  writer: W,
  iovec_batch: Vec<libc::iovec>,
}

impl<W> MonoioConnWriter<W> {
  fn new(writer: W) -> Self {
    Self { writer, iovec_batch: Vec::with_capacity(MAX_BUFFERS_PER_BATCH) }
  }
}

impl<W: AsyncWriteRent> ConnWriter for MonoioConnWriter<W> {
  async fn write_flush_batch(&mut self, batch: &mut Vec<PoolBuffer>) -> anyhow::Result<()> {
    if batch.is_empty() {
      return Ok(());
    }

    // Populate the pre-allocated iovec batch from the pool buffers.
    self
      .iovec_batch
      .extend(batch.iter().map(|b| libc::iovec { iov_base: b.read_ptr() as *mut _, iov_len: b.bytes_init() }));

    let mut remaining: usize = self.iovec_batch.iter().map(|v| v.iov_len).sum();

    // Take the iovec batch for ownership-based writev.
    let mut iovecs = std::mem::take(&mut self.iovec_batch);

    // Loop writev until every byte is out.
    while remaining > 0 {
      let (result, returned_iovecs) = self.writer.writev(iovecs).await;
      let written = result?;

      iovecs = returned_iovecs;

      if written == 0 {
        batch.clear();
        iovecs.clear();
        self.iovec_batch = iovecs;
        return Err(anyhow!("writev unable to make progress"));
      }
      remaining -= written;

      if remaining == 0 {
        break;
      }

      // Advance iovecs past the bytes just written.
      let mut consumed = 0usize;
      let mut to_skip = written;

      for iovec in iovecs.iter_mut() {
        if to_skip >= iovec.iov_len {
          to_skip -= iovec.iov_len;
          consumed += 1;
        } else {
          // SAFETY: we advance the pointer within the bounds of the
          // original PoolBuffer, which is kept alive in `batch`.
          iovec.iov_base = unsafe { (iovec.iov_base as *mut u8).add(to_skip) as *mut _ };
          iovec.iov_len -= to_skip;
          break;
        }
      }

      // Remove fully-consumed entries so writev never sees leading
      // zero-length iovecs (which some implementations return 0 for).
      if consumed > 0 {
        iovecs.drain(..consumed);
      }
    }

    batch.clear();
    iovecs.clear();
    self.iovec_batch = iovecs; // Put back for reuse.

    self.writer.flush().await?;

    Ok(())
  }

  async fn shutdown(&mut self) -> std::io::Result<()> {
    self.writer.shutdown().await
  }
}

impl<ST: Service> ConnRuntime<ST> {
  /// Runs a single client connection to completion.
  pub async fn run_connection<S, D: Dispatcher, DF: DispatcherFactory<D>>(
    &self,
    mut stream: S,
    mut dispatcher_factory: DF,
  ) where
    S: AsyncReadRent + AsyncWriteRent + 'static,
  {
    let inner = &self.0;

    // Assign a unique handler
    let handler = inner.next_handler.fetch_add(1, Ordering::SeqCst);
    let config = inner.config.clone();
    let active_connections = inner.active_connections.clone();
    let metrics = inner.metrics.clone();

    // Check if we've reached the maximum number of connections
    let current_connections = active_connections.fetch_add(1, Ordering::SeqCst);

    if current_connections >= config.max_connections {
      active_connections.fetch_sub(1, Ordering::SeqCst);
      metrics.connections_rejected.inc();

      let _ = stream.write_all(SERVER_OVERLOADED_ERROR.to_vec()).await;
      let _ = stream.flush().await;
      let _ = stream.shutdown().await;

      let max_conns = config.max_connections;
      warn!(max_conns, service_type = ST::NAME, "max connections limit reached");

      return;
    }

    metrics.connections_active.inc();

    // Create a new connection.
    let send_msg_channel_size = config.outbound_message_queue_size as usize;

    let (send_msg_tx, send_msg_rx) = bounded(send_msg_channel_size);
    let (close_tx, close_rx) = bounded(1);

    let tx = ConnTx::new(send_msg_tx, close_tx, metrics.outbound_queue_full.clone());

    let dispatcher = dispatcher_factory.create(handler, tx.clone()).await;

    let mut conn = Conn::new(handler, config.clone(), dispatcher, tx);

    conn.schedule_timeout(config.connect_timeout, Some(StringAtom::from("connection timeout")));

    trace!(handler = handler, service_type = ST::NAME, "connection registered");

    // Create monoio-specific reader and writer from the stream.
    let local_stream = LocalStream::new(stream);
    let reader = MonoioStreamReader::new(local_stream.clone(), &inner.message_buffer_pool).await;
    let mut writer = MonoioConnWriter::new(local_stream);

    let payload_read_timeout = config.payload_read_timeout;
    let max_payload_size = config.max_payload_size as usize;
    let rate_limit = config.rate_limit;

    match Conn::<D>::run::<_, _, ST>(
      reader,
      &mut writer,
      &mut conn,
      handler,
      send_msg_rx,
      close_rx,
      inner.shutdown_token.clone(),
      inner.message_buffer_pool.clone(),
      inner.payload_buffer_pool.clone(),
      inner.newline_buffer.clone(),
      payload_read_timeout,
      max_payload_size,
      rate_limit,
    )
    .await
    {
      Ok(_) => {},
      Err(e) => {
        warn!(handler = handler, service_type = ST::NAME, "connection error: {}", e.to_string());
      },
    }

    // Decrement the active connections counter
    active_connections.fetch_sub(1, Ordering::SeqCst);
    metrics.connections_active.dec();

    trace!(handler = handler, service_type = ST::NAME, "connection deregistered");
  }
}
