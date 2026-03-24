// SPDX-License-Identifier: BSD-3-Clause

use anyhow::anyhow;
use monoio::BufResult;
use monoio::buf::{IoBuf, IoBufMut, IoVecBuf, IoVecBufMut};
use monoio::io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt};

use narwhal_protocol::ErrorReason::BadRequest;
use narwhal_util::codec_monoio::{StreamReader, StreamReaderError};
use narwhal_util::pool::{BucketedPool, Pool, PoolBuffer};
use narwhal_util::string_atom::StringAtom;

use super::{ConnStreamFactory, ConnStreamReader, ConnWriter, LocalStream, MAX_BUFFERS_PER_BATCH, ReadLineError};

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

pub struct MonoioStreamReader<T: AsyncReadRent + 'static> {
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

pub struct MonoioConnWriter<W> {
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

impl<S: AsyncReadRent + AsyncWriteRent + 'static> ConnStreamFactory for S {
  type Reader = MonoioStreamReader<S>;
  type Writer = MonoioConnWriter<LocalStream<S>>;

  async fn create(self, pool: &Pool) -> (Self::Reader, Self::Writer) {
    let local_stream = LocalStream::new(self);
    let reader = MonoioStreamReader::new(local_stream.clone(), pool).await;
    let writer = MonoioConnWriter::new(local_stream);
    (reader, writer)
  }

  async fn reject(mut self, error: &[u8]) {
    let _ = self.write_all(error.to_vec()).await;
    let _ = AsyncWriteRent::flush(&mut self).await;
    let _ = AsyncWriteRent::shutdown(&mut self).await;
  }
}
