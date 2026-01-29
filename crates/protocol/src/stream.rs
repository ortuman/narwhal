// SPDX-License-Identifier: BSD-3-Clause

use std::io::Cursor;

use futures::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use narwhal_util::codec_tokio::StreamReader;
use narwhal_util::pool::MutablePoolBuffer;

use crate::{Message, deserialize, serialize};

/// Sends a request message and returns the response.
///
/// This function implements a request-response pattern: it serializes a request,
/// writes it to the stream, reads the response, and deserializes it.
///
/// # Type Parameters
/// * `S` - The stream type (must implement AsyncRead + AsyncWrite + Unpin)
///
/// # Arguments
/// * `message` - The message to send
/// * `stream` - The stream to write to and read from
/// * `pool_buffer` - Pre-allocated buffer from a pool
///
/// # Returns
/// * `Ok(Message)` - The deserialized response message
/// * `Err(_)` - Serialization, IO, or deserialization error
pub async fn request<S>(message: Message, stream: &mut S, mut pool_buffer: MutablePoolBuffer) -> anyhow::Result<Message>
where
  S: AsyncRead + AsyncWrite + Unpin,
{
  // Serialize the message into the buffer
  let n = serialize(&message, pool_buffer.as_mut_slice())?;

  // Write the message to the stream
  stream.write_all(&pool_buffer.as_slice()[..n]).await?;
  stream.flush().await?;

  // Convert to reader for reading the response
  let mut stream_reader = StreamReader::with_pool_buffer(&mut *stream, pool_buffer);

  // Read the response
  let _ = stream_reader.next().await?;

  // Deserialize the response
  match stream_reader.get_line() {
    Some(line_bytes) => {
      let response = deserialize(Cursor::new(line_bytes))?;
      Ok(response)
    },
    None => Err(anyhow::anyhow!("failed to read response from server")),
  }
}
