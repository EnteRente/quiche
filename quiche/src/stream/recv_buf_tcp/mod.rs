// Copyright (C) 2023, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use std::cmp;

use std::collections::VecDeque;

use std::time::Duration;
use std::time::Instant;

use crate::stream::RecvAction;
use crate::stream::RecvBufResetReturn;
use crate::Error;
use crate::Result;

use crate::flowcontrol;

use crate::range_buf::RangeBuf;

use self::queue::OutOfOrder;

mod queue;

// This models the fragment cap in the metadata reference. It is a bound on
// RangeBuf descriptors, not a claim about Linux skb memory layout.
const MAX_FRAGMENTS: usize = 17;

/// Experimental receive buffer with a separate in-order queue and red-black
/// tree for out-of-order data.
///
/// Inspired by the queue organization in Linux TCP's `tcp_input.c`. Payloads
/// remain in RangeBuf allocations; coalescing only groups their descriptors.
/// QUIC offsets, final size, and first-received overlap semantics are retained.
///
/// Stream data received by the peer is buffered in a list of data chunks
/// ordered by offset in ascending order. Contiguous data can then be read
/// into a slice.
#[derive(Debug, Default)]
pub struct RecvBuf {
    /// Contiguous data received from the peer but not read by the application.
    data: VecDeque<RangeBuf>,

    /// Data behind gaps, with a cached first offset and an owned tail.
    out_of_order: OutOfOrder,

    /// End of contiguous received data, including unread application data.
    /// Corresponds to TCP's rcv_nxt; `off` corresponds to copied_seq.
    contiguous_off: u64,

    /// The lowest data offset that has yet to be read by the application.
    off: u64,

    /// The total length of data received on this stream.
    len: u64,

    /// Receiver flow controller.
    flow_control: flowcontrol::FlowControl,

    /// The final stream offset received from the peer, if any.
    fin_off: Option<u64>,

    /// The error code received via RESET_STREAM.
    error: Option<u64>,

    /// Whether incoming data is validated but not buffered.
    drain: bool,
}

impl RecvBuf {
    /// Creates a new receive buffer.
    pub fn new(max_data: u64, initial_window: u64, max_window: u64) -> RecvBuf {
        RecvBuf {
            flow_control: flowcontrol::FlowControl::new(
                max_data,
                initial_window,
                max_window,
            ),
            ..RecvBuf::default()
        }
    }

    /// Inserts the given chunk of data in the buffer.
    ///
    /// This also takes care of enforcing stream flow control limits, as well
    /// as handling incoming data that overlaps data that is already in the
    /// buffer.
    pub fn write(&mut self, mut buf: RangeBuf) -> Result<()> {
        if buf.max_off() > self.max_data() {
            return Err(Error::FlowControl);
        }

        if let Some(fin_off) = self.fin_off {
            // Stream's size is known, forbid data beyond that point.
            if buf.max_off() > fin_off {
                return Err(Error::FinalSize);
            }

            // Stream's size is already known, forbid changing it.
            if buf.fin() && fin_off != buf.max_off() {
                return Err(Error::FinalSize);
            }
        }

        // Stream's known size is lower than data already received.
        if buf.fin() && buf.max_off() < self.len {
            return Err(Error::FinalSize);
        }

        // We already saved the final offset, so there's nothing else we
        // need to keep from the RangeBuf if it's empty.
        if self.fin_off.is_some() && buf.is_empty() {
            return Ok(());
        }

        let new_fin = buf.fin() && self.fin_off.is_none();

        if new_fin {
            self.fin_off = Some(buf.max_off());
        }

        // No need to store empty buffer that doesn't carry the fin flag, but
        // its offset still advances the largest received offset used by flow
        // control (RFC 9000 Section 19.8).
        if !buf.fin() && buf.is_empty() {
            self.len = cmp::max(self.len, buf.max_off());

            if self.drain {
                // we are not storing any data, off == len
                self.off = self.len;
                self.contiguous_off = self.off;
            }

            return Ok(());
        }

        // Discard consumed data unless it carries the first FIN. Such data is
        // replaced with an empty buffer at the final size, so the application
        // reads the FIN without receiving duplicate data and the queued buffer
        // does not keep the duplicate payload alive.
        if self.off >= buf.max_off() && !buf.is_empty() {
            if !new_fin {
                return Ok(());
            }

            buf = RangeBuf::from(b"", buf.max_off(), true);
        }

        self.len = cmp::max(self.len, buf.max_off());

        if self.drain {
            self.off = self.len;
            self.contiguous_off = self.off;
            return Ok(());
        }

        // FIN consumes no stream offset in QUIC. A marker is only necessary
        // when there is no payload left to deliver the terminal notification.
        if buf.is_empty() {
            if buf.off() == self.contiguous_off && self.data.is_empty() {
                self.data.push_back(buf);
            }
            return Ok(());
        }

        // Drop duplicates of both consumed and queued in-order bytes.
        if buf.max_off() <= self.contiguous_off {
            return Ok(());
        }

        if buf.off() < self.contiguous_off {
            buf.consume((self.contiguous_off - buf.off()) as usize);
        }

        let first = self.out_of_order.first_off();
        if buf.off() == self.contiguous_off &&
            first.is_none_or(|off| off >= buf.max_off())
        {
            // In-order traffic never allocates or searches a tree node.
            self.contiguous_off = buf.max_off();
            self.data.push_back(buf);
            self.promote();
            return Ok(());
        }

        self.out_of_order.write(buf);
        self.promote();

        Ok(())
    }

    /// Reads contiguous data from the receive buffer.
    ///
    /// Data is written into the given `out` buffer, up to the length of `out`.
    ///
    /// Only contiguous data is removed, starting from offset 0. The offset is
    /// incremented as data is taken out of the receive buffer. If there is no
    /// data at the expected read offset, the `Done` error is returned.
    ///
    /// On success the amount of data read and a flag indicating
    /// if there is no more data in the buffer, are returned as a tuple.
    #[inline]
    pub fn emit(&mut self, mut out: &mut [u8]) -> Result<(usize, bool)> {
        self.emit_or_discard(RecvAction::Emit { out: &mut out })
    }

    /// Reads or discards contiguous data from the receive buffer.
    ///
    /// Passing an `action` of `StreamRecvAction::Emit` results in data being
    /// written into the provided buffer, up to its length.
    ///
    /// Passing an `action` of `StreamRecvAction::Discard` results in up to
    /// the indicated number of bytes being discarded without copying.
    ///
    /// Only contiguous data is removed, starting from offset 0. The offset is
    /// incremented as data is taken out of the receive buffer. If there is no
    /// data at the expected read offset, the `Done` error is returned.
    ///
    /// On success the amount of data read or discarded, and a flag indicating
    /// if there is no more data in the buffer, are returned as a tuple.
    pub fn emit_or_discard<B: bytes::BufMut>(
        &mut self, mut action: RecvAction<B>,
    ) -> Result<(usize, bool)> {
        let mut len = 0;
        let mut cap = match &action {
            RecvAction::Emit { out } => out.remaining_mut(),
            RecvAction::Discard { len } => *len,
        };

        if !self.ready() {
            return Err(Error::Done);
        }

        // The stream was reset, so clear its data and return the error code
        // instead.
        if let Some(e) = self.error {
            self.data.clear();
            self.out_of_order.clear();
            return Err(Error::StreamReset(e));
        }

        while self.ready() {
            let buf = self.data.front_mut().unwrap();

            if cap == 0 && !buf.is_empty() {
                break;
            }

            let buf_len = cmp::min(buf.len(), cap);

            // Only copy data if we're emitting, not discarding.
            if let RecvAction::Emit { ref mut out } = action {
                // Note: `BufMut::remaining_mut()` cannot "shrink", but BufMut
                // impls are allowed to grow the buffer, so we
                // check here that we still have at least
                // `cap` bytes, but we can't require equality
                debug_assert!(
                    cap <= out.remaining_mut(),
                    "We updated `cap` incorrectly"
                );
                out.put_slice(&buf[..buf_len])
            }

            self.off += buf_len as u64;

            len += buf_len;
            cap -= buf_len;

            if buf_len < buf.len() {
                buf.consume(buf_len);

                // We reached the maximum capacity, so end here.
                break;
            }

            self.data.pop_front();
        }

        // Update consumed bytes for flow control.
        self.flow_control.add_consumed(len as u64);

        Ok((len, self.is_fin()))
    }

    /// Resets the stream at the given offset.
    pub fn reset(
        &mut self, error_code: u64, final_size: u64,
    ) -> Result<RecvBufResetReturn> {
        // Stream's size is already known, forbid changing it.
        if let Some(fin_off) = self.fin_off {
            if fin_off != final_size {
                return Err(Error::FinalSize);
            }
        }

        // Stream's known size is lower than data already received.
        if final_size < self.len {
            return Err(Error::FinalSize);
        }

        if final_size > self.max_data() {
            return Err(Error::FlowControl);
        }

        if self.error.is_some() {
            // We already verified that the final size matches
            return Ok(RecvBufResetReturn::zero());
        }

        // Calculate how many bytes need to be removed from the connection flow
        // control.
        let result = RecvBufResetReturn {
            max_data_delta: final_size - self.len,
            consumed_flowcontrol: final_size - self.off,
        };

        let notify = !self.drain && (!self.is_fin() || self.ready());

        self.error = Some(error_code);
        self.fin_off = Some(final_size);
        self.len = final_size;

        // Clear all data already buffered.
        self.off = final_size;

        self.data.clear();
        self.out_of_order.clear();
        self.contiguous_off = final_size;

        // Preserve an unread terminal notification even if FIN already
        // established the final size, but do not notify again after EOF.
        if notify {
            let buf = RangeBuf::from(b"", final_size, true);
            self.data.push_back(buf);
        }

        Ok(result)
    }

    /// Commits the new max_data limit.
    pub fn update_max_data(&mut self, now: Instant) {
        self.flow_control.update_max_data(now);
    }

    /// Return the new max_data limit.
    pub fn max_data_next(&mut self) -> u64 {
        self.flow_control.max_data_next()
    }

    /// Return the current flow control limit.
    pub fn max_data(&self) -> u64 {
        self.flow_control.max_data()
    }

    /// Return the current window.
    pub fn window(&self) -> u64 {
        self.flow_control.window()
    }

    /// Autotune the window size.
    pub fn autotune_window(&mut self, now: Instant, rtt: Duration) {
        self.flow_control.autotune_window(now, rtt);
    }

    /// Shuts down receiving data and returns the number of bytes
    /// that should be returned to the connection level flow
    /// control
    pub fn shutdown(&mut self) -> Result<u64> {
        if self.drain {
            return Err(Error::Done);
        }

        self.drain = true;

        self.data.clear();
        self.out_of_order.clear();

        let consumed = self.max_off() - self.off;
        self.off = self.max_off();
        self.contiguous_off = self.off;

        Ok(consumed)
    }

    /// Returns the lowest offset of data buffered.
    pub fn off_front(&self) -> u64 {
        self.off
    }

    /// Returns true if we need to update the local flow control limit.
    pub fn almost_full(&self) -> bool {
        self.fin_off.is_none() && self.flow_control.should_update_max_data()
    }

    /// Returns the largest offset ever received.
    pub fn max_off(&self) -> u64 {
        self.len
    }

    /// Returns true if the receive-side of the stream is complete.
    ///
    /// This happens when the stream's receive final size is known, and the
    /// application has read all data from the stream.
    pub fn is_fin(&self) -> bool {
        if self.fin_off == Some(self.off) {
            return true;
        }

        false
    }

    /// Returns true if the stream is not storing incoming data.
    pub fn is_draining(&self) -> bool {
        self.drain
    }

    /// Returns true if the stream has data to be read.
    pub fn ready(&self) -> bool {
        !self.data.is_empty()
    }

    /// Returns the contiguous readable byte count, capped at `max_len`.
    ///
    /// The receive frontier makes this independent of the number of chunks.
    pub fn readable_len(&self, max_len: usize) -> usize {
        (self.contiguous_off - self.off).min(max_len as u64) as usize
    }

    fn promote(&mut self) {
        self.out_of_order
            .promote(&mut self.data, &mut self.contiguous_off);
    }

    #[cfg(test)]
    pub(crate) fn flow_control_for_tests(&self) -> &flowcontrol::FlowControl {
        &self.flow_control
    }
}

#[cfg(test)]
mod tests;
