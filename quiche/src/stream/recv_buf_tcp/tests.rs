// Copyright (C) 2026, Cloudflare, Inc.
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

use super::*;
use crate::stream::recv_buf::RecvBuf as BTreeRecvBuf;

struct Pair {
    tree: BTreeRecvBuf,
    tcp: RecvBuf,
}

impl Pair {
    fn new(max_data: u64) -> Self {
        Self {
            tree: BTreeRecvBuf::new(max_data, 128, 512),
            tcp: RecvBuf::new(max_data, 128, 512),
        }
    }

    fn check(&mut self) {
        self.tcp.out_of_order.validate_cache();
        assert_eq!(self.tcp.off_front(), self.tree.off_front());
        assert_eq!(self.tcp.max_off(), self.tree.max_off());
        assert_eq!(self.tcp.ready(), self.tree.ready());
        assert_eq!(self.tcp.is_fin(), self.tree.is_fin());
        assert_eq!(self.tcp.is_draining(), self.tree.is_draining());
        assert_eq!(self.tcp.max_data(), self.tree.max_data());
        assert_eq!(self.tcp.max_data_next(), self.tree.max_data_next());
        assert_eq!(self.tcp.window(), self.tree.window());
        assert_eq!(self.tcp.almost_full(), self.tree.almost_full());
        assert_eq!(
            self.tcp.flow_control_for_tests().consumed(),
            self.tree.flow_control_for_tests().consumed(),
        );
        for cap in [0, 1, 7, 128, usize::MAX] {
            assert_eq!(self.tcp.readable_len(cap), self.tree.readable_len(cap));
        }
        let mut off = self.tcp.off;
        for buf in &self.tcp.data {
            assert_eq!(buf.off(), off);
            off = buf.max_off();
        }
        assert_eq!(off, self.tcp.contiguous_off);
        for segment in self.tcp.out_of_order.iter() {
            assert!(segment.off > self.tcp.contiguous_off);
            assert!(segment.off >= off);
            off = segment.off;
            assert!(segment.bufs.len() <= MAX_FRAGMENTS);
            for buf in &segment.bufs {
                assert!(!buf.is_empty());
                assert_eq!(buf.off(), off);
                off = buf.max_off();
            }
            assert_eq!(segment.end, off);
        }
        assert_eq!(
            self.tcp.out_of_order.first_off(),
            self.tcp
                .out_of_order
                .iter()
                .next()
                .map(|segment| segment.off)
        );
    }

    fn write(&mut self, data: &[u8], off: u64, fin: bool) {
        let buf = RangeBuf::from(data, off, fin);
        assert_eq!(self.tcp.write(buf.clone()), self.tree.write(buf));
        self.check();
    }

    fn read(&mut self, cap: usize, discard: bool) {
        let mut tcp = vec![0; cap];
        let mut tree = vec![0; cap];
        let (a, b) = if discard {
            (
                self.tcp.emit_or_discard(RecvAction::<&mut [u8]>::Discard {
                    len: cap,
                }),
                self.tree.emit_or_discard(RecvAction::<&mut [u8]>::Discard {
                    len: cap,
                }),
            )
        } else {
            (self.tcp.emit(&mut tcp), self.tree.emit(&mut tree))
        };
        assert_eq!(a, b);
        assert_eq!(tcp, tree);
        self.check();
    }

    fn reset(&mut self, error: u64, final_size: u64) {
        assert_eq!(
            self.tcp.reset(error, final_size),
            self.tree.reset(error, final_size)
        );
        self.check();
    }

    fn shutdown(&mut self) {
        assert_eq!(self.tcp.shutdown(), self.tree.shutdown());
        self.check();
    }
}

fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn randomized_payloads_and_partial_reads() {
    for seed in 1..=64 {
        let mut state = seed;
        let mut pair = Pair::new(512);
        for _ in 0..256 {
            let off = random(&mut state) % 256;
            let len = (random(&mut state) % 48) as usize;
            // Distinct payloads expose which bytes survive an overlap.
            let data = vec![random(&mut state) as u8; len];
            pair.write(&data, off, false);
            if random(&mut state).is_multiple_of(4) {
                let cap = (random(&mut state) % 32) as usize;
                pair.read(cap, random(&mut state).is_multiple_of(2));
            }
        }
        pair.write(&[0xff; 304], 0, true);
        while pair.tcp.ready() {
            pair.read(7, false);
        }
        assert!(pair.tcp.is_fin());
        pair.read(1, false);
    }
}

#[test]
fn randomized_terminal_and_flow_control_operations() {
    for seed in 1..=128 {
        let mut state = seed;
        let mut pair = Pair::new(128);
        for _ in 0..128 {
            match random(&mut state) % 32 {
                0 => pair.shutdown(),
                1 => pair.reset(random(&mut state) % 4, random(&mut state) % 256),
                2 => {
                    let now = Instant::now();
                    pair.tree.update_max_data(now);
                    pair.tcp.update_max_data(now);
                    pair.tree.autotune_window(now, Duration::from_millis(10));
                    pair.tcp.autotune_window(now, Duration::from_millis(10));
                    pair.check();
                },
                3..=10 => pair.read(
                    (random(&mut state) % 32) as usize,
                    seed.is_multiple_of(2),
                ),
                _ => {
                    let off = random(&mut state) % 192;
                    let len = (random(&mut state) % 32) as usize;
                    let fin = random(&mut state).is_multiple_of(8);
                    pair.write(&vec![off as u8; len], off, fin);
                },
            }
        }
    }
}

#[test]
fn in_order_data_bypasses_tree_while_application_lags() {
    let mut pair = Pair::new(1024);
    for i in 0..64 {
        pair.write(&[i as u8; 8], i * 8, i == 63);
        assert_eq!(pair.tcp.out_of_order.len(), 0);
    }
    assert_eq!(pair.tcp.readable_len(usize::MAX), 512);
    pair.read(0, false);
    pair.read(3, false);
    pair.read(509, false);
    pair.read(1, false);
}

#[test]
fn tail_and_predecessor_coalescing_respect_fragment_limit() {
    let mut pair = Pair::new(1024);
    for i in 1..=MAX_FRAGMENTS {
        pair.write(&[i as u8; 3], (i * 3) as u64, false);
        assert_eq!(pair.tcp.out_of_order.len(), 1);
    }
    pair.write(b"tail", 400, false);
    pair.write(b"abc", ((MAX_FRAGMENTS + 1) * 3) as u64, false);
    assert_eq!(pair.tcp.out_of_order.len(), 3);
    pair.write(b"def", ((MAX_FRAGMENTS + 2) * 3) as u64, false);
    assert_eq!(pair.tcp.out_of_order.len(), 3);
    pair.write(b"xyz", 0, false);
    assert_eq!(pair.tcp.out_of_order.len(), 1);
    pair.read(5, false);
    pair.read(100, false);
    pair.write(&[9; 400], 0, false);
    pair.read(500, false);
    assert_eq!(pair.tcp.out_of_order.len(), 0);
    pair.write(b"new tail", 500, false);
    pair.write(&[8; 96], 404, false);
    pair.read(104, false);
}

#[test]
fn rightmost_arrivals_survive_promotion_and_refill() {
    // A stride of one forces tail appends at the fragment limit. A stride of
    // four leaves a gap between every arrival, preventing all coalescing.
    for stride in [1, 4] {
        let mut pair = Pair::new(4096);
        for i in 1..=128 {
            pair.write(&[i as u8], i * stride, false);
        }
        let segments = if stride == 1 {
            128usize.div_ceil(MAX_FRAGMENTS)
        } else {
            128
        };
        assert_eq!(pair.tcp.out_of_order.len(), segments);

        // Promote all the contiguous case, or only a prefix of the gapped
        // case. Subsequent rightmost appends must use the updated cache.
        pair.write(&vec![0xaa; (64 * stride) as usize], 0, false);
        pair.read(17, false);
        for i in 129..=192 {
            pair.write(&[i as u8], 1024 + i * stride, false);
        }

        let final_size = (1024 + 193 * stride) as usize;
        pair.write(&vec![0xbb; final_size], 0, true);
        while pair.tcp.ready() {
            pair.read(37, false);
        }
        assert!(pair.tcp.is_fin());
        assert_eq!(pair.tcp.out_of_order.len(), 0);
    }
}

#[test]
fn coalescing_and_promotion_move_payload_handles() {
    let mut recv = RecvBuf::new(128, 128, 128);
    let first: RangeBuf = RangeBuf::from(b"abc", 3, false);
    let second: RangeBuf = RangeBuf::from(b"def", 6, true);
    let a = first.data.as_ref().as_ptr();
    let b = second.data.as_ref().as_ptr();
    recv.write(first).unwrap();
    recv.write(second).unwrap();
    let segment = recv.out_of_order.iter().next().unwrap();
    assert_eq!(segment.bufs[0].data.as_ref().as_ptr(), a);
    assert_eq!(segment.bufs[1].data.as_ref().as_ptr(), b);
    recv.write(RangeBuf::from(b"123", 0, false)).unwrap();
    assert_eq!(recv.data[1].data.as_ref().as_ptr(), a);
    assert_eq!(recv.data[2].data.as_ref().as_ptr(), b);
    let mut out = [0; 9];
    assert_eq!(recv.emit(&mut out), Ok((9, true)));
    assert_eq!(&out, b"123abcdef");
}

#[test]
fn overlapping_ranges_keep_existing_payload() {
    let mut pair = Pair::new(128);
    pair.write(b"abcdef", 6, false);
    pair.write(b"GHIJKL", 18, false);
    pair.write(b"0123456789abcdefghijklmnopqrstuvwxyz", 0, false);
    pair.write(b"xxxx", 4, false);
    pair.read(9, false);
    pair.write(b"YYYYYYYYYYYY", 6, false);
    pair.read(64, false);
    pair.write(b"discarded retransmission", 12, true);
    pair.read(0, false);
    pair.read(1, false);
}

#[test]
fn fin_reset_and_shutdown_match_baseline() {
    for mode in 0..6 {
        let mut pair = Pair::new(128);
        match mode {
            0 => pair.write(b"", 0, true),
            1 => pair.write(b"abc", 0, true),
            2 => {
                pair.write(b"abc", 0, true);
                pair.read(1, false);
            },
            3 => pair.write(b"c", 2, true),
            4 => {
                pair.write(b"abc", 0, true);
                pair.read(3, false);
            },
            5 => {
                pair.write(b"abc", 0, true);
                pair.shutdown();
            },
            _ => unreachable!(),
        }
        let final_size = pair.tcp.max_off();
        pair.reset(42, final_size);
        pair.reset(99, final_size);
        pair.read(0, false);
        pair.read(8, true);
        pair.write(b"", final_size, true);
        pair.read(1, false);
    }
    let mut pair = Pair::new(128);
    pair.write(b"abc", 0, false);
    pair.read(3, true);
    pair.write(b"abc", 0, true);
    pair.read(0, false);
    pair.write(b"abc", 0, true);
    pair.read(0, false);
}

#[test]
fn empty_frames_and_large_offsets_use_quic_final_size() {
    let mut pair = Pair::new(1 << 62);
    pair.write(b"", 1 << 40, false);
    pair.write(b"abc", (1 << 40) - 3, true);
    assert!(!pair.tcp.ready());
    assert_eq!(pair.tcp.max_off(), 1 << 40);
    pair.write(b"x", 1 << 40, false);
    pair.reset(7, (1 << 40) - 1);
    pair.reset(7, 1 << 40);
    pair.read(1, false);
    pair.shutdown();
    pair.write(b"", 1 << 40, true);
}

#[test]
fn partially_consumed_input_is_sliced_from_its_read_position() {
    let mut recv = RecvBuf::new(128, 128, 128);
    recv.write(RangeBuf::from(b"XY", 5, false)).unwrap();
    let mut buf = RangeBuf::from(b"0123456789", 0, false);
    buf.consume(2);
    recv.write(buf).unwrap();
    recv.write(RangeBuf::from(b"ab", 0, true)).unwrap_err();
    recv.write(RangeBuf::from(b"ab", 0, false)).unwrap();
    let mut out = [0; 10];
    assert_eq!(recv.emit(&mut out), Ok((10, false)));
    assert_eq!(&out, b"ab234XY789");
}

#[test]
fn receive_buffer_remains_send_and_sync() {
    fn check<T: Send + Sync>() {}
    check::<RecvBuf>();
}
