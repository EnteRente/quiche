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

use std::hint::black_box;
use std::time::Duration;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BatchSize;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;

use quiche::RangeBuf;
use quiche::RecvAction;
use quiche::RecvBuf;
use quiche::TcpRecvBuf;

const PACKET_SIZE: usize = 1200;
const READ_SIZE: usize = 16 * 1024;

trait Buffer {
    fn new() -> Self;
    fn write(&mut self, buf: RangeBuf);
    fn drain(&mut self, out: &mut [u8], discard: bool) -> usize;
}

macro_rules! buffer {
    ($ty:ty) => {
        impl Buffer for $ty {
            fn new() -> Self {
                Self::new(1 << 30, 1 << 20, 1 << 24)
            }

            fn write(&mut self, buf: RangeBuf) {
                self.write(buf).unwrap();
            }

            fn drain(&mut self, out: &mut [u8], discard: bool) -> usize {
                let mut total = 0;
                loop {
                    let result = if discard {
                        self.emit_or_discard(RecvAction::<&mut [u8]>::Discard {
                            len: out.len(),
                        })
                    } else {
                        self.emit(out)
                    };
                    match result {
                        Ok((len, _)) => total += len,
                        Err(quiche::Error::Done) => break,
                        Err(e) => panic!("unexpected read error: {e:?}"),
                    }
                }
                black_box(out);
                total
            }
        }
    };
}

buffer!(RecvBuf);
buffer!(TcpRecvBuf);

fn input(scheme: &str, count: usize) -> (Vec<RangeBuf>, usize) {
    let mut order: Vec<_> = (0..count).collect();
    let mut expected = count * PACKET_SIZE;
    match scheme {
        "in_order" | "overlaps" => (),
        "first_to_last" => order.rotate_left(1),
        "reverse" => order.reverse(),
        "shuffle" => {
            let mut state = 42u64;
            for i in (1..count).rev() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                order.swap(i, state as usize % (i + 1));
            }
        },
        "delayed_loss" => {
            // Delay every sixteenth packet, then deliver it as a retransmit.
            let delayed: Vec<_> =
                order.iter().copied().filter(|i| i % 16 == 15).collect();
            order.retain(|i| i % 16 != 15);
            order.extend(delayed);
        },
        "permanent_gaps" => {
            order.retain(|i| i % 16 != 15);
            expected = 15 * PACKET_SIZE;
        },
        "rightmost_gaps" => {
            // Every arrival is the new rightmost range, with no coalescing
            // possible. Leave the initial gap open so the tree keeps growing.
            order.iter_mut().for_each(|i| *i = 2 * *i + 1);
            expected = 0;
        },
        _ => unreachable!(),
    }
    let frames = order
        .into_iter()
        .map(|i| {
            let len = if scheme == "overlaps" && i + 1 < count {
                PACKET_SIZE + PACKET_SIZE / 2
            } else {
                PACKET_SIZE
            };
            RangeBuf::from(&vec![0x5a; len], (i * PACKET_SIZE) as u64, false)
        })
        .collect();
    (frames, expected)
}

fn run<B: Buffer>(
    mut recv: B, input: Vec<RangeBuf>, mut out: Vec<u8>, batch: usize,
    discard: bool,
) -> usize {
    let mut total = 0;
    for (i, buf) in input.into_iter().enumerate() {
        recv.write(buf);
        if (i + 1) % batch == 0 {
            total += recv.drain(&mut out, discard);
        }
    }
    total += recv.drain(&mut out, discard);
    black_box(recv);
    total
}

fn bench(c: &mut Criterion) {
    for count in [100, 1_000, 10_000] {
        for scheme in [
            "in_order",
            "first_to_last",
            "reverse",
            "shuffle",
            "delayed_loss",
            "permanent_gaps",
            "rightmost_gaps",
            "overlaps",
        ] {
            let (input, expected) = input(scheme, count);
            let bytes: usize = input.iter().map(RangeBuf::len).sum();
            for (read_mode, batch) in [("batch", usize::MAX), ("stream", 32)] {
                for (action, discard) in [("emit", false), ("discard", true)] {
                    let mut group = c.benchmark_group(format!(
                        "recv_buf/{scheme}/{read_mode}/{action}"
                    ));
                    group.throughput(Throughput::Bytes(bytes as u64));
                    macro_rules! measure {
                        ($name:literal, $ty:ty) => {
                            // Validate delivery before entering the timed loop.
                            assert_eq!(run(<$ty as Buffer>::new(), input.clone(),
                                vec![0; READ_SIZE], batch, discard), expected);
                            group.bench_with_input(
                                BenchmarkId::new($name, count), &input,
                                |b, input| b.iter_batched(
                                    || (<$ty as Buffer>::new(), input.clone(),
                                        vec![0; READ_SIZE]),
                                    |(recv, frames, out)| black_box(run(
                                        recv, frames, out, batch, discard,
                                    )),
                                    BatchSize::LargeInput,
                                ),
                            );
                        };
                    }
                    measure!("btree", RecvBuf);
                    measure!("tcp", TcpRecvBuf);
                    group.finish();
                }
            }
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(2));
    targets = bench
}
criterion_main!(benches);
