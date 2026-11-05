This is an experimental receive buffer for
[quiche issue #2855](https://github.com/cloudflare/quiche/issues/2855).
It uses the existing `intrusive_collections::RBTree` dependency, actual
`RangeBuf` payloads, and the same receive API as the BTreeMap implementation.
The default stream receive buffer remains `stream/recv_buf.rs`.

The design follows the queue organization in
[Linux TCP's receive path](https://github.com/torvalds/linux/blob/master/net/ipv4/tcp_input.c),
particularly `tcp_data_queue_ofo()`, `tcp_ofo_queue()`, and coalescing adjacent
segments.


| Concept | Payload reference |
| --- | --- |
| In-order receive queue | `VecDeque<RangeBuf>`; ordinary in-order writes bypass the tree |
| `rcv_nxt` | `contiguous_off`, the end of contiguous received data |
| `copied_seq` | `off`, the application's read offset |
| Out-of-order queue | Intrusive red-black tree of boxed segments, plus an owned rightmost cached segment |
| Cached tail coalescing | Append a `RangeBuf` descriptor directly to the owned tail |
| Cached tail insertion | An owning cursor retains the rightmost linked position, so spilling a full tail or appending after a gap needs no root lookup |
| Fragment bound | At most 17 descriptors per out-of-order segment (**work in progress**) |
| Gap closure | Remove consecutive segments with a single forward cursor and move their descriptors to the receive queue |
| Readability | Constant-time `ready()` and `readable_len()` |

Payloads are neither concatenated nor copied during coalescing or promotion.
Splitting an overlap clones the existing reference-counted payload handle;
`emit()` copies into the application's destination, while discard does not.
The receive queue keeps individual descriptors rather than reproducing the
kernel's skb allocation and coalescing machinery.

The rightmost segment is deliberately detached from the intrusive tree so
coalescing can mutate its payload descriptors through ordinary ownership.
A `CursorOwning` retains the rightmost linked node. When coalescing is impossible
because of a gap or the fragment limit, `insert_after()` links the previous tail
at that cached position and the cursor advances to the new maximum. **Both tail
paths avoid a root search**. Locating the insertion position is O(1); red-black
balancing can still take O(log n).

`CursorOwning` in intrusive-collections 0.10.3 is `Send` but not `Sync`. It is
stored in a `Mutex` solely to preserve the receive buffer's `Send + Sync`
properties. All access uses `Mutex::get_mut()` with exclusive `&mut self`;
there are no lock acquisitions in the implementation, including Debug formatting.
The receive code adds no unsafe blocks.

General insertion and gap closure temporarily recover the owned tree, then
refresh the cursor to the rightmost linked node before returning. Writes
overlapping the detached tail temporarily link it and take it back afterwards;
predecessor coalescing removes and reinserts its node. These slow paths may
traverse the tree; monotonically increasing rightmost writes do not take them.
Tests validate the cached position after writes, reads, resets, and shutdown,
including append sequences after partial and complete promotion.

QUIC behavior is retained: non-wrapping `u64` offsets, FIN without an extra
sequence byte, final-size validation, stream flow control, reset, shutdown,
partial reads, and discard. Existing payload wins on overlaps, including
conflicting bytes. Consequently this does not adopt Linux's replacement of
same-start segments or removal of fully covered successors when doing so
would replace previously buffered payload. TCP ACK/SACK bookkeeping and
receive-memory pruning are outside this reference.

The implementation is kept separate so the existing implementation remains a
stable comparison. Validation and lifecycle code must be kept in sync when
those semantics change; differential tests compare both implementations after
each operation, including emitted bytes and consumed flow-control credit.

To use the reference for actual QUIC streams, enable `recv-buf-tcp`:

```sh
cargo test -p quiche --all-targets --features internal,recv-buf-tcp,ffi,qlog
cargo test -p quiche --doc --features internal,recv-buf-tcp,ffi,qlog
cargo clippy -p quiche --all-targets --features internal,recv-buf-tcp,ffi,qlog -- -D warnings
cargo +nightly fmt -- --check
```

With `internal`, external benchmarks can import `quiche::RecvBuf`,
`quiche::TcpRecvBuf`, `quiche::RangeBuf`, and `quiche::RecvAction`. These exports
have the same unstable status as other `internal` APIs. `RecvBuf` always names
the BTreeMap baseline, regardless of the selected stream backend. Both buffer
constructors take `(max_data, initial_window, max_window)`. For example:

```rust
use quiche::RangeBuf;
use quiche::TcpRecvBuf;

let mut recv = TcpRecvBuf::new(65_536, 65_536, 65_536);
recv.write(RangeBuf::from(b"world", 5, true)).unwrap();
recv.write(RangeBuf::from(b"hello", 0, false)).unwrap();
let mut out = [0; 10];
assert_eq!(recv.emit(&mut out), Ok((10, true)));
assert_eq!(&out, b"helloworld");
```

The benchmark source is
[`quiche/benches/recv_buf.rs`](../../../benches/recv_buf.rs), relative to the
workspace root. The workspace directory is itself named `quiche`, and the
benchmark is inside its nested `quiche` crate directory. It is a new local file
until included in a commit. Cargo registers it as the `recv_buf` bench target.

This Criterion benchmark compares both buffers in the same executable:

```sh
# Validate all benchmark cases without collecting measurements.
cargo bench -p quiche --bench recv_buf --features internal -- --test

# Measure a specific scenario and size.
cargo bench -p quiche --bench recv_buf --features internal -- 'recv_buf/first_to_last/batch/emit/.*/1000$'

# Run the full matrix (several minutes).
cargo bench -p quiche --bench recv_buf --features internal
```

Inputs use 1200-byte chunks and deterministic ordering at 100, 1000, and 10000
packets. Cases cover in-order delivery, first-to-last reordering, reverse
order, shuffled order, delayed loss recovery, permanent gaps, non-coalescing
rightmost arrivals, and overlapping frames. Each case measures both emitting and discarding, with reads either
after all writes or every 32 writes. Throughput counts inserted payload bytes,
including overlaps; it is not delivered network goodput.

`rightmost_gaps` puts every chunk at the next higher offset with an intervening
gap, starting behind an initial gap. Every write takes the non-coalescing tail
append path and the linked tree keeps growing. No bytes become readable, so
this case measures insertion and destruction without output copying:

```sh
cargo bench -p quiche --bench recv_buf --features internal -- 'recv_buf/rightmost_gaps/batch/emit/.*/(1000|10000)$'
```

Initial payload allocation, cloning input descriptors, and destination
allocation happen outside the timed batch. Timed work includes writes,
internal allocations, reads or discards, and buffer destruction. The original
input retains payload references across iterations, so payload allocation and
final payload deallocation are not measured. Results describe receive-buffer
processing with prepared payloads, not packet parsing, cryptography, or full
connection throughput. The benchmark checks expected delivery before timing.

The ordering families correspond to the earlier `quick_resequencing` study,
but the traces are generated locally and are not identical to that suite's
traces. Its metadata-only `insert(offset, len)` and `emit_void()` adapter can
be replaced with `RangeBuf::from(payload, offset, fin)` and `emit()` or
`emit_or_discard()` using these internal exports. This workspace does not
modify that separate repository.

A suitable first PR title is **stream: add an experimental TCP-style receive
buffer with payload benchmarks**. The pitch is to evaluate queue organization,
coalescing, and the red-black tree together against the existing receive
buffer, with the normal backend unchanged. Benchmarks alone cannot attribute
any gain specifically to the tree type; a BTreeMap with the same queue fast
paths would be a useful further comparison before selecting a default.

A short local run on 2026-11-05 (macOS arm64, rustc 1.92.0, optimized Cargo
bench profile), after adding the cached append cursor, produced the following
Criterion estimates. Each iteration
inserts 1000 prepared 1200-byte chunks and emits them after all writes; the
settings are 30 samples, 1 second warm-up, and 2 seconds measurement per case.

| Input | BTreeMap (µs) | TCP reference (µs) | Baseline / reference time |
| --- | ---: | ---: | ---: |
| `in_order` | 91.91 | 35.06 | 2.62x |
| `first_to_last` | 91.98 | 51.90 | 1.77x |
| `shuffle` | 110.57 | 95.73 | 1.16x |
| `delayed_loss` | 94.33 | 52.80 | 1.79x |

These numbers cover this local buffer microbenchmark, including output copying.
They do not establish full-connection throughput or performance across other
machines and workload sizes. Raw Criterion output is in `quiche/target/criterion`
(relative to the workspace root). Reproduce this subset with:

```sh
cargo bench -p quiche --bench recv_buf --features internal -- 'recv_buf/(in_order|first_to_last|shuffle|delayed_loss)/batch/emit/.*/1000$'
```

The `rightmost_gaps` case was also measured before and after adding the cached
owning cursor, using the same Criterion settings. Both versions buffer all
chunks without emitting bytes. Times include queue destruction.

| Chunks | Backend | Before cached append (µs) | After cached append (µs) |
| ---: | --- | ---: | ---: |
| 1000 | `btree` | 45.70 | 48.57 |
| 1000 | `tcp` | 40.70 | 39.38 |
| 10000 | `btree` | 631.04 | 688.13 |
| 10000 | `tcp` | 508.93 | 412.33 |

The unchanged BTreeMap baseline also varied between these short runs, so the
numbers should be treated as local observations. The constant lookup property
comes from retaining the rightmost cursor and calling `insert_after()` at a
node with no right child, not from extrapolating these timings. The saved
before-change estimates are in each case's `before_cached_append` directory.

# Prevalent issues
- The cap of **17 `RangeBuf`s** is a modeling choice. Linux counts page-fragment descriptors, not incoming packets.
- Promotion currently expands segments into individual receive-queue `RangeBuf`s. It does not reproduce Linux’s receive-queue coalescing. Adjacent views of the same backing allocation could potentially become one `RangeBuf`, but that operation is not implemented.
- Possible Improvement: Adjacent views of the same backing allocation could potentially become one `RangeBuf`, but that operation is not implemented.
