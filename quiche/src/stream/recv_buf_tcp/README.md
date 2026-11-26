# Red-black tree stream receive buffer

Enable `recv-buf-tcp` to use this buffer for QUIC streams. The default
receive buffer uses a `BTreeMap`.

Contiguous data goes directly into a `VecDeque<RangeBuf>`. A gap sends
data to an intrusive red-black tree ordered by stream offset. Closing
the gap moves consecutive segments into the read queue. The difference
between the contiguous end offset and the application's read offset gives
the readable byte count in constant time.

The rightmost segment is held separately so increasing offsets can extend
it in place. Each segment holds up to 17 adjacent `RangeBuf` descriptors
that retain their payload allocations. When a gap or the descriptor limit
requires a new tail, a cursor at the rightmost linked node supplies the
insertion point. Tree rebalancing can still take O(log n). General
insertion keeps the first payload received at each offset.

## Tests and benchmarks

The differential tests feed both buffers the same writes, reads, discards,
resets and shutdowns. They compare emitted bytes, readiness, final size and
flow control credit. This command runs the stream tests with the feature
enabled.

```sh
cargo test -p quiche --features recv-buf-tcp --lib stream::
```

The Criterion benchmark compares both buffers in one executable. Workloads
use 1,200-byte chunks with in-order delivery, reordering and gaps. The
overlap workload extends each chunk except the last by 600 bytes.
Each workload emits or discards data after all writes or every 32 writes.
Timing includes buffer operations and destruction. Payload preparation and
destination allocation happen before timing. Throughput counts inserted
payload bytes, including overlaps.

```sh
cargo bench -p quiche --bench recv_buf --features internal -- --test
cargo bench -p quiche --bench recv_buf --features internal -- 'recv_buf/(in_order|shuffle)/batch/emit/.*/1000$'
```
