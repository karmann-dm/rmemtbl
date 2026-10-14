# rmemtbl

[![CI](https://github.com/karmann-dm/rmemtbl/actions/workflows/build-pr.yaml/badge.svg)](https://github.com/karmann-dm/rmemtbl/actions/workflows/build-pr.yaml)
[![crates.io](https://img.shields.io/crates/v/rmemtbl.svg)](https://crates.io/crates/rmemtbl)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

The in-memory write buffer of an LSM storage engine, built on [rskipl](https://github.com/karmann-dm/rskipl), a lock-free, arena-backed MVCC skiplist.

A `MemtableSet` holds one active memtable that takes writes, plus a queue of frozen memtables waiting to be flushed to SSTables. It handles rotation, ties every memtable to the WAL segment it came from, and gives you snapshot reads across all of them.

```text
             writes
               |
          [ active ]            rotates at 90% full
               |
  frozen:  [ M3 ] [ M2 ] [ M1 ]   flushed oldest first, then retired
                           |
                       SSTables

  reads: active, then frozen newest to oldest, then SSTables
```

## Quick start

```rust
use memtable::{Lookup, MAX_SEQNO, MemtableError, MemtableSet, WalRoller};

// Your WAL. The set calls `roll` when it freezes a memtable.
impl WalRoller for Wal {
    fn roll(&self) -> std::io::Result<u64> {
        // seal the current segment, open the next one, return its id
    }
}

let set = MemtableSet::new(
    64 << 20,    // arena bytes per memtable
    1,           // WAL segment that is current right now
    4,           // frozen memtables allowed before writers should wait
    wal.clone(),
)?;

// The closure appends the record to the WAL. The set calls it at the right moment.
set.put(1, b"apple", b"red",   || wal.append(1, b"apple", Some(b"red")))?;
set.put(2, b"apple", b"green", || wal.append(2, b"apple", Some(b"green")))?;
set.delete(3, b"apple",        || wal.append(3, b"apple", None))?;

// Reads take a snapshot seqno and a closure that sees the borrowed value.
let v = set.get(2, b"apple", |l| match l {
    Lookup::Value(v) => Some(v.to_vec()),
    Lookup::Tombstone => None,
});
assert_eq!(v, Some(Some(b"green".to_vec())));
assert_eq!(set.get(MAX_SEQNO, b"apple", |l| l == Lookup::Tombstone), Some(true)); // deleted
assert!(set.get(0, b"apple", |_| ()).is_none());                         // not written yet
```

A runnable version lives in `examples/readme.rs` (`cargo run --example readme`).

## Using it in an engine

The set owns the data structures. Your engine drives it from three places: writers, a flusher and readers.

### 1. Writers

```rust
if set.needs_backpressure() {
    // flusher is behind: stall, sleep, or return Busy to the client
}
let seq = seqgen.next();
let rotated = set.put(seq, key, value, || wal.append(seq, key, Some(value)))?;
if rotated {
    flusher.wake();
}
```

What `put` does internally, in this order:

1. Pin the active memtable, so it can't be flushed while the write is in progress.
2. Check that the entry fits. If it doesn't, rotate first and pin the new memtable.
3. Call your `log` closure. Since the memtable is already pinned, its WAL tag is never newer than the segment your record went to.
4. Insert into the skiplist.
5. Unpin, and rotate if this write crossed the freeze watermark.

`log` usually runs exactly once. If another writer eats the space between steps 2 and 4, the write moves to a fresh memtable and `log` runs again. That's safe, because replaying the same `(key, seq)` twice upserts in place.

### 2. The flusher

Always flush oldest first:

```rust
while let Some(mt) = set.oldest_frozen() {
    mt.wait_for_writers();                    // contents are final after this
    if !mt.is_empty() {
        let sst = build_sstable(&mt)?;        // via mt.for_each_entry(..)
        install(sst)?;                        // durable AND visible to readers
    }
    set.retire_oldest(&mt);                   // only after install
    wal.delete_segments_up_to(mt.wal_segment());
}
```

`for_each_entry` yields `(key, seqno, Option<value>)` already in SSTable order: key ascending, seqno descending, tombstones included (`None`). Write them straight out.

A frozen memtable can be empty. A failed insert can use up arena space without linking a node, and that memtable still has to rotate. Skip the SST for it, but still retire it and truncate the WAL.

### 3. Readers

```rust
let mem = set.get(snapshot, key, |l| /* copy or use the value */);
let disk = sstables.get(snapshot, key);
// take whichever hit has the higher seqno
```

Check the memtables before the SSTables. If you do it the other way round, a flush can install and retire between your two lookups, and the key disappears from both.

## Semantics

| Topic | Behaviour |
| --- | --- |
| Ordering | Bytewise key ascending, seqno descending. |
| Snapshot read | `get(s, k)` returns the version of `k` with the highest seqno `<= s`, across every memtable. |
| Out-of-order seqnos | Seqnos are handed out before the insert, so a lower seqno can land in a newer memtable. `get` still returns the highest one. It costs one atomic load per older memtable in the normal case. |
| Same `(key, seq)` twice | Last write wins: upsert in place within one memtable, newest memtable across several. |
| Tombstone vs absent | `Some(Lookup::Tombstone)` means deleted, so stop searching. `None` means not here, so go on to the SSTables. |
| Empty values | `put(k, b"")` stores an empty value. Only `delete` makes a tombstone. |
| Snapshot rule | Only read at seqnos whose writes have all completed (publish the visible seqno after `put` returns). `MAX_SEQNO` is fine for "latest". |

## Errors

| Error | When | What to do |
| --- | --- | --- |
| `EntryTooLarge` | Key + value + node overhead exceeds `max_entry_size()` (about 10% of capacity) | Nothing was logged. Reject it, or move large values to a blob log. |
| `CapacityTooSmall` | Capacity below `MIN_CAPACITY` (4096) | Configuration bug. |
| `Wal { logged: false, .. }` | `roll()` failed before your record was logged | Nothing happened. Safe to retry or report. |
| `Wal { logged: true, .. }` | `roll()` failed after your record was logged | The outcome is unknown, because the write will come back on WAL replay. Treat it like a timeout and stop taking writes until the WAL is healthy. |
| your `E` | Your `log` closure failed | Passed through unchanged. Nothing was inserted. |

`MemtableError::Full` is internal to the retry loop, and you won't see it from `MemtableSet`.

## Sizing

- The arena holds key bytes, value bytes, a 24-byte node header, a tower of 4 bytes per level (up to 20 levels), and alignment padding. Small entries carry a noticeable overhead, so a 64 MiB arena holds well under 64 MiB of user data.
- Rotation starts at 90% full. The remaining 10% is headroom for writes that are already in flight, which is also where `max_entry_size()` comes from.
- One arena is capped at about 4 GiB (rskipl uses `u32` offsets).
- `needs_backpressure()` is advisory. A few writers can slip past `max_frozen` between the check and the write.

## Guarantees

The design relies on four invariants, and each one has a test.

A flush never loses a write. A memtable can only be pinned while it is active, and `wait_for_writers` blocks until every writer that pinned it has finished.

WAL truncation is safe. Every record lives in a segment `>=` the `wal_segment()` of the memtable holding its data, so once memtable `k` and everything older is installed, segments `<= k` can go.

Retirement is strictly FIFO. `retire_oldest` panics if you pass anything but the oldest frozen memtable. It checks before removing anything, so a bad call doesn't lose data or poison the lock.

A full arena can't cause a livelock. An entry within `max_entry_size()` always fits in a fresh memtable, and a full memtable always rotates, even when it's empty.

## Gotchas

- `wait_for_writers` spins and yields. Only call it on frozen memtables. On the active one it may never return.
- Your `log` closure runs while the memtable is pinned. A slow fsync there delays that memtable's flush, which is fine but worth knowing when you tune group commit.
- `WalRoller::roll` runs under the set's write lock. Keep it short, and don't panic in it: a panic poisons the lock.
- Seqnos up to `u64::MAX` need rskipl 0.1.4 or later. 0.1.3 still has a 56-bit `debug_assert`, so debug builds panic above `2^56 - 1`.

## Testing

```sh
cargo test                     # 46 tests, about 2 s
cargo test --release           # the stress tests are more aggressive here
cargo clippy --all-targets
```

The suite covers the basics (tombstones, empty keys and values, binary and prefix keys, seq 0 and `u64::MAX`), snapshot reads with versions spread over several memtables and out-of-order seqnos, the exact entry size limit, tiny capacities, and every failure path: WAL roll errors in both `logged` cases, failing or panicking `log` closures, and bad `retire_oldest` calls.

There is also a model test that runs 20k random operations with jittered seqnos across 20+ rotations and compares every read against a `BTreeMap`. The stress tests run concurrent writers that check the WAL segment invariant, a hot key under 8 threads, and a mix of writers, readers and a flusher where readers must never see a value older than the last completed write.
