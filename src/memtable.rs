//! Memtable backed by rskipl SkipList
//! Write protocol (see `MemtableSet::put`)
//!     1. pin the active memtable
//!     2. make sure the entry fits, rotating first if it doesn't
//!     3. log the record to the WAL
//!     4. insert into the pinned memtable
//!     5. unpin
//! Pinning before logging gives the invariant
//! the WAL truncation rule relies on - every record lives
//! in a WAL segment >= the `wal_segment` tag of the memtable holding
//! its data. So once memtable K and everything older is flushed and
//! installed, every segment <= K can be deleted.

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use rskipl::{Error as SkipError, MAX_HEIGHT, SkipList};

/// Monotonic seq number, doubling as the mvcc version
pub type Seqno = u64;

/// Read everything snapshot
pub const MAX_SEQNO: Seqno = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lookup<'a> {
    Value(&'a [u8]),
    Tombstone,
}

#[derive(Debug, Clone)]
pub enum MemtableError {
    /// The arena ran out, internal to the retry loop, callers never see it.
    Full,

    /// The entry can never fit, see `MemtableSet::max_entry_size`.
    EntryTooLarge,

    /// Arena too small to hold even one minimal entry above the head node.
    CapacityTooSmall,

    /// Rolling the WAL to a new segment failed.
    ///
    /// `logged == false`: the write was not applied anywhere, safe to retry or report.
    /// `logged == true`: the record is already in the WAL but not in the memtable.
    /// The outcome is indeterminate (it comes back on replay), treat it like a
    /// timeout and stop accepting writes until the WAL is healthy.
    Wal { err: Arc<io::Error>, logged: bool },
}

impl From<SkipError> for MemtableError {
    fn from(e: SkipError) -> Self {
        match e {
            SkipError::Full => MemtableError::Full,
            SkipError::TooLarge => MemtableError::EntryTooLarge,
        }
    }
}

/// Smallest arena that leaves room for the head node plus one minimal entry
/// inside the headroom above the freeze watermark.
pub const MIN_CAPACITY: u32 = 4096;

/// Upper bound on the arena bytes one insert can consume: key and value bytes,
/// a node with the tallest possible tower and worst-case alignment padding.
#[inline]
fn entry_footprint(key_len: usize, value_len: usize) -> usize {
    const NODE_HEADER: usize = 24;
    const MAX_PADDING: usize = 7;
    key_len + value_len + NODE_HEADER + 4 * MAX_HEIGHT + MAX_PADDING
}

/// Seals the current WAL segment and opens the next one
pub trait WalRoller: Send + Sync {
    /// Returns the ID of the newly opened segment. Called with the memtable set's
    /// write lock held, exactly once per rotation, so wal and memtable generation always advance together.
    fn roll(&self) -> io::Result<u64>;
}

pub struct Memtable {
    list: SkipList,
    /// WAL segment that was current when this memtable became active
    wal_segment: u64,
    /// Rotate once the arena is this full. Everything above is headroom reserved for in-flight writes.
    freeze_watermark: usize,
    max_seqno: AtomicU64,
    /// Writers that pinned this memtable and haven't finished yet.
    writers: AtomicUsize,
}

impl Memtable {
    /// `capacity` is the arena size in bytes. Node towers, k/v bytes and alignment padding all
    /// come out of it, so a 64MiB arena holds noticeably less than 64MiB of user data.
    pub fn new(capacity: u32, wal_segment: u64) -> Result<Self, MemtableError> {
        if capacity < MIN_CAPACITY {
            return Err(MemtableError::CapacityTooSmall);
        }
        Ok(Self {
            list: SkipList::new(capacity)?,
            wal_segment,
            freeze_watermark: (capacity as usize / 10) * 9,
            max_seqno: AtomicU64::new(0),
            writers: AtomicUsize::new(0),
        })
    }

    #[inline]
    pub fn wal_segment(&self) -> u64 {
        self.wal_segment
    }

    #[inline]
    pub fn delete(&self, seq: Seqno, key: &[u8]) -> Result<(), MemtableError> {
        self.list.remove(seq, key)?;
        self.max_seqno.fetch_max(seq, Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn put(&self, seq: Seqno, key: &[u8], value: &[u8]) -> Result<(), MemtableError> {
        self.list.insert(seq, key, value)?;
        self.max_seqno.fetch_max(seq, Ordering::Release);
        Ok(())
    }

    /// Newest version of key at or below snapshot, with its seqno.
    pub fn get(&self, snapshot: Seqno, key: &[u8]) -> Option<(Seqno, Lookup<'_>)> {
        let entry = self.list.get(snapshot, key)?;
        let lookup = match entry.value {
            Some(v) => Lookup::Value(v),
            None => Lookup::Tombstone,
        };
        Some((entry.version, lookup))
    }

    /// Every entry in SSTable order: key ascending, seqno descending, tombstones included.
    /// Call `wait_for_writers` first when flushing.
    pub fn for_each_entry(&self, mut f: impl FnMut(&[u8], Seqno, Option<&[u8]>)) {
        for e in self.list.iter_all() {
            f(e.key, e.version, e.value);
        }
    }

    /// Blocks until every writer that pinned this memtable has finished. Once a memtable is
    /// frozen nobody can pin it again, so after this returns its contents are final and `for_each_entry` sees all of 'em.
    pub fn wait_for_writers(&self) {
        while self.writers.load(Ordering::Acquire) != 0 {
            std::thread::yield_now();
        }
    }

    #[inline]
    pub fn approximate_size(&self) -> usize {
        self.list.allocated()
    }

    #[inline]
    pub fn should_freeze(&self) -> bool {
        self.list.allocated() >= self.freeze_watermark
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.list.len()
    }

    #[inline]
    pub fn max_seqno(&self) -> Seqno {
        self.max_seqno.load(Ordering::Acquire)
    }
}

/// Keeps a memtable pinned for the duration of one write
struct Pin(Arc<Memtable>);

impl Drop for Pin {
    fn drop(&mut self) {
        // Release pairs with the Acquire in wait_for_writers, so the flusher sees everything
        // this writer inserted.
        self.0.writers.fetch_sub(1, Ordering::Release);
    }
}

struct Inner {
    active: Arc<Memtable>,
    /// Oldest first, flushed and retired strictly in this order
    frozen: VecDeque<Arc<Memtable>>,
}

pub struct MemtableSet {
    inner: RwLock<Inner>,
    capacity: u32,
    max_frozen: usize,
    max_entry: usize,
    wal: Arc<dyn WalRoller>,
}

impl MemtableSet {
    /// `wal_segment` is the WAL segment that is current right now.
    pub fn new(
        capacity: u32,
        wal_segment: u64,
        max_frozen: usize,
        wal: Arc<dyn WalRoller>,
    ) -> Result<Self, MemtableError> {
        let first = Memtable::new(capacity, wal_segment)?;
        let max_entry = capacity as usize - first.freeze_watermark;

        Ok(Self {
            inner: RwLock::new(Inner {
                active: Arc::new(first),
                frozen: VecDeque::new(),
            }),
            capacity,
            max_frozen,
            max_entry,
            wal,
        })
    }

    /// Largest arena footprint a single entry may have. An entry this size still fits into any memtable below its freeze watermarks,
    /// which is what rules out endless retries. Anything bigger belongs in a blob/value log.
    pub fn max_entry_size(&self) -> usize {
        self.max_entry
    }

    /// Writes `key=value` at `seq`.
    ///
    /// `log` must append the record to the WAL. It runs after the target memtable is pinned
    /// and after a capacity check, so it runs once in the common case. Under a race with
    /// other writers it may run again for a fresh memtable; replay is idempotent because
    /// the same `(key, seq)` upserts in place.
    ///
    /// Returns `true` when this call froze a memtable, as a hint to wake the flusher.
    pub fn put<E>(
        &self,
        seq: Seqno,
        key: &[u8],
        value: &[u8],
        log: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E>
    where
        E: From<MemtableError>,
    {
        self.write_with(entry_footprint(key.len(), value.len()), log, |mt| {
            mt.put(seq, key, value)
        })
    }

    pub fn delete<E>(
        &self,
        seq: Seqno,
        key: &[u8],
        log: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E>
    where
        E: From<MemtableError>,
    {
        self.write_with(entry_footprint(key.len(), 0), log, |mt| mt.delete(seq, key))
    }

    fn write_with<E>(
        &self,
        footprint: usize,
        mut log: impl FnMut() -> Result<(), E>,
        op: impl Fn(&Memtable) -> Result<(), MemtableError>,
    ) -> Result<bool, E>
    where
        E: From<MemtableError>,
    {
        if footprint > self.max_entry {
            return Err(MemtableError::EntryTooLarge.into());
        }
        let mut rotated = false;
        let mut logged = false;

        loop {
            let pin = self.pin_active();

            // Rotate *before* logging when the entry clearly won't fit, so a WAL
            // failure here leaves nothing behind.
            if pin.0.list.remaining() < footprint {
                let mt = Arc::clone(&pin.0);
                drop(pin);
                rotated |= self.rotate(&mt).map_err(|e| with_logged(e, logged))?;
                continue;
            }

            log()?;
            logged = true;

            match op(&pin.0) {
                Ok(()) => {
                    // The write landed but pushed the arena past the watermark. Rotate
                    // now so the next write goes to a fresh memtable. The write itself
                    // already succeeded, so a failed roll is not its error: the next
                    // writer retries the rotation.
                    if pin.0.should_freeze() {
                        let mt = Arc::clone(&pin.0);
                        drop(pin);
                        rotated |= self.rotate(&mt).unwrap_or(false);
                    }
                    return Ok(rotated);
                }
                Err(MemtableError::Full) => {
                    // Concurrent writers ate the space between the check and the insert.
                    // Nothing was linked (rskipl publishes only after every allocation
                    // succeeds), so rotate, re-log, retry. Can't spin forever: Full means
                    // the arena is past its watermark, so rotate() always freezes it.
                    let mt = Arc::clone(&pin.0);
                    drop(pin);
                    rotated |= self.rotate(&mt).map_err(|e| with_logged(e, logged))?;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn pin_active(&self) -> Pin {
        let inner = self.inner.read().unwrap();
        inner.active.writers.fetch_add(1, Ordering::Relaxed);
        Pin(Arc::clone(&inner.active))
    }

    /// Freezes `full` if it is still the active memtable and past its watermark.
    /// Returns false when another thread already rotated it away.
    ///
    /// The gate is the watermark, not `is_empty()`: a failed insert consumes key
    /// and value bytes without linking a node, so a memtable can be exhausted with
    /// `len() == 0`. Such a memtable is frozen too; the flusher skips writing an
    /// SST for it but still retires it and truncates the WAL.
    fn rotate(&self, full: &Arc<Memtable>) -> Result<bool, MemtableError> {
        let mut inner = self.inner.write().unwrap();

        if !Arc::ptr_eq(&inner.active, full) || !inner.active.should_freeze() {
            return Ok(false);
        }

        let segment = self.wal.roll().map_err(|e| MemtableError::Wal {
            err: Arc::new(e),
            logged: false,
        })?;
        let fresh = Arc::new(Memtable::new(self.capacity, segment)?);
        let old = std::mem::replace(&mut inner.active, fresh);
        inner.frozen.push_back(old);
        Ok(true)
    }

    /// Newest version of `key` at or below `snapshot`, across all memtables.
    ///
    /// Seqnos are assigned before the insert, so a lower seqno can land in a
    /// newer memtable than a higher one. After a hit at version `v`, older
    /// memtables are still checked whenever their `max_seqno` exceeds `v`.
    /// Usually it doesn't and the check is a single atomic load.
    ///
    /// `snapshot` must come from a seqno published only after the writes up to it
    /// have completed.
    pub fn get<R>(
        &self,
        snapshot: Seqno,
        key: &[u8],
        f: impl FnOnce(Lookup<'_>) -> R,
    ) -> Option<R> {
        let tables: Vec<Arc<Memtable>> = {
            let inner = self.inner.read().unwrap();
            std::iter::once(&inner.active)
                .chain(inner.frozen.iter().rev())
                .cloned()
                .collect()
        };

        let mut best: Option<(Seqno, Lookup<'_>)> = None;
        for mt in &tables {
            if let Some((v, _)) = best
                && mt.max_seqno() <= v
            {
                continue;
            }
            if let Some((v, hit)) = mt.get(snapshot, key)
                && best.is_none_or(|(b, _)| v > b)
            {
                best = Some((v, hit))
            }
        }
        best.map(|(_, hit)| f(hit))
    }

    /// The memtable to flush next, oldest first. The flush loop is:
    ///
    /// ```text
    /// while let Some(mt) = set.oldest_frozen() {
    ///     mt.wait_for_writers();
    ///     install(build_sstable(&mt))?;   // durable and visible to readers
    ///     set.retire_oldest(&mt);
    ///     wal.delete_segments_up_to(mt.wal_segment());
    /// }
    /// ```
    pub fn oldest_frozen(&self) -> Option<Arc<Memtable>> {
        self.inner.read().unwrap().frozen.front().cloned()
    }

    /// Drops the oldest frozen memtable. Its SSTable must already be installed,
    /// otherwise readers that start after this call miss its data.
    ///
    /// # Panics
    /// If `flushed` is not the oldest frozen memtable. Out-of-order retirement
    /// would make WAL truncation by segment id unsafe.
    pub fn retire_oldest(&self, flushed: &Arc<Memtable>) {
        debug_assert_eq!(
            flushed.writers.load(Ordering::Acquire),
            0,
            "retire before wait_for_writers"
        );
        let mut inner = self.inner.write().unwrap();
        // Check before popping and panic only after releasing the lock, so a bad
        // call neither drops the real oldest memtable nor poisons the set.
        let matches = inner
            .frozen
            .front()
            .map(|front| Arc::ptr_eq(front, flushed));
        if matches == Some(true) {
            inner.frozen.pop_front();
            return;
        }
        drop(inner);
        match matches {
            None => panic!("no frozen memtable to retire"),
            _ => panic!("memtables must be retired oldest first"),
        }
    }

    pub fn frozen_count(&self) -> usize {
        self.inner.read().unwrap().frozen.len()
    }

    pub fn active_size(&self) -> usize {
        self.inner.read().unwrap().active.approximate_size()
    }

    /// True when the flusher is far enough behind that writers should wait.
    /// Advisory: check it before `put`, a few writers may slip past the limit.
    pub fn needs_backpressure(&self) -> bool {
        self.frozen_count() >= self.max_frozen
    }
}

fn with_logged(e: MemtableError, logged: bool) -> MemtableError {
    match e {
        MemtableError::Wal { err, .. } => MemtableError::Wal { err, logged },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::time::Duration;

    const CAP: u32 = 1 << 20;

    /// In-memory WAL: remembers which segment every record was logged to.
    #[derive(Default)]
    struct TestWal {
        current: AtomicU64,
        records: Mutex<Vec<(Vec<u8>, Seqno, u64)>>,
    }

    impl TestWal {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                current: AtomicU64::new(1),
                ..Default::default()
            })
        }
        fn append(&self, key: &[u8], seq: Seqno) -> Result<(), MemtableError> {
            // Real WALs serialise appends against roll with their own lock.
            let mut recs = self.records.lock().unwrap();
            let seg = self.current.load(Ordering::SeqCst);
            recs.push((key.to_vec(), seq, seg));
            Ok(())
        }
    }

    impl WalRoller for TestWal {
        fn roll(&self) -> io::Result<u64> {
            let _recs = self.records.lock().unwrap();
            Ok(self.current.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    fn set_with(cap: u32, max_frozen: usize) -> (MemtableSet, Arc<TestWal>) {
        let wal = TestWal::new();
        let set = MemtableSet::new(cap, 1, max_frozen, wal.clone()).unwrap();
        (set, wal)
    }

    fn put(set: &MemtableSet, seq: Seqno, key: &[u8], value: &[u8]) -> bool {
        set.put(seq, key, value, || Ok::<_, MemtableError>(()))
            .unwrap()
    }

    fn value_of(set: &MemtableSet, snapshot: Seqno, key: &[u8]) -> Option<Option<Vec<u8>>> {
        set.get(snapshot, key, |l| match l {
            Lookup::Value(v) => Some(v.to_vec()),
            Lookup::Tombstone => None,
        })
    }

    fn fill_until_rotation(set: &MemtableSet, seq: &mut Seqno) {
        let before = set.frozen_count();
        while set.frozen_count() == before {
            put(set, *seq, format!("fill{seq:08}").as_bytes(), &[0u8; 200]);
            *seq += 1;
        }
    }

    #[test]
    fn put_and_get_through_the_set() {
        let (set, _) = set_with(CAP, 4);
        put(&set, 1, b"alpha", b"one");
        put(&set, 2, b"beta", b"two");

        assert_eq!(value_of(&set, 5, b"alpha"), Some(Some(b"one".to_vec())));
        assert_eq!(value_of(&set, 5, b"beta"), Some(Some(b"two".to_vec())));
        assert_eq!(value_of(&set, 5, b"gamma"), None);
    }

    #[test]
    fn tombstone_reaches_the_caller() {
        let (set, _) = set_with(CAP, 4);
        put(&set, 1, b"k", b"v");
        set.delete(2, b"k", || Ok::<_, MemtableError>(())).unwrap();

        assert_eq!(value_of(&set, 1, b"k"), Some(Some(b"v".to_vec())));
        assert_eq!(value_of(&set, 2, b"k"), Some(None));
        assert_eq!(value_of(&set, 2, b"never-written"), None);
    }

    #[test]
    fn max_seqno_is_usable_as_a_snapshot() {
        let (set, _) = set_with(CAP, 4);
        put(&set, 1, b"k", b"v");
        assert_eq!(value_of(&set, MAX_SEQNO, b"k"), Some(Some(b"v".to_vec())));
    }

    #[test]
    fn full_arena_rotates_and_data_stays_readable() {
        let (set, _) = set_with(16 * 1024, 64);
        let payload = vec![b'z'; 200];
        for i in 0..400u64 {
            put(&set, i + 1, format!("key{i:06}").as_bytes(), &payload);
        }
        assert!(set.frozen_count() > 0, "arena never rotated");
        for i in 0..400u64 {
            let key = format!("key{i:06}");
            assert_eq!(
                value_of(&set, MAX_SEQNO, key.as_bytes()),
                Some(Some(payload.clone())),
                "lost {key} across rotation"
            );
        }
    }

    #[test]
    fn retire_is_fifo() {
        let (set, _) = set_with(8 * 1024, 64);
        let mut seq = 1;
        fill_until_rotation(&set, &mut seq);
        fill_until_rotation(&set, &mut seq);
        let before = set.frozen_count();

        let first = set.oldest_frozen().unwrap();
        set.retire_oldest(&first);
        assert_eq!(set.frozen_count(), before - 1);
        assert!(!Arc::ptr_eq(&set.oldest_frozen().unwrap(), &first));
    }

    #[test]
    #[should_panic(expected = "oldest first")]
    fn retiring_out_of_order_panics() {
        let (set, _) = set_with(8 * 1024, 64);
        let mut seq = 1;
        fill_until_rotation(&set, &mut seq);
        fill_until_rotation(&set, &mut seq);
        let second = set.inner.read().unwrap().frozen[1].clone();
        set.retire_oldest(&second);
    }

    #[test]
    fn backpressure_trips_at_the_limit() {
        let (set, _) = set_with(8 * 1024, 2);
        assert!(!set.needs_backpressure());
        let mut seq = 1;
        fill_until_rotation(&set, &mut seq);
        fill_until_rotation(&set, &mut seq);
        assert!(set.needs_backpressure());
    }

    #[test]
    fn flush_order_matches_sstable_order() {
        let mt = Memtable::new(CAP, 1).unwrap();
        mt.put(1, b"b", b"b1").unwrap();
        mt.put(3, b"a", b"a3").unwrap();
        mt.delete(2, b"b").unwrap();
        mt.put(1, b"a", b"a1").unwrap();

        let mut seen = Vec::new();
        mt.for_each_entry(|k, v, val| {
            seen.push((String::from_utf8(k.to_vec()).unwrap(), v, val.is_some()))
        });
        assert_eq!(
            seen,
            vec![
                ("a".into(), 3, true),
                ("a".into(), 1, true),
                ("b".into(), 2, false),
                ("b".into(), 1, true),
            ]
        );
    }

    #[test]
    fn single_memtable_matches_a_btreemap_model() {
        let mt = Memtable::new(8 << 20, 1).unwrap();
        type Model = BTreeMap<(Vec<u8>, std::cmp::Reverse<u64>), Option<Vec<u8>>>;
        let mut model: Model = BTreeMap::new();
        let mut x: u64 = 0xDEAD_BEEF_CAFE_1234;
        let mut rng = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for i in 0..10_000u64 {
            let key = format!("k{:04}", rng() % 400).into_bytes();
            let seq = (i % 40) + 1;
            if rng() % 6 == 0 {
                mt.delete(seq, &key).unwrap();
                model.insert((key, std::cmp::Reverse(seq)), None);
            } else {
                let val = format!("v{i}").into_bytes();
                mt.put(seq, &key, &val).unwrap();
                model.insert((key, std::cmp::Reverse(seq)), Some(val));
            }
        }
        let mut got = Vec::new();
        mt.for_each_entry(|k, v, val| got.push((k.to_vec(), v, val.map(|b| b.to_vec()))));
        let want: Vec<_> = model
            .iter()
            .map(|((k, std::cmp::Reverse(v)), val)| (k.clone(), *v, val.clone()))
            .collect();
        assert_eq!(got, want);
    }

    // ---- regression tests for the review findings

    /// #1: a writer that pinned a memtable before it froze holds the flush back,
    /// and its write is visible to the flush once it finishes.
    #[test]
    fn flush_waits_for_writers_pinned_before_rotation() {
        let (set, _) = set_with(8 * 1024, 64);
        let set = Arc::new(set);

        let pin = set.pin_active();
        let mut seq = 1;
        fill_until_rotation(&set, &mut seq);
        let frozen = set.oldest_frozen().unwrap();
        assert!(Arc::ptr_eq(&frozen, &pin.0));

        let (tx, rx) = mpsc::channel();
        let flusher = {
            let frozen = Arc::clone(&frozen);
            std::thread::spawn(move || {
                frozen.wait_for_writers();
                let mut keys = Vec::new();
                frozen.for_each_entry(|k, _, _| keys.push(k.to_vec()));
                tx.send(keys).unwrap();
            })
        };

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "flush started while a writer was still pinned"
        );
        pin.0.put(999_999, b"late", b"write").unwrap();
        drop(pin);

        let keys = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            keys.iter().any(|k| k == b"late"),
            "flush missed the late write"
        );
        flusher.join().unwrap();
    }

    /// #2: a lower seqno landing in a newer memtable must not shadow a higher one.
    #[test]
    fn out_of_order_seq_across_rotation_reads_newest() {
        let (set, _) = set_with(16 * 1024, 64);
        put(&set, 11, b"k", b"newer");
        let mut seq = 100;
        fill_until_rotation(&set, &mut seq);
        put(&set, 10, b"k", b"older");

        assert_eq!(
            value_of(&set, MAX_SEQNO, b"k"),
            Some(Some(b"newer".to_vec()))
        );
        assert_eq!(value_of(&set, 10, b"k"), Some(Some(b"older".to_vec())));
    }

    /// #4: an entry that can never fit is rejected instead of spinning.
    #[test]
    fn oversize_entry_is_rejected() {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let (set, _) = set_with(16 * 1024, 4);
            let r = set.put(
                1,
                b"k",
                &vec![0u8; 32 * 1024],
                || Ok::<_, MemtableError>(()),
            );
            tx.send(matches!(r, Err(MemtableError::EntryTooLarge)))
                .unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_secs(3)).unwrap());
    }

    #[test]
    fn entry_at_the_limit_is_accepted() {
        let (set, _) = set_with(16 * 1024, 4);
        let value = vec![0u8; set.max_entry_size() - entry_footprint(1, 0)];
        put(&set, 1, b"k", &value);
        assert_eq!(value_of(&set, 1, b"k"), Some(Some(value)));
    }

    /// #3 and #5 under contention: nothing is lost, rotations queue up in order,
    /// and every entry is backed by a WAL record in a segment >= its memtable's
    /// tag, which is what makes "delete segments <= tag after flush" safe.
    #[test]
    fn concurrent_writers_keep_the_wal_invariant() {
        const THREADS: usize = 4;
        const PER: u64 = 2_000;

        let (set, wal) = set_with(64 * 1024, 1024);
        let set = Arc::new(set);

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let (set, wal) = (Arc::clone(&set), Arc::clone(&wal));
                std::thread::spawn(move || {
                    for i in 0..PER {
                        let key = format!("t{t}-k{i:06}");
                        let seq = (t as u64) * PER + i + 1;
                        let val = format!("v{t}-{i}");
                        set.put(seq, key.as_bytes(), val.as_bytes(), || {
                            wal.append(key.as_bytes(), seq)
                        })
                        .unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert!(set.frozen_count() > 0, "test never exercised rotation");

        for t in 0..THREADS {
            for i in 0..PER {
                let key = format!("t{t}-k{i:06}");
                assert_eq!(
                    value_of(&set, MAX_SEQNO, key.as_bytes()),
                    Some(Some(format!("v{t}-{i}").into_bytes())),
                    "lost {key}"
                );
            }
        }

        let records = wal.records.lock().unwrap();
        let mut newest_segment: BTreeMap<(Vec<u8>, Seqno), u64> = BTreeMap::new();
        for (k, s, seg) in records.iter() {
            let e = newest_segment.entry((k.clone(), *s)).or_insert(0);
            *e = (*e).max(*seg);
        }

        let inner = set.inner.read().unwrap();
        let tags: Vec<u64> = inner.frozen.iter().map(|m| m.wal_segment()).collect();
        assert!(
            tags.windows(2).all(|w| w[0] < w[1]),
            "frozen queue out of order"
        );

        for mt in inner.frozen.iter().chain(std::iter::once(&inner.active)) {
            mt.for_each_entry(|k, seq, _| {
                let seg = newest_segment[&(k.to_vec(), seq)];
                assert!(
                    seg >= mt.wal_segment(),
                    "record in segment {seg} but data in memtable tagged {}",
                    mt.wal_segment()
                );
            });
        }
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;
    use std::collections::BTreeMap;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::AtomicBool;
    use std::sync::{Mutex, mpsc};
    use std::time::Duration;

    // ------------------------------------------------------------ helpers

    /// WAL whose roll can be made to fail, and which records appends per segment.
    struct FlakyWal {
        current: AtomicU64,
        fail_roll: AtomicBool,
        records: Mutex<Vec<(Vec<u8>, Seqno, u64)>>,
    }

    impl FlakyWal {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                current: AtomicU64::new(1),
                fail_roll: AtomicBool::new(false),
                records: Mutex::new(Vec::new()),
            })
        }
        fn append(&self, key: &[u8], seq: Seqno) -> Result<(), MemtableError> {
            let mut recs = self.records.lock().unwrap();
            recs.push((key.to_vec(), seq, self.current.load(Ordering::SeqCst)));
            Ok(())
        }
        fn logged(&self, key: &[u8], seq: Seqno) -> bool {
            self.records
                .lock()
                .unwrap()
                .iter()
                .any(|(k, s, _)| k == key && *s == seq)
        }
    }

    impl WalRoller for FlakyWal {
        fn roll(&self) -> io::Result<u64> {
            if self.fail_roll.load(Ordering::SeqCst) {
                return Err(io::Error::other("disk full"));
            }
            let _g = self.records.lock().unwrap();
            Ok(self.current.fetch_add(1, Ordering::SeqCst) + 1)
        }
    }

    fn set_with(cap: u32, max_frozen: usize) -> (MemtableSet, Arc<FlakyWal>) {
        let wal = FlakyWal::new();
        (
            MemtableSet::new(cap, 1, max_frozen, wal.clone()).unwrap(),
            wal,
        )
    }

    fn put(set: &MemtableSet, seq: Seqno, key: &[u8], value: &[u8]) -> bool {
        set.put(seq, key, value, || Ok::<_, MemtableError>(()))
            .unwrap()
    }

    fn del(set: &MemtableSet, seq: Seqno, key: &[u8]) {
        set.delete(seq, key, || Ok::<_, MemtableError>(())).unwrap();
    }

    fn read(set: &MemtableSet, snapshot: Seqno, key: &[u8]) -> Option<Option<Vec<u8>>> {
        set.get(snapshot, key, |l| match l {
            Lookup::Value(v) => Some(v.to_vec()),
            Lookup::Tombstone => None,
        })
    }

    fn rotate_now(set: &MemtableSet, filler_seq: &mut Seqno) {
        let before = set.frozen_count();
        while set.frozen_count() == before {
            put(
                set,
                *filler_seq,
                format!("~fill{filler_seq:010}").as_bytes(),
                &[0u8; 200],
            );
            *filler_seq += 1;
        }
    }

    /// Runs `f` on a thread and fails the test if it does not finish in time.
    fn within<T: Send + 'static>(secs: u64, f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(Duration::from_secs(secs))
            .expect("operation hung")
    }

    fn rng(seed: u64) -> impl FnMut() -> u64 {
        let mut x = seed;
        move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        }
    }

    // ------------------------------------------------------------ keys and values

    #[test]
    fn empty_key_and_empty_value_are_ordinary_data() {
        let (set, _) = set_with(1 << 16, 4);
        put(&set, 0, b"", b"");
        put(&set, 1, b"x", b"");

        assert_eq!(read(&set, 0, b""), Some(Some(vec![])), "empty key at seq 0");
        assert_eq!(
            read(&set, MAX_SEQNO, b"x"),
            Some(Some(vec![])),
            "empty value is not a tombstone"
        );
    }

    #[test]
    fn empty_key_tombstone() {
        let (set, _) = set_with(1 << 16, 4);
        put(&set, 1, b"", b"v");
        del(&set, 2, b"");
        assert_eq!(read(&set, MAX_SEQNO, b""), Some(None));
        assert_eq!(read(&set, 1, b""), Some(Some(b"v".to_vec())));
    }

    #[test]
    fn prefix_and_binary_keys_do_not_alias() {
        let (set, _) = set_with(1 << 16, 4);
        let keys: [&[u8]; 6] = [b"a", b"a\0", b"ab", b"\xff", b"\x00", b"\xff\xff"];
        for (i, k) in keys.iter().enumerate() {
            put(&set, i as u64 + 1, k, &[i as u8]);
        }
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(read(&set, MAX_SEQNO, k), Some(Some(vec![i as u8])), "{k:?}");
        }
        assert_eq!(read(&set, MAX_SEQNO, b"a\0\0"), None);
        assert_eq!(read(&set, MAX_SEQNO, b""), None);
    }

    // ------------------------------------------------------------ snapshots

    #[test]
    fn snapshot_below_every_version_misses() {
        let (set, _) = set_with(1 << 16, 4);
        put(&set, 5, b"k", b"v5");
        assert_eq!(read(&set, 4, b"k"), None);
        assert_eq!(read(&set, 0, b"k"), None);
        assert_eq!(read(&set, 5, b"k"), Some(Some(b"v5".to_vec())));
    }

    #[test]
    fn seq_zero_is_a_real_version() {
        // max_seqno starts at 0, so the skip check must not treat "0" as "empty".
        let (set, _) = set_with(16 * 1024, 64);
        put(&set, 0, b"k", b"zero");
        let mut f = 1_000;
        rotate_now(&set, &mut f);
        assert_eq!(read(&set, 0, b"k"), Some(Some(b"zero".to_vec())));
        assert_eq!(read(&set, MAX_SEQNO, b"k"), Some(Some(b"zero".to_vec())));
    }

    /// Needs rskipl without the 56-bit debug_assert (0.1.4).
    #[test]
    #[cfg_attr(debug_assertions, ignore = "needs rskipl 0.1.4 (56-bit debug_assert)")]
    fn seq_u64_max_round_trips() {
        let (set, _) = set_with(1 << 16, 4);
        put(&set, u64::MAX - 1, b"k", b"below");
        put(&set, u64::MAX, b"k", b"top");
        assert_eq!(read(&set, MAX_SEQNO, b"k"), Some(Some(b"top".to_vec())));
        assert_eq!(
            read(&set, u64::MAX - 1, b"k"),
            Some(Some(b"below".to_vec()))
        );
    }

    #[test]
    fn snapshot_between_versions_spread_over_memtables() {
        // M1: k@5, k@20   M2 (newer): k@10
        let (set, _) = set_with(16 * 1024, 64);
        put(&set, 5, b"k", b"v5");
        put(&set, 20, b"k", b"v20");
        let mut f = 1_000;
        rotate_now(&set, &mut f);
        put(&set, 10, b"k", b"v10");

        assert_eq!(read(&set, 4, b"k"), None);
        assert_eq!(read(&set, 7, b"k"), Some(Some(b"v5".to_vec())));
        assert_eq!(read(&set, 15, b"k"), Some(Some(b"v10".to_vec())));
        assert_eq!(read(&set, 25, b"k"), Some(Some(b"v20".to_vec())));
    }

    #[test]
    fn newer_memtable_lower_seq_tombstone_does_not_hide_older_higher_value() {
        let (set, _) = set_with(16 * 1024, 64);
        put(&set, 11, b"k", b"live");
        let mut f = 1_000;
        rotate_now(&set, &mut f);
        del(&set, 10, b"k");
        assert_eq!(read(&set, MAX_SEQNO, b"k"), Some(Some(b"live".to_vec())));
        assert_eq!(read(&set, 10, b"k"), Some(None));
    }

    #[test]
    fn older_memtable_higher_seq_tombstone_wins() {
        let (set, _) = set_with(16 * 1024, 64);
        del(&set, 11, b"k");
        let mut f = 1_000;
        rotate_now(&set, &mut f);
        put(&set, 10, b"k", b"stale");
        assert_eq!(read(&set, MAX_SEQNO, b"k"), Some(None));
    }

    #[test]
    fn same_key_and_seq_in_two_memtables_newest_memtable_wins() {
        // What a retried write (logged twice) looks like after a Full.
        let (set, _) = set_with(16 * 1024, 64);
        put(&set, 7, b"k", b"first");
        let mut f = 1_000;
        rotate_now(&set, &mut f);
        put(&set, 7, b"k", b"second");
        assert_eq!(read(&set, MAX_SEQNO, b"k"), Some(Some(b"second".to_vec())));
    }

    #[test]
    fn same_key_and_seq_upserts_inside_one_memtable() {
        let (set, _) = set_with(1 << 16, 4);
        put(&set, 7, b"k", b"a");
        put(&set, 7, b"k", b"b");
        del(&set, 7, b"k");
        put(&set, 7, b"k", b"c");
        let inner = set.inner.read().unwrap();
        assert_eq!(inner.active.len(), 1, "upsert must not add a node");
        drop(inner);
        assert_eq!(read(&set, 7, b"k"), Some(Some(b"c".to_vec())));
    }

    #[test]
    fn get_callback_runs_only_on_hit() {
        let (set, _) = set_with(1 << 16, 4);
        let mut calls = 0;
        assert!(set.get(MAX_SEQNO, b"nope", |_| calls += 1).is_none());
        assert_eq!(calls, 0);
    }

    // ------------------------------------------------------------ sizing

    #[test]
    fn one_byte_over_the_limit_is_rejected() {
        let (set, _) = set_with(16 * 1024, 4);
        let ok = set.max_entry_size() - entry_footprint(1, 0);
        assert!(
            set.put(1, b"k", &vec![0; ok + 1], || Ok::<_, MemtableError>(()))
                .is_err()
        );
        assert!(
            set.put(1, b"k", &vec![0; ok], || Ok::<_, MemtableError>(()))
                .is_ok()
        );
    }

    #[test]
    fn huge_key_delete_is_rejected_without_logging() {
        let (set, _) = set_with(16 * 1024, 4);
        let mut logged = false;
        let r = set.delete(1, &vec![0; 64 * 1024], || {
            logged = true;
            Ok::<_, MemtableError>(())
        });
        assert!(matches!(r, Err(MemtableError::EntryTooLarge)));
        assert!(!logged, "a rejected write must never reach the WAL");
    }

    #[test]
    fn a_stream_of_max_size_entries_rotates_every_time_and_never_hangs() {
        within(10, || {
            let (set, _) = set_with(16 * 1024, 1024);
            let v = vec![7u8; set.max_entry_size() - entry_footprint(8, 0)];
            for i in 0..200u64 {
                put(&set, i + 1, format!("{i:08}").as_bytes(), &v);
            }
            for i in 0..200u64 {
                assert!(read(&set, MAX_SEQNO, format!("{i:08}").as_bytes()).is_some());
            }
        });
    }

    #[test]
    fn tiny_capacity_is_an_error_not_a_panic_or_a_dead_set() {
        // Arena::new asserts cap > 8, the head node needs 112 bytes, and a
        // set whose max_entry_size is below the smallest footprint accepts nothing.
        for cap in [0u32, 8, 64, 200] {
            let r = catch_unwind(|| {
                let wal = FlakyWal::new();
                MemtableSet::new(cap, 1, 4, wal)
                    .map(|s| s.put(1, b"", b"", || Ok::<_, MemtableError>(())).is_ok())
            });
            match r {
                Ok(Err(_)) => {}
                Ok(Ok(true)) => {}
                Ok(Ok(false)) => panic!("cap {cap}: set constructed but rejects every write"),
                Err(_) => panic!("cap {cap}: constructor panicked"),
            }
        }
    }

    // ------------------------------------------------------------ rotation and the Full path

    /// Arena bytes are consumed by a failed insert (key and value are allocated
    /// before the node), but len() stays 0. rotate() then refuses because the
    /// memtable "is empty", and the writer spins on Full forever.
    ///
    /// Here the waste is produced directly; in production it takes concurrent
    /// writers racing on a fresh memtable where none of them gets its node in.
    #[test]
    fn empty_but_exhausted_memtable_still_rotates() {
        within(5, || {
            let (set, _) = set_with(16 * 1024, 4);
            {
                let pin = set.pin_active();
                let left = pin.0.list.remaining();
                // value fits, the node after it does not
                let r = pin.0.put(1, b"w", &vec![0; left - 1 - 8]);
                assert!(matches!(r, Err(MemtableError::Full)));
                assert!(pin.0.is_empty());
            }
            put(&set, 2, b"k", b"v");
            assert_eq!(read(&set, MAX_SEQNO, b"k"), Some(Some(b"v".to_vec())));
        });
    }

    #[test]
    fn writer_pinned_to_a_frozen_memtable_that_is_full_moves_on() {
        let (set, _) = set_with(16 * 1024, 64);
        let stale = set.pin_active();
        let mut f = 1_000;
        rotate_now(&set, &mut f);
        // stale writer's memtable is frozen; rotate(stale) must be a no-op
        assert!(!set.rotate(&stale.0).unwrap());
        drop(stale);
        put(&set, 5, b"k", b"v");
        assert_eq!(set.inner.read().unwrap().active.len(), 1);
    }

    #[test]
    fn rotate_twice_on_same_memtable_creates_one_frozen() {
        let (set, _) = set_with(16 * 1024, 64);
        let active = set.inner.read().unwrap().active.clone();
        assert!(
            !set.rotate(&active).unwrap(),
            "below the watermark: no rotation"
        );
        let mut i = 0u64;
        while !active.should_freeze() {
            active
                .put(i, format!("{i:08}").as_bytes(), &[0; 100])
                .unwrap();
            i += 1;
        }
        assert!(set.rotate(&active).unwrap());
        assert!(!set.rotate(&active).unwrap());
        assert_eq!(set.frozen_count(), 1);
    }

    #[test]
    fn rotation_tags_are_strictly_increasing_wal_segments() {
        let (set, wal) = set_with(8 * 1024, 64);
        let mut f = 1;
        for _ in 0..5 {
            rotate_now(&set, &mut f);
        }
        let inner = set.inner.read().unwrap();
        let tags: Vec<u64> = inner
            .frozen
            .iter()
            .chain(std::iter::once(&inner.active))
            .map(|m| m.wal_segment())
            .collect();
        assert_eq!(tags, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(wal.current.load(Ordering::SeqCst), 6);
    }

    // ------------------------------------------------------------ WAL failures

    /// A write that landed in the memtable (and the WAL) must not be reported as
    /// failed just because the follow-up rotation could not roll the WAL.
    #[test]
    fn roll_failure_after_a_successful_insert_does_not_fail_the_write() {
        let (set, wal) = set_with(16 * 1024, 64);
        wal.fail_roll.store(true, Ordering::SeqCst);

        let mut seq = 1;
        loop {
            let key = format!("k{seq:06}");
            let r = set.put(seq, key.as_bytes(), &[0; 200], || {
                wal.append(key.as_bytes(), seq)
            });
            let visible = read(&set, MAX_SEQNO, key.as_bytes()).is_some();
            if r.is_err() {
                assert!(
                    !visible,
                    "seq {seq}: put returned Err but the write is visible (and logged)"
                );
                break;
            }
            seq += 1;
        }
    }

    /// If put returns Err, the record must not be replayable from the WAL,
    /// otherwise a write the client saw fail comes back after a crash.
    #[test]
    fn failed_write_leaves_no_replayable_record() {
        let (set, wal) = set_with(16 * 1024, 64);
        // Fill below the watermark so the next write takes the Full path.
        let mut seq = 1;
        while !set.inner.read().unwrap().active.should_freeze() {
            put(&set, seq, format!("f{seq:06}").as_bytes(), &[0; 100]);
            seq += 1;
            if set.frozen_count() > 0 {
                break;
            }
        }
        wal.fail_roll.store(true, Ordering::SeqCst);
        let mut failed = None;
        for s in seq..seq + 200 {
            let key = format!("g{s:06}");
            if set
                .put(s, key.as_bytes(), &[0; 200], || {
                    wal.append(key.as_bytes(), s)
                })
                .is_err()
            {
                failed = Some((key, s));
                break;
            }
        }
        let (key, s) = failed.expect("roll never failed");
        assert!(read(&set, MAX_SEQNO, key.as_bytes()).is_none());
        assert!(
            !wal.logged(key.as_bytes(), s),
            "write {key}@{s} was reported failed but sits in the WAL"
        );
    }

    #[test]
    fn log_error_is_returned_and_nothing_is_inserted() {
        let (set, _) = set_with(1 << 16, 4);
        let r = set.put(1, b"k", b"v", || {
            Err(MemtableError::Wal {
                err: Arc::new(io::Error::other("x")),
                logged: false,
            })
        });
        assert!(r.is_err());
        assert_eq!(read(&set, MAX_SEQNO, b"k"), None);
        assert_eq!(
            set.inner
                .read()
                .unwrap()
                .active
                .writers
                .load(Ordering::SeqCst),
            0
        );
    }

    #[test]
    fn panicking_log_closure_does_not_leak_a_pin() {
        let (set, _) = set_with(1 << 16, 4);
        let r = catch_unwind(AssertUnwindSafe(|| {
            let _ = set.put(1, b"k", b"v", || -> Result<(), MemtableError> {
                panic!("boom")
            });
        }));
        assert!(r.is_err());
        let active = set.inner.read().unwrap().active.clone();
        assert_eq!(active.writers.load(Ordering::SeqCst), 0);
        within(2, move || active.wait_for_writers());
    }

    // ------------------------------------------------------------ retire

    #[test]
    #[should_panic(expected = "no frozen memtable")]
    fn retire_with_nothing_frozen_panics() {
        let (set, _) = set_with(1 << 16, 4);
        let active = set.inner.read().unwrap().active.clone();
        set.retire_oldest(&active);
    }

    #[test]
    fn retire_of_the_active_memtable_does_not_drop_the_oldest_frozen() {
        let (set, _) = set_with(8 * 1024, 64);
        let mut f = 1;
        rotate_now(&set, &mut f);
        let active = set.inner.read().unwrap().active.clone();
        let _ = catch_unwind(AssertUnwindSafe(|| set.retire_oldest(&active)));

        // A bad retire call must not lose data or poison the set.
        let alive = catch_unwind(AssertUnwindSafe(|| set.frozen_count()));
        assert_eq!(alive.ok(), Some(1), "frozen memtable lost or lock poisoned");
    }

    #[test]
    fn retire_does_not_disturb_in_flight_readers() {
        let (set, _) = set_with(8 * 1024, 64);
        put(&set, 1, b"k", b"v");
        let mut f = 100;
        rotate_now(&set, &mut f);
        let frozen = set.oldest_frozen().unwrap();
        set.retire_oldest(&frozen);
        // the Arc keeps the arena alive; borrowed slices stay valid
        assert_eq!(frozen.get(MAX_SEQNO, b"k").map(|(s, _)| s), Some(1));
        assert_eq!(
            read(&set, MAX_SEQNO, b"k"),
            None,
            "retired data leaves the set"
        );
    }

    // ------------------------------------------------------------ backpressure

    #[test]
    fn backpressure_clears_after_retire() {
        let (set, _) = set_with(8 * 1024, 2);
        let mut f = 1;
        rotate_now(&set, &mut f);
        rotate_now(&set, &mut f);
        assert!(set.needs_backpressure());
        let oldest = set.oldest_frozen().unwrap();
        set.retire_oldest(&oldest);
        assert!(!set.needs_backpressure());
    }

    #[test]
    fn max_frozen_zero_means_always_backpressured() {
        let (set, _) = set_with(8 * 1024, 0);
        assert!(
            set.needs_backpressure(),
            "document or reject max_frozen == 0"
        );
    }

    // ------------------------------------------------------------ model and stress

    /// Random puts/deletes with shuffled seqnos and many rotations, checked
    /// against a model at random snapshots. Ties on (key, seq) resolve to the
    /// last write, which is what "newest memtable wins" gives.
    #[test]
    fn set_matches_model_across_rotations_with_out_of_order_seqs() {
        let (set, _) = set_with(8 * 1024, 1 << 20);
        let mut model: BTreeMap<Vec<u8>, BTreeMap<Seqno, Option<Vec<u8>>>> = BTreeMap::new();
        let mut r = rng(0x5EED_1234_ABCD_0001);

        for i in 0..20_000u64 {
            let key = format!("k{:03}", r() % 64).into_bytes();
            // seqs jitter around i so neighbours land out of order across rotations
            let seq = i.saturating_sub(r() % 16);
            if r().is_multiple_of(5) {
                del(&set, seq, &key);
                model.entry(key).or_default().insert(seq, None);
            } else {
                let v = format!("v{i}").into_bytes();
                put(&set, seq, &key, &v);
                model.entry(key).or_default().insert(seq, Some(v));
            }
        }
        assert!(
            set.frozen_count() > 20,
            "model test should cross many rotations"
        );

        for _ in 0..20_000 {
            let key = format!("k{:03}", r() % 70).into_bytes();
            let snap = match r() % 4 {
                0 => MAX_SEQNO,
                _ => r() % 20_100,
            };
            let want = model
                .get(&key)
                .and_then(|vs| vs.range(..=snap).next_back())
                .map(|(_, v)| v.clone());
            assert_eq!(
                read(&set, snap, &key),
                want,
                "key {key:?} at snapshot {snap}"
            );
        }
    }

    /// Writers, readers and a flusher that "installs" into an SST model before
    /// retiring. A reader must never see an older value than the last completed
    /// write for that key. Also checks the read order: memtables first, then SSTs.
    #[test]
    fn readers_never_go_back_in_time_while_flushing() {
        const WRITERS: usize = 4;
        const PER: u64 = 3_000;

        type Sst = BTreeMap<(Vec<u8>, std::cmp::Reverse<Seqno>), Option<Vec<u8>>>;
        let (set, _) = set_with(16 * 1024, 1 << 20);
        let set = Arc::new(set);
        let sst: Arc<Mutex<Sst>> = Arc::default();
        let done = Arc::new(AtomicBool::new(false));
        let completed: Arc<Vec<AtomicU64>> =
            Arc::new((0..WRITERS).map(|_| AtomicU64::new(0)).collect());

        let lookup = {
            let (set, sst) = (Arc::clone(&set), Arc::clone(&sst));
            move |key: &[u8]| -> Option<Seqno> {
                let mem = set.get(MAX_SEQNO, key, |l| match l {
                    Lookup::Value(v) => u64::from_le_bytes(v[..8].try_into().unwrap()),
                    Lookup::Tombstone => 0,
                });
                let disk = sst
                    .lock()
                    .unwrap()
                    .range((key.to_vec(), std::cmp::Reverse(MAX_SEQNO))..)
                    .next()
                    .filter(|((k, _), _)| k == key)
                    .map(|((_, std::cmp::Reverse(s)), _)| *s);
                mem.max(disk)
            }
        };

        let flusher = {
            let (set, sst, done) = (Arc::clone(&set), Arc::clone(&sst), Arc::clone(&done));
            std::thread::spawn(move || {
                loop {
                    match set.oldest_frozen() {
                        Some(mt) => {
                            mt.wait_for_writers();
                            let mut out = Vec::new();
                            mt.for_each_entry(|k, s, v| {
                                out.push((k.to_vec(), s, v.map(|v| v.to_vec())))
                            });
                            let mut g = sst.lock().unwrap();
                            for (k, s, v) in out {
                                g.insert((k, std::cmp::Reverse(s)), v);
                            }
                            drop(g);
                            set.retire_oldest(&mt);
                        }
                        None if done.load(Ordering::SeqCst) => break,
                        None => std::thread::yield_now(),
                    }
                }
            })
        };

        let readers: Vec<_> = (0..2)
            .map(|_| {
                let (done, completed, lookup) =
                    (Arc::clone(&done), Arc::clone(&completed), lookup.clone());
                std::thread::spawn(move || {
                    let mut r = rng(0xFEED);
                    while !done.load(Ordering::SeqCst) {
                        let t = (r() % WRITERS as u64) as usize;
                        let floor = completed[t].load(Ordering::Acquire);
                        let seen = lookup(format!("w{t}").as_bytes()).unwrap_or(0);
                        assert!(
                            seen >= floor,
                            "writer {t}: saw seq {seen} after {floor} completed"
                        );
                    }
                })
            })
            .collect();

        let writers: Vec<_> = (0..WRITERS)
            .map(|t| {
                let (set, completed) = (Arc::clone(&set), Arc::clone(&completed));
                std::thread::spawn(move || {
                    for i in 1..=PER {
                        let seq = i * WRITERS as u64 + t as u64;
                        let mut v = seq.to_le_bytes().to_vec();
                        v.extend_from_slice(&[0; 120]);
                        put(&set, seq, format!("w{t}").as_bytes(), &v);
                        completed[t].store(seq, Ordering::Release);
                    }
                })
            })
            .collect();

        for w in writers {
            w.join().unwrap();
        }
        done.store(true, Ordering::SeqCst);
        for r in readers {
            r.join().unwrap();
        }
        flusher.join().unwrap();
        for t in 0..WRITERS {
            assert_eq!(
                lookup(format!("w{t}").as_bytes()),
                Some(PER * WRITERS as u64 + t as u64)
            );
        }
    }

    #[test]
    fn hot_key_contention_across_rotations() {
        let (set, _) = set_with(16 * 1024, 1 << 20);
        let set = Arc::new(set);
        let hs: Vec<_> = (0..8u64)
            .map(|t| {
                let set = Arc::clone(&set);
                std::thread::spawn(move || {
                    for i in 0..2_000u64 {
                        let seq = i * 8 + t + 1;
                        put(&set, seq, b"hot", &[0; 64]);
                        assert!(
                            set.get(seq, b"hot", |_| ()).is_some(),
                            "own write invisible at its own seq"
                        );
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert!(set.frozen_count() > 0);
        assert_eq!(set.get(MAX_SEQNO, b"hot", |_| ()), Some(()));
    }
}
