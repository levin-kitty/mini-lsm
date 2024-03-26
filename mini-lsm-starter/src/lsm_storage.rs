use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::Key;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{map_bound, MemTable};
use crate::mvcc::LsmMvccInner;
use crate::table::{self, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: &[u8],
    table_end: &[u8],
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin => {
            return false;
        }
        Bound::Included(key) if key < table_begin => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end => {
            return false;
        }
        Bound::Included(key) if key > table_end => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(user_key: &[u8], table_begin: &[u8], table_end: &[u8]) -> bool {
    table_begin <= user_key && user_key <= table_end
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir(path)?;
        }

        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        assert!(!key.is_empty(), "key cannot be empty");

        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table].clone();
            if key_within(key, table.first_key().raw_ref(), table.last_key().raw_ref()) {
                /*
                                iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                                    table,
                                    Key::from_slice(key),
                                )?));
                */
                if let Some(bloom) = &table.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                            table,
                            Key::from_slice(key),
                        )?));
                    } else {
                        iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                            table,
                            Key::from_slice(key),
                        )?));
                    }
                }
            }
        }
        let iter = MergeIterator::create(iters);
        if iter.is_valid() && iter.key() == Key::from_slice(key) && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        assert!(!value.is_empty(), "key cannot be empty");

        let size;
        {
            let guard = self.state.read();
            guard.memtable.put(key, value)?;
            size = guard.memtable.approximate_size();
        }

        self.try_freeze(size)?;

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");

        let size;
        {
            let guard = self.state.read();
            guard.memtable.put(key, b"")?;
            size = guard.memtable.approximate_size();
        }

        self.try_freeze(size)?;

        Ok(())
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            // state_lock 을 잡아아만 freeze 를 할 수 있다
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();

            // 다른 thread가 이미 freeze를 했을 수 있다, 불필요한 freeze를 막기 위해 사이즈를 다시 체크한다
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // add 1 on current next_sst_id, and then return previous next_sst_id
        let memtable_id = self.next_sst_id();
        let memtable = Arc::new(MemTable::create(memtable_id));

        let old_memtable;
        {
            // 이 scope를 탈춣하면 write lock은 해제된다
            let mut guard = self.state.write();

            // state를 바로 수정하지 않고, snapshot을 수정하여 안정적인 update를 노린다
            let mut snapshot = guard.as_ref().clone();
            // memtable에 새로운 memtable을 넣고
            old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
            // 이전 memtable은 imm_memtables의 맨 앞에 넣는다
            snapshot.imm_memtables.insert(0, old_memtable.clone());
            // 변경된 state로 수정한다
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let flush_memtable;
        {
            let guard = self.state.read();
            flush_memtable = guard
                .imm_memtables
                .last()
                .expect("no imm memtables!")
                .clone();
        }

        let mut builder: SsTableBuilder = SsTableBuilder::new(self.options.block_size);
        flush_memtable.flush(&mut builder)?;
        let sst_id = flush_memtable.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let mem = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);
            snapshot.l0_sstables.insert(0, sst_id);
            println!("flushed {}.sst with size={}", sst_id, sst.table_size());
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // push 1) current, 2) old tables
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        //dbg!("== current ==");
        //snapshot.memtable.scan(lower, upper).debug();
        for memtable in snapshot.imm_memtables.iter() {
            //dbg!("== other ==");
            //memtable.debug();
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut table_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().raw_ref(),
                table.last_key().raw_ref(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(table, Key::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter =
                            SsTableIterator::create_and_seek_to_key(table, Key::from_slice(key))?;
                        if iter.is_valid() && iter.key() == Key::from_slice(key) {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };
                table_iters.push(Box::new(iter));
            }
        }

        let table_iter = MergeIterator::create(table_iters);
        let iter = TwoMergeIterator::create(memtable_iter, table_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}
