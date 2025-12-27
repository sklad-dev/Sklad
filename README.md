<p align="center">
  <img src='docs/img/sklad_logo.png?raw=true' width='25%'>
</p>

# Sklad
Sklad is a high-performance key-value store that uses an asynchronous, non-blocking design and lock-free data structures to efficiently handle concurrent workloads.

## 👷‍♀️ Building Sklad
Building Sklad is very straightforward. Install Zig toolchain and run
```
zig build --release=safe
```

## 🧑‍💻 Using Sklad
There is a simple Python client: [sklient](https://github.com/sklad-db/sklient). Currently, the data store implements three operations

### 1. Adding a new key-value pair:
```
set <key> <value>
```

### 2. Retrieving a value by key:
```
get <key>
```

### 3. Deleting a data by key:
```
delete <key>
```

**Note**: The strings have to be surrounded with single quotes, e.g., this will work `get 'test'` while this will fail `get test`. It is worth removing the requirement to surround single-word strings with the quotes in the future.

Sklad is listening on TCP port 7733, awaiting incoming messages formatted as JSON strings with the following structure:
```
{
    "kind": 1,
    "query": "<query string e.g. get 'test'>",
    "timestamp": 1234567890
}
```

## 🔧 Configuration
Currently, Sklad expects `config/configuration.json` file in the same folder as the executable.

Configuration file example:

```
{
    "worker_pool": {
        "min_workers": 1,
        "max_workers": 8,
        "idle_timeout_seconds": 5,
        "task_wait_threshold_us": 800
    },
    "memtable": {
        "max_size": 2621440,
        "max_level": 8
    },
    "sstable": {
        "block_size": 4096,
        "bloom_bits_per_key": 10
    },
    "sstable_cache": {
        "size": 32
    },
    "compaction": {
        "tiered": {
            "max_level": 6,
            "level_multiplier": 2,
            "level_threshold": 4
        }
    },
    "cleanup": {
        "interval_seconds": 60,
        "file_count_threshold": 5
    },
    "max_connections": 128
}
```

### Parameters:
* **worker_pool.min_workers** - (u8) minimum number of worker threads that will be kept alive
* **worker_pool.max_workers** - (u8) maximum number of worker threads
* **worker_pool.idle_timeout_seconds** - (i64) a timeout in seconds after which an idle worker thread is terminated
* **worker_pool.task_wait_threshold_us** - (u64) a p95 (95th percentile) wait-time threshold for tasks in the queue; once exceeded, a new worker thread is spawned
* **memtable.max_size** - (u64) maximum size of a memtable. After reaching the maximum size, the memtable is flushed to a SSTable file on disk
* **max_level** - (u8) the memtable is implemented as a skip-list; this parameter sets the maximum height of a skip-list node's tower
* **sstable.block_size** - (u32) the size of SSTable data block in bytes
* **sstable.bloom_bits_per_key** - (u8) how many bits to use for each stored key
* **sstable_cache.size** - (u8) SSTable cache capacity
* **compaction.tiered.max_level** - (u8) maximum compaction level
* **compaction.tiered.level_multiplier** - (u8) number of files compacted per compaction run
* **compaction.tiered.level_threshold** - (u8) file-count threshold at a level that triggers compaction
* **cleanup.interval_seconds** - (i64) minimum time between cleanup runs (seconds)
* **cleanup.file_count_threshold** - (u16) minimum number of deleted files required to run cleanup
* **max_connections** - (u16) maximum concurrent client connections

## 🏗️ Architecture
Sklad is built around an asynchronous task queue with a small pool of worker threads. This design allows the system to efficiently handle a large number of concurrent requests without overloading resources.

The project emphasizes lock-free data structures and algorithms wherever possible, reducing contention and improving concurrency — a core principle throughout the system.

For storage, Sklad uses an LSM-tree (Log-Structured Merge Tree) to optimize write performance and support high-throughput workloads.

### More:
1) [SSTable file structure](docs/sstable.md)
2) [MANIFEST file](docs/manifest.md)

## Todo
* Port to Linux, use io_uring for I/O
* Add metrics: SSTable count per level and pending memtable count
* End SSTables with pre-defined postifx to make sure the creation was completed
* Add data integrity checks: CRC for SSTable files and per-record xxh3 for WAL
* TTL
* Implement more advanced compaction strategies
  - [bLSM](https://dl.acm.org/doi/10.1145/2213836.2213862)
  - [Monkey](https://dl.acm.org/doi/10.1145/3035918.3064054)
  - [Dostoevsky](https://dl.acm.org/doi/10.1145/3183713.3196927)
* Implement key-value separation
  - [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)
