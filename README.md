# Sklad.

**Sklad** is a lightweight key-value database that uses an asynchronous, non-blocking design and lock-free data structures to efficiently handle concurrent workloads.

**⚠️ Status: v0.1.0 Prototype**
This initial 0.1.0 release is a functioning prototype. It is designed to get the ball rolling and demonstrate the core architecture. Please note that there are still known limitations, missing features, and likely a few bugs. It is not yet recommended for production use.

## 👷‍♀️ Building Sklad
Building Sklad is very straightforward. Install Zig toolchain and run
```
zig build --release=safe
```

## 🧑‍💻 Using Sklad
There is a terminal client: [sklient](https://github.com/sklad-dev/sklient). You can follow the instructions to build it, you'll need Zig 0.15.2.

Currently, the storage implements three operations:

### 1. Adding a new key-value pair:
```
set <key> <value>
```

### 1a. Adding a key-value pair with TTL:

```
set <key> <value> expire '<time>'
```

Where `<time>` can be:
- A number in milliseconds: `'1000'`
- Seconds with 's' suffix: `'10s'`
- Milliseconds with 'ms' suffix: `'500ms'`

Example: `set 'mykey' 'myvalue' expire '30s'`

### 2. Retrieving a value by key:
```
get <key>
```

### 2a. Retrieving a range of key-value pairs:
```
get range <start_key> <end_key>
```

Where `<start_key>` and `<end_key>` define the range boundaries.

**Note**: Both keys must be of the same data type.


### 3. Deleting data by key:
```
delete <key>
```

**Note**: Strings currently have to be surrounded with single quotes (e.g., `get 'test'`). Unquoted single-word strings like `get test` will fail to be parsed.

Sklad listens on TCP port 7733, awaiting incoming messages formatted as JSON strings with the following structure:
```
{
    "kind": 1,
    "query": "<query string e.g. get 'test'>",
    "timestamp": 1234567890
}
```

More on the Sklad communication protocol:
* [Sklad protocol](docs/protocol.md)

## 🔧 Configuration
Sklad currently requires the configuration file to be located at `./config/configuration.json` **relative to the Sklad binary**. If you move the binary, you must ensure the `config/` folder is placed right next to it.

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
* `data_folder` - (string, optional) parent directory for the `.sklad` data folder containing database files (WAL, SSTables, manifest). Defaults to the binary's directory if not specified
* `worker_pool.min_workers` - (u8) minimum number of worker threads that will be kept alive
* `worker_pool.max_workers` - (u8) maximum number of worker threads
* `worker_pool.idle_timeout_seconds` - (i64) a timeout in seconds after which an idle worker thread is terminated
* `worker_pool.task_wait_threshold_us` - (u64) a p95 (95th percentile) wait-time threshold for tasks in the queue; once exceeded, a new worker thread is spawned
* `memtable.max_size` - (u64) maximum size of a memtable. After reaching the maximum size, the memtable is flushed to an SSTable file on disk
* `memtable.max_level` - (u8) the memtable is implemented as a skip-list; this parameter sets the maximum height of a skip-list node's tower
* `sstable.block_size` - (u32) the size of an SSTable data block in bytes
* `sstable.bloom_bits_per_key` - (u8) how many bits to use for each stored key
* `sstable_cache.size` - (u8) SSTable cache capacity
* `compaction.tiered.max_level` - (u8) maximum compaction level
* `compaction.tiered.level_multiplier` - (u8) number of files compacted per compaction run
* `compaction.tiered.level_threshold` - (u8) file-count threshold at a level that triggers compaction
* `cleanup.interval_seconds` - (i64) minimum time between cleanup runs (seconds)
* `cleanup.file_count_threshold` - (u16) minimum number of deleted files required to run cleanup
* `max_connections` - (u16) maximum concurrent client connections
* `batch_response_limit` - (u64) maximum number of bytes returned by `get range` query

## 🏗️ Architecture
Sklad is built around an asynchronous task queue with a small pool of worker threads. This design allows the system to efficiently handle a large number of concurrent requests without overloading resources.

The project emphasizes lock-free data structures and algorithms wherever possible, reducing contention and improving concurrency — a core principle throughout the system.

For storage, Sklad uses an LSM-tree (Log-Structured Merge Tree) to optimize write performance and support high-throughput workloads.

### More:
1) [SSTable file structure](docs/sstable.md)
2) [SSTable file management](docs/sstable_file_management.md)
3) [MANIFEST file](docs/manifest.md)

## Todo
* Add io_uring option for Linux I/O
* Add metrics: SSTable count per level
* End SSTables with a predefined postfix to ensure creation completed
* Add data integrity checks: CRC for SSTable files and per-record xxh3 for WAL
* Implement more advanced compaction strategies
  - [bLSM](https://dl.acm.org/doi/10.1145/2213836.2213862)
  - [Monkey](https://dl.acm.org/doi/10.1145/3035918.3064054)
  - [Dostoevsky](https://dl.acm.org/doi/10.1145/3183713.3196927)
* Implement key-value separation
  - [WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)
