<p align="center">
  <img src='docs/img/sklad_logo.png?raw=true' width='25%'>
</p>

# Sklad
Sklad is an experimental project aiming to create a new kind of HTAP (Hybrid Transactional/Analytical Processing) data store. In the first stage, Sklad focuses on a simple, high-performance key-value store.

## 👷‍♀️ Building Sklad
Building Sklad is very straightforward. Install Zig toolchain and run
```
zig build --release=safe
```

## 🧑‍💻 Using Sklad
There is a simple Python client: [sklient](https://github.com/sklad-db/sklient). Currently, the data store implements two operations

### 1. Adding a new key-value pair:
```
set <key> <value>
```

### 2. Retrieving a value by key:
```
get <key>
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
    "memtable": {
        "max_size": 2097152,
        "max_level": 8
    },
    "sstable": {
        "block_size": 4096,
        "bloom_bits_per_key": 20
    }
}
```

### Parameters:
* **memtable.max_size** - (u64) maximum size of a memtable. After reaching the maximum size, the memtable is flushed to a SSTable file on disk
* **max_level** - (u8) the memtable is implemented as a skip-list; this parameter sets the maximum height of a skip-list node's tower
* **sstable.block_size** - (u32) the size of SSTable data block
* **sstable.bloom_bits_per_key** - (u8) how many bits to use for each stored key

## 🏗️ Architecture
Sklad is built around an asynchronous task queue with a small pool of worker threads. This design allows the system to efficiently handle a large number of concurrent requests without overloading resources.

The project emphasizes lock-free data structures and algorithms wherever possible, reducing contention and improving concurrency—a core principle throughout the system.

For storage, Sklad uses an LSM-tree (Log-Structured Merge Tree) to optimize write performance and support high-throughput workloads.

### More:
1) [SSTable file structure](docs/sstable.md)

## Todo
### Stage 1:
* LSM-tree compaction
* Handle inputs of arbitrary length
### Stage 2:
* Value separation
* Prefix compression
