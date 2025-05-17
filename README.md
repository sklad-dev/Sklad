<p align="center">
  <img src='docs/img/sklad_logo.png?raw=true' width='25%'>
</p>

# Sklad
Sklad is an experimental project to build a novel wide-column data store.

## 👷‍♀️ Building Sklad
Building Sklad is very straightforward. Install Zig toolchain and run
```
zig build --release=safe
```

## 🧑‍💻 Using Sklad
There is a simple Python client: [sklient](https://github.com/sklad-db/sklient). Currently, the data store implements two operations

### 1. Adding a new key-value pair:
```
set key value
```

### 2. Retrieving a value by key:
```
get key
```

**Note**: The strings have to be surrounded by double quotes, e.g., this will work `get "test"` while this will fail `get test`. It is worth removing the requirement to surround single-word strings with double quotes in the future.

Sklad is listening on TCP port 7733, awaiting incoming messages formatted as JSON strings with the following structure:
```
{
    "command": "<query string e.g. get "test">"
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
        "sparse_index_step": 256,
        "bloom_bits_per_key": 20
    }
}
```

### Parameters:
* **memtable.max_size** - maximum size of a memtable. After reaching the maximum size, the memtable is flushed to a SSTable file on disk
* **max_level** - the memtable is implemented as a skip-list; this parameter sets the maximum height of a skip-list node's tower
* **sstable.sparse_index_step** - determines how many data blocks are skipped between entries in the index
* **sstable.bloom_bits_per_key** - how many bits to use for each stored key

## 🏗️ Architecture
Sklad is built around an asynchronous task queue with a small pool of worker threads. This design allows the system to handle a large number of concurrent requests without overwhelming system resources.

Wherever possible, the project uses lock-free data structures and algorithms to minimize contention and improve concurrency, serving as a core design principle throughout the system.

The storage layer is implemented using an LSM-tree (Log-Structured Merge Tree) to optimize write performance.
