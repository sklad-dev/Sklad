# SStable File Management

This document describes how Sklad tracks and safely deletes SSTable files on disk.

## Overview

Sklad uses a three-component architecture to manage SSTables:
- **TableFileManager**: keeps track of which files are currently active
- **SSTableCache**: performance layer that also ensures we never accidentally open a deleted file
- **Manifest**: persists the state to disk for crash recovery

## File naming

Each SSTable is identified by a `FileHandle` containing two pieces of information: `level` (which LSM level it belongs to) and `file_id` (a unique monotonically increasing number). On disk, these files are stored as: `{path}/{level}.{file_id}.sstable`. For example: `./0.123.sstable` is the SSTable at level 0 with file ID 123.

## Key components

### TableFileManager

`TableFileManager` serves as the authoritative registry of all active SSTable files. It maintains an up-to-date list of which files exist at each LSM level.

For readers:
- Call `acquireFilesAtLevel(level)` to get a stable snapshot of files at that level
- Iterate through that snapshot—even if compaction happens during your read, your list won't change

For writers:
- Call `addFileAtLevel(level, file_id)` when you create a new SSTable (during flush or compaction)
- Call `deleteFilesAtLevel(level, file_ids)` to mark files as no longer active (after compaction)

### SSTableCache

The `SSTableCache` is a best-effort LRU-style cache that does two jobs. It caches recently-used SSTable files so we don't constantly re-read them from disk, and it prevents opening files that have been deleted.

How `get(handle, file_manager)` works:
1. Scan cache entries for a matching `{ level, file_id }`:
   - If found and not marked `is_deleted`: acquire reference and return the cached record
   - If found but marked `is_deleted`: evict it and return null
   - If not found: proceed to step 2

2. Consult TableFileManager call `file_manager.acquireFilesAtLevel(handle.level)` and check if `handle.file_id` is present in the active list
   - If not found in TableFileManager return `null` (file has been logically deleted)
   - If found in TableFileManager proceed to step 3

3. Open and cache call `SSTable.open(handle)` to read the file from disk, wrap it in a ref-counted record, and attempt to insert into cache
   - Cache insertion is best-effort (may fail if cache is full)
   - The opened record is returned regardless of whether caching succeeded

**Important:** When you're done with a cached SSTable, you must call `record.release()` to decrement the reference count.

### Manifest

The [manifest file](./manifest.md) is an append-only log that records all file additions and removals. During startup, `Manifest.recover()` reads this log to figure out which files were deleted before a crash, ensuring we don't treat them as active.

## Safe file deletion

Deleting files safely is a two-phase process. We separate the *logical* removal (making a file invisible to new readers) from the *physical* deletion (actually removing it from disk).

### Phase 1: Logical removal (immediate)

When compaction completes:
1. Call `TableFileManager.deleteFilesAtLevel(level, file_ids)` to remove old files from the active set
2. Record `fileRemoved` entries in the manifest

At this point, the files are **logically deleted** but still exist on disk. `SSTableCache.get()` will return `null` for these files.

### Phase 2: Physical deletion (asynchronous)

Physical deletion happens later. When the following conditions are met a `CleanupTask` is issued:
- Enough time has passed since the last cleanup (configured by `cleanup.interval_seconds`)
- Enough files have been deleted (configured by `cleanup.file_count_threshold`)

When cleanup runs it iterates over `fileRemoved` entries from `MANIFEST` starting at the last checkpoint. For each removed file handle:
1) It calls `SSTableCache.get(handle, &table_file_manager)`
    - Because the file was removed from `TableFileManager`, a cache miss will return null
    - If the file is still present in the cache, get may return a record: cleanup stops to avoid deleting something that could still be referenced
2) If get returned `null`, cleanup deletes `{level}.{file_id}.sstable` from disk
3) After deleting one or more files, cleanup writes a `cleanupCheckpoint` with the last processed offset and flushes the manifest, then resets cleanup counters

## Practical guidance
- ✅ **Always** obtain SSTables through `TableFileManager` and `SSTableCache.get()`
- ✅ **Always** call `release()` on cache records when you're done
- ✅ **Always** use `deleteFilesAtLevel()` to remove files (never delete `.sstable` files directly)
- ❌ **Never** open `.sstable` files by constructing filenames yourself
- ❌ **Never** delete SSTable files directly from disk
