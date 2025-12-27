# MANIFEST file

## Overview
The manifest file (`MANIFEST`) is a critical component of the storage system that maintains a sequential log of all file operations and cleanup operations performed on SSTable files. It serves as the source of truth for tracking which SSTable files are part of the active dataset and which files are pending deletion.

The manifest file serves three primary purposes:
1) **File lifecycle tracking**: records when SSTable files are added to or removed from the `TableFileManager`
2) **Recovery**: enables the system to reconstruct the current state of SSTable files after a restart
3) **Cleanup coordination**: tracks cleanup progress to ensure safe deletion of removed files

## File structure
Each manifest entry is exactly 18 bytes in size with the following layout:

Byte Offset | Field Name      | Type | Size    | Description
------------|-----------------|------|---------|---------------------------
0           | entry_type      | u8   | 1 byte  | Type of manifest entry
1           | level/reserved  | u8   | 1 byte  | Level ID or reserved (0)
2-9         | file_id/offset  | u64  | 8 bytes | File ID or checkpoint offset
10-17       | timestamp       | i64  | 8 bytes | Microsecond timestamp

### File added entry
Records when a new SSTable file is added to the `TableFileManager`.

**Fields:**
- `entry_type`: 0 (`fileAdded`)
- `level`: (u8) the level where the file is placed (0-255)
- `file_id`: (u64) unique identifier for the SSTable file
- `timestamp`: when the file was added

**When written**:
- After flushing a memtable to disk
- After completing a compaction operation


### File removed entry
Records when an SSTable file is removed from the `TableFileManager`'s active file list. **NOTE**: the physical file still exists on disk at this point.

**Fields:**
- `entry_type`: 1 (`fileRemoved`)
- `level`: (u8) the level where the file was located (0-255)
- `file_id`: (u64) unique identifier for the SSTable file
- `timestamp`: when the file was removed from the `TableFileManager`

**When written**:
- After compaction completes and old files are no longer needed

### Cleanup checkpoint:
Records the manifest file offset of the last successfully deleted SSTable file. This checkpoint allows the cleanup process to resume from where it left off.

**Fields:**
- `entry_type`: 2 (`cleanupCheckpoint`)
- `reserved`: unused, set to 0
- `offset`: (u64) byte offset in the manifest file of the last processed removal entry
- `timestamp`: when the checkpoint was recorded

**When written**:
- After the `CleanupTask` successfully deletes some files from disk
