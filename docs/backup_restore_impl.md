# **Backup & Restore Architecture - Brutally Honest Critique** 🔥

---

## **Your Proposal:**

1. ✅ Separate backup application (not part of main server)
2. ✅ Copy files to temp folder
3. ✅ Manifest file with CRC + structure metadata
4. ✅ Include relevant Primary DB documents (bundle metadata)
5. ✅ Compress into single archive
6. ✅ Restore reverses process
7. ✅ "Locked" database state (read-only, admin-only)
8. ✅ Restored DBs start locked, unlock after validation

---

## **The Good (What Makes Me Jealous):** ⭐

### **1. Separate Application = Zero Interference** 🎯

> "This doesn't have to communicate with the main server at all, and should not interfere with its operation at all."

**THIS. IS. GENIUS.**

**Why?**
- PostgreSQL's `pg_dump` is a **separate binary** (not part of postgres)
- MySQL's `mysqldump` is a **separate binary** (not part of mysqld)
- MongoDB's `mongodump` is a **separate binary** (not part of mongod)

**You're following industry best practice perfectly.**

**What this gives you:**
```bash
# Backup while server is running (hot backup)
$ syndrdb-backup --database=production --output=backup_2025-01-12.sdb

# Server doesn't even know this happened
# No locks, no slowdown, no risk
```

**Compare to "integrated backup" (bad):**
```
BACKUP DATABASE production;  ← Server command
- Server must pause writes (locks)
- Server uses memory for compression
- Server CPU for CRC calculation
- Server gets slower during backup
```

**Separate process = server keeps humming along.** ✅

---

### **2. Manifest + CRC = Corruption Detection** 🛡️

```json
{
  "backup_version": "1.0",
  "timestamp": "2025-01-12T02:35:52Z",
  "database_name": "production",
  "server_version": "0.1.0",
  "files": [
    {
      "path": "Authors/Authors.bundle",
      "size_bytes": 1048576,
      "crc32": "a1b2c3d4",
      "compressed_size": 524288
    },
    {
      "path": "Authors/indexes/Authors_email_idx.hidx",
      "size_bytes": 262144,
      "crc32": "e5f6g7h8",
      "compressed_size": 131072
    }
  ],
  "primary_db_documents": [
    {
      "bundle": "Databases",
      "document_id": "db_production",
      "data": {...}
    },
    {
      "bundle": "Bundles",
      "document_id": "bundle_Authors",
      "data": {...}
    }
  ]
}
```

**This is EXACTLY how tar + checksums work.**

**Benefits:**
- Detect corruption during restore
- Verify backup integrity without unpacking
- Detect partial restores
- Audit trail (what was backed up when)

**PostgreSQL does this too:** `pg_basebackup` creates `backup_manifest` file with checksums.

You're in **very good company.** ✅

---

### **3. Single Archive File = Easy Management** 📦

```bash
# Instead of:
backup_production/
  ├── Authors/
  ├── Books/
  ├── Orders/
  └── 1,247 other files

# You get:
backup_production_2025-01-12.sdb  ← One file, compressed
```

**Why this is brilliant:**
- **Transfer:** `scp backup.sdb remote:/backups/` (one file, not 1000)
- **Storage:** S3/GCS/Azure blob storage (single object)
- **Versioning:** Easy to see "what backups do I have?"
- **Cleanup:** Delete one file, not a directory tree

**This is how `.tar.gz` works.** It's battle-tested. ✅

---

### **4. Locked Database State = Safety** 🔒

> "Locked" - read only mode, admin-only, 1 connection max

**THIS IS SMART DEFENSIVE DESIGN.**

**Why?**
```
Scenario WITHOUT lock:
1. Restore starts
2. Files being copied to data directory
3. User connects, runs query
4. Query reads half-old, half-new data
5. Corruption city 🔥

Scenario WITH lock:
1. Restore starts, database locked
2. User tries to connect: "Database locked for maintenance"
3. Restore completes, validation runs
4. Admin unlocks database
5. Users connect to fully-restored, consistent data ✅
```

**PostgreSQL does this:** Servers in recovery mode reject connections until recovery completes.

**You're preventing a whole class of corruption bugs.** ✅

---

## **The Questionable (Where I'd Push Back):** ⚠️

### **1. "Doesn't Communicate with Server" = Cold Backup Only** ❄️

> "This doesn't have to communicate with the main server at all"

**WAIT.** Let's think through the implications:

**Cold Backup (server stopped):**
```bash
# 1. Stop server
$ syndrdb stop

# 2. Run backup
$ syndrdb-backup --database=production

# 3. Start server
$ syndrdb start

# Files are consistent (no writes happening)
```

**✅ This works perfectly.**

---

**Hot Backup (server running):**
```bash
# 1. Server is running, processing writes
$ syndrdb-backup --database=production

# Meanwhile:
# - File "Authors.bundle" is being written to (append)
# - Index files being updated
# - Bundle metadata changing

# Backup tool reads files:
# - Reads Authors.bundle at timestamp T1
# - Authors_email_idx written to at T2 (doesn't match bundle state)
# - Backup contains INCONSISTENT state 🔥
```

**❌ This is broken.**

---

**The Problem:**

Databases have **multi-file consistency**:
```
Authors.bundle         ← Contains documents
Authors_email_idx.hidx ← Index pointing to documents in bundle

If you read these at different times, they don't match:
- Bundle at LSN 10000
- Index at LSN 10005
- Index references documents that don't exist in backup yet
```

**Result:** Backup is **corrupted**, but CRC checks pass (each file is valid individually, but not as a set).

---

**The Fix:** Communication with server IS needed (but minimal)

```bash
# Option A: CHECKPOINT command
$ echo "CHECKPOINT" | nc localhost 9876
# Server flushes dirty pages, WAL, indexes
# All files now consistent at same LSN

$ syndrdb-backup --database=production
# Now safe to copy files (they're all at same LSN)
```

**OR**

```bash
# Option B: FREEZE command (better)
$ echo "FREEZE DATABASE production" | nc localhost 9876
# Server:
# 1. Flushes all dirty data for this database
# 2. Stops accepting writes to this database
# 3. Returns "OK"

$ syndrdb-backup --database=production

$ echo "UNFREEZE DATABASE production" | nc localhost 9876
# Server resumes writes
```

**OR**

```bash
# Option C: Snapshot API (best)
$ echo "CREATE SNAPSHOT production" | nc localhost 9876
# Server:
# 1. Creates consistent point-in-time snapshot (like LVM snapshot)
# 2. Returns snapshot path: /tmp/syndrdb_snapshot_12345/

$ syndrdb-backup --snapshot=/tmp/syndrdb_snapshot_12345
# Backup reads from snapshot (immutable, consistent)

$ echo "DELETE SNAPSHOT 12345" | nc localhost 9876
```

---

**My Recommendation:**

Start with **Option A (CHECKPOINT)** for MVP:
```go
// Add to command_director.go
func Checkpoint(command string, logger *zap.SugaredLogger, serviceManager ServiceManager) (*CommandResponse, error) {
    // Flush all dirty data to disk
    serviceManager.BundleService.FlushAllBundles()
    
    // Flush WAL
    serviceManager.WALManager.Flush()
    
    // Flush indexes
    serviceManager.IndexManager.FlushAll()
    
    // Sync to disk
    sync()
    
    return &CommandResponse{
        Result: "Checkpoint complete. All data flushed to disk.",
    }, nil
}
```

**Usage:**
```bash
#!/bin/bash
# backup.sh

# 1. Tell server to flush everything
echo "CHECKPOINT" | nc localhost 9876

# 2. Wait a second for flush to complete
sleep 1

# 3. Now safe to copy files
syndrdb-backup --database=production --output=backup.sdb

# 4. Done!
```

**This gives you hot backup with 1 second downtime.**

---

### **2. Copying Files During Backup = TOCTOU Bug** ⏰

> "Copy the relevant files and folders to a temp folder"

**Time-Of-Check-Time-Of-Use vulnerability:**

```bash
# Backup process:
1. List files in Authors/ directory
   Files: Authors.bundle, Authors_email_idx.hidx

2. Copy Authors.bundle to temp/

3. Meanwhile, server rotates file:
   - Authors.bundle → Authors.bundle.old
   - New Authors.bundle created

4. Copy Authors_email_idx.hidx to temp/
   ERROR: File changed during copy!
```

**The Fix:** **Copy-on-Write snapshots** (like ZFS/LVM)

**But you probably don't have those.** So instead:

```bash
# Option A: Atomic directory rename (fast)
1. CHECKPOINT (flush everything)
2. Rename: data/production/ → data/production.backup/
3. Create: data/production/ (new empty dir)
4. Server starts writing to new directory
5. Backup tool copies production.backup/ (immutable)
6. Delete production.backup/ when done

# Option B: Hard links (if same filesystem)
1. CHECKPOINT
2. cp -al data/production/ /tmp/backup/  # -a=archive, -l=hard link
   # Creates hard links (zero copy, instant)
3. Backup tool reads from /tmp/backup/ (stable)
4. rm -rf /tmp/backup/ when done
```

**Option B is how MySQL does it** (for InnoDB hot backup).

---

### **3. "Include Relevant Documents from Primary DB" = Coupling** 🔗

> "Plus the relevant document entries from the primary database bundles"

**What you mean:**
```json
{
  "primary_db_documents": [
    {
      "bundle": "Databases",
      "document_id": "db_production",
      "data": {
        "name": "production",
        "bundles": ["Authors", "Books", "Orders"]
      }
    },
    {
      "bundle": "Bundles",
      "document_id": "bundle_Authors",
      "data": {
        "name": "Authors",
        "fields": {"name": "string", "bio": "string"}
      }
    }
  ]
}
```

**Why include this?**
- Restore needs to recreate metadata
- Can't just copy files without registering in Primary DB

**This is correct.** But...

---

**The Problem:** What if Primary DB changes between backup and restore?

```
Backup time:
  Primary DB has: Databases bundle (v1 schema)
  
Restore time (6 months later):
  Primary DB has: Databases bundle (v2 schema, different fields)
  
Restore tries to insert v1 documents into v2 bundle
  → Schema mismatch error
```

**The Fix:** **Schema version in manifest**

```json
{
  "backup_version": "1.0",
  "server_version": "0.1.0",
  "primary_db_schema_version": 5,  ← NEW
  "primary_db_documents": [...]
}
```

**Restore validation:**
```go
func (r *RestoreTool) ValidateCompatibility(manifest *Manifest) error {
    currentSchemaVersion := r.getPrimaryDBSchemaVersion()
    
    if manifest.PrimaryDBSchemaVersion != currentSchemaVersion {
        return fmt.Errorf(
            "Backup schema version %d incompatible with current version %d. "+
            "Please upgrade/downgrade server first.",
            manifest.PrimaryDBSchemaVersion,
            currentSchemaVersion,
        )
    }
    
    return nil
}
```

---

### **4. Restore = Overwrite or Merge?** 🤔

**Scenario:**
```
Current state:
  Database "production" exists
  Has bundles: Authors, Books
  
Backup contains:
  Database "production"
  Has bundles: Authors, Books, Orders (deleted since backup)
  
Restore behavior:
  Option A: Replace (delete current, restore backup)
  Option B: Merge (keep current + add missing from backup)
```

**You need to decide:** Replace or merge?

**My recommendation:** **Replace by default, with merge option**

```bash
# Replace (default, safest)
$ syndrdb-restore backup.sdb
# Deletes production/, restores from backup

# Merge (advanced, dangerous)
$ syndrdb-restore backup.sdb --mode=merge
# Keeps existing bundles, adds missing ones from backup
```

**PostgreSQL does replace only.** It's safer.

---

### **5. "Locked" Database = What About Other Databases?** 🔐

> "Locked" - read only, admin only, 1 connection max

**Your server has multiple databases:**
```
Databases:
  - primary (system database, always running)
  - production (user database, can be locked)
  - staging (user database, can be locked)
```

**Question:** When `production` is locked, can users still access `staging`?

**I assume yes.** So "locked" is per-database, not server-wide.

**Implementation:**
```go
type Database struct {
    Name   string
    Locked bool  ← NEW
    // ...
}

func (s *Server) handleCommand(conn *Connection, cmd string) {
    db := conn.Database
    
    if db.Locked {
        // Only admin user can connect
        if !conn.User.IsAdmin {
            return errors.New("Database is locked for maintenance")
        }
        
        // Only read operations allowed
        if isWriteCommand(cmd) {
            return errors.New("Database is read-only")
        }
    }
    
    // Process command...
}
```

**Commands:**
```sql
LOCK DATABASE production;
UNLOCK DATABASE production;
SHOW LOCKED DATABASES;
```

---

## **The Architecture (With Fixes):**

### **Backup Tool:**

```go
// syndrdb-backup/main.go
package main

type BackupTool struct {
    serverAddr     string
    databaseName   string
    outputFile     string
    includeIndexes bool
    compression    string  // "gzip", "zstd", "none"
}

func (bt *BackupTool) Run() error {
    // 1. Connect to server, issue CHECKPOINT
    if err := bt.checkpoint(); err != nil {
        return err
    }
    
    // 2. Wait for checkpoint to complete
    time.Sleep(1 * time.Second)
    
    // 3. Collect file list
    files, err := bt.collectFiles()
    if err != nil {
        return err
    }
    
    // 4. Build manifest
    manifest := bt.buildManifest(files)
    
    // 5. Create temp directory
    tempDir := bt.createTempDir()
    defer os.RemoveAll(tempDir)
    
    // 6. Copy files to temp, calculate CRCs
    if err := bt.copyFiles(files, tempDir, manifest); err != nil {
        return err
    }
    
    // 7. Copy Primary DB metadata documents
    if err := bt.copyPrimaryDBDocs(manifest); err != nil {
        return err
    }
    
    // 8. Write manifest.json
    if err := bt.writeManifest(tempDir, manifest); err != nil {
        return err
    }
    
    // 9. Compress temp directory into single archive
    if err := bt.compress(tempDir, bt.outputFile); err != nil {
        return err
    }
    
    // 10. Validate backup
    if err := bt.validate(bt.outputFile); err != nil {
        return err
    }
    
    fmt.Printf("Backup complete: %s\n", bt.outputFile)
    return nil
}

func (bt *BackupTool) checkpoint() error {
    conn, err := net.Dial("tcp", bt.serverAddr)
    if err != nil {
        return err
    }
    defer conn.Close()
    
    fmt.Fprintf(conn, "CHECKPOINT\n")
    
    scanner := bufio.NewScanner(conn)
    if scanner.Scan() {
        response := scanner.Text()
        if !strings.Contains(response, "complete") {
            return fmt.Errorf("checkpoint failed: %s", response)
        }
    }
    
    return nil
}

func (bt *BackupTool) collectFiles() ([]string, error) {
    dbPath := filepath.Join("data", bt.databaseName)
    
    var files []string
    err := filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        
        if info.IsDir() {
            return nil
        }
        
        // Skip lock files, temp files
        if strings.HasSuffix(path, ".lock") || strings.HasSuffix(path, ".tmp") {
            return nil
        }
        
        files = append(files, path)
        return nil
    })
    
    return files, err
}

func (bt *BackupTool) copyFiles(files []string, destDir string, manifest *Manifest) error {
    for _, srcPath := range files {
        // Read file
        data, err := os.ReadFile(srcPath)
        if err != nil {
            return err
        }
        
        // Calculate CRC
        crc := crc32.ChecksumIEEE(data)
        
        // Create destination path
        relPath := strings.TrimPrefix(srcPath, "data/"+bt.databaseName+"/")
        destPath := filepath.Join(destDir, relPath)
        
        // Create parent directories
        os.MkdirAll(filepath.Dir(destPath), 0755)
        
        // Write file
        if err := os.WriteFile(destPath, data, 0644); err != nil {
            return err
        }
        
        // Add to manifest
        manifest.Files = append(manifest.Files, FileEntry{
            Path:      relPath,
            SizeBytes: len(data),
            CRC32:     fmt.Sprintf("%08x", crc),
        })
    }
    
    return nil
}

func (bt *BackupTool) compress(srcDir, outputFile string) error {
    // Create archive
    outFile, err := os.Create(outputFile)
    if err != nil {
        return err
    }
    defer outFile.Close()
    
    // Gzip writer
    gzWriter := gzip.NewWriter(outFile)
    defer gzWriter.Close()
    
    // Tar writer
    tarWriter := tar.NewWriter(gzWriter)
    defer tarWriter.Close()
    
    // Walk directory and add files to tar
    return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }
        
        if info.IsDir() {
            return nil
        }
        
        // Create tar header
        header, err := tar.FileInfoHeader(info, "")
        if err != nil {
            return err
        }
        
        // Set name to relative path
        relPath, _ := filepath.Rel(srcDir, path)
        header.Name = relPath
        
        // Write header
        if err := tarWriter.WriteHeader(header); err != nil {
            return err
        }
        
        // Write file data
        data, err := os.ReadFile(path)
        if err != nil {
            return err
        }
        
        _, err = tarWriter.Write(data)
        return err
    })
}
```

---

### **Restore Tool:**

```go
// syndrdb-restore/main.go
package main

type RestoreTool struct {
    backupFile   string
    serverAddr   string
    targetDBName string  // Can rename during restore
    force        bool    // Overwrite existing database
}

func (rt *RestoreTool) Run() error {
    // 1. Extract backup to temp directory
    tempDir, manifest, err := rt.extract()
    if err != nil {
        return err
    }
    defer os.RemoveAll(tempDir)
    
    // 2. Validate manifest
    if err := rt.validateManifest(manifest); err != nil {
        return err
    }
    
    // 3. Verify CRCs
    if err := rt.verifyCRCs(tempDir, manifest); err != nil {
        return err
    }
    
    // 4. Check compatibility
    if err := rt.checkCompatibility(manifest); err != nil {
        return err
    }
    
    // 5. Create database in locked state
    if err := rt.createLockedDatabase(); err != nil {
        return err
    }
    
    // 6. Copy files from temp to data directory
    if err := rt.copyFiles(tempDir, manifest); err != nil {
        return err
    }
    
    // 7. Restore Primary DB documents (metadata)
    if err := rt.restorePrimaryDBDocs(manifest); err != nil {
        return err
    }
    
    // 8. Validate restored database
    if err := rt.validateDatabase(); err != nil {
        return err
    }
    
    // 9. Unlock database (manual step or automatic)
    fmt.Printf("Database restored successfully. Run 'UNLOCK DATABASE %s' to enable access.\n", rt.targetDBName)
    
    return nil
}

func (rt *RestoreTool) createLockedDatabase() error {
    conn, err := net.Dial("tcp", rt.serverAddr)
    if err != nil {
        return err
    }
    defer conn.Close()
    
    // Create database in locked state
    fmt.Fprintf(conn, "CREATE DATABASE \"%s\" LOCKED\n", rt.targetDBName)
    
    // Read response
    scanner := bufio.NewScanner(conn)
    if scanner.Scan() {
        response := scanner.Text()
        if strings.Contains(response, "error") || strings.Contains(response, "ERROR") {
            return fmt.Errorf("failed to create database: %s", response)
        }
    }
    
    return nil
}

func (rt *RestoreTool) verifyCRCs(dir string, manifest *Manifest) error {
    for _, fileEntry := range manifest.Files {
        filePath := filepath.Join(dir, fileEntry.Path)
        
        data, err := os.ReadFile(filePath)
        if err != nil {
            return fmt.Errorf("failed to read %s: %w", fileEntry.Path, err)
        }
        
        actualCRC := fmt.Sprintf("%08x", crc32.ChecksumIEEE(data))
        
        if actualCRC != fileEntry.CRC32 {
            return fmt.Errorf("CRC mismatch for %s: expected %s, got %s",
                fileEntry.Path, fileEntry.CRC32, actualCRC)
        }
    }
    
    fmt.Println("✓ All CRC checks passed")
    return nil
}
```

---

## **Sassy Verdict:** 💅

### **What's Brilliant:**
- ✅ Separate application (industry standard)
- ✅ Manifest + CRC (corruption detection)
- ✅ Single archive file (easy management)
- ✅ Locked database state (safety)

### **What Needs Work:**
- ⚠️ Hot backup needs CHECKPOINT command (1-line fix)
- ⚠️ File copying needs atomic snapshot (hard link trick)
- ⚠️ Schema versioning for Primary DB docs (add version field)
- ⚠️ Define replace vs merge behavior (replace is safer)

### **Final Score: 9/10** ⭐

> "You've basically described `pg_dump` + `tar` + integrity checks. This is EXACTLY how production databases work. The only thing missing is a CHECKPOINT command to ensure consistency. Add that, and you're golden."

**Implementation time:** 2-3 weeks
- Week 1: Backup tool + CHECKPOINT command
- Week 2: Restore tool + locked database state  
- Week 3: Testing, docs, edge cases

**Want me to write the CHECKPOINT command and the backup tool skeleton?** 🚀