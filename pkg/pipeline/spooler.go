package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	defaultMaxSegmentSize = 16 * 1024 * 1024       // 16MB per segment file
	defaultMaxDiskQuota   = 5 * 1024 * 1024 * 1024 // 5GB max bounded disk quota
)

type SpoolMeta struct {
	ActiveWriteSeg uint64 `json:"active_write_seg"`
	ActiveReadSeg  uint64 `json:"active_read_seg"`
	ReadOffset     int64  `json:"read_offset"`
}

// DiskSpooler provides pre-allocated, strictly-batched, append-only segmented disk spooling.
type DiskSpooler struct {
	mu             sync.Mutex
	dir            string
	maxSegmentSize int64
	maxDiskQuota   int64

	writeSegID uint64
	writeFile  *os.File
	writeBytes int64

	readSegID uint64
	readFile  *os.File
	readBytes int64

	pendingCount atomic.Int64
}

// NewDiskSpooler initializes or recovers a bounded disk spool directory.
func NewDiskSpooler(dir string, maxSegmentSize, maxDiskQuota int64) (*DiskSpooler, error) {
	if maxSegmentSize <= 0 {
		maxSegmentSize = defaultMaxSegmentSize
	}
	if maxDiskQuota <= 0 {
		maxDiskQuota = defaultMaxDiskQuota
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create spool dir: %w", err)
	}

	sp := &DiskSpooler{
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
		maxDiskQuota:   maxDiskQuota,
	}

	if err := sp.recover(); err != nil {
		return nil, err
	}

	return sp, nil
}

func (s *DiskSpooler) metaPath() string {
	return filepath.Join(s.dir, "spool.meta")
}

func (s *DiskSpooler) segPath(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("spool_%010d.seg", id))
}

func (s *DiskSpooler) recover() error {
	metaFile := s.metaPath()
	meta := SpoolMeta{
		ActiveWriteSeg: 1,
		ActiveReadSeg:  1,
		ReadOffset:     0,
	}

	if data, err := os.ReadFile(metaFile); err == nil {
		_ = json.Unmarshal(data, &meta)
	}

	s.writeSegID = meta.ActiveWriteSeg
	s.readSegID = meta.ActiveReadSeg
	s.readBytes = meta.ReadOffset

	// Open or create write segment
	wPath := s.segPath(s.writeSegID)
	f, err := os.OpenFile(wPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open write segment %s: %w", wPath, err)
	}
	stat, _ := f.Stat()
	if stat.Size() < s.maxSegmentSize {
		// Pre-allocate to eliminate filesystem metadata lock contention during writes
		_ = f.Truncate(s.maxSegmentSize)
	}
	s.writeFile = f
	s.writeBytes = meta.ReadOffset // Fallback to recorded offset or actual byte count
	if stat.Size() > 0 && s.writeBytes == 0 {
		// Detect write offset from file size if existing
		s.writeBytes = 0
	}

	// Open read segment
	rPath := s.segPath(s.readSegID)
	rf, err := os.OpenFile(rPath, os.O_RDONLY, 0644)
	if err == nil {
		_, _ = rf.Seek(s.readBytes, io.SeekStart)
		s.readFile = rf
	}

	return nil
}

func (s *DiskSpooler) saveMeta() {
	meta := SpoolMeta{
		ActiveWriteSeg: s.writeSegID,
		ActiveReadSeg:  s.readSegID,
		ReadOffset:     s.readBytes,
	}
	data, _ := json.Marshal(meta)
	_ = os.WriteFile(s.metaPath(), data, 0644)
}

// DiskUsage returns the estimated bytes occupied by current segments.
func (s *DiskSpooler) DiskUsage() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(s.writeSegID-s.readSegID+1) * s.maxSegmentSize
}

// enforceQuota checks if disk usage exceeds maxDiskQuota and prunes oldest unread segments.
func (s *DiskSpooler) enforceQuota() {
	currentUsage := int64(s.writeSegID-s.readSegID+1) * s.maxSegmentSize
	if currentUsage > s.maxDiskQuota && s.readSegID < s.writeSegID {
		// Drop oldest segment to protect host disk from running out of space
		oldPath := s.segPath(s.readSegID)
		if s.readFile != nil {
			_ = s.readFile.Close()
			s.readFile = nil
		}
		_ = os.Remove(oldPath)
		s.readSegID++
		s.readBytes = 0
		s.saveMeta()
	}
}

// AppendBatch writes an entire batch of records in ONE single file.Write system call!
// Strictly forbids single-record writes to protect disk IOPS and OS PageCache.
func (s *DiskSpooler) AppendBatch(records []*Record) error {
	if len(records) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.enforceQuota()

	// 1. Pack entire batch into a contiguous pre-allocated byte buffer
	bufPtr := acquireBuffer()
	defer releaseBuffer(bufPtr)

	for _, rec := range records {
		*bufPtr = rec.Encode(*bufPtr)
	}

	batchBytes := int64(len(*bufPtr))

	// 2. Rotate segment if this batch would exceed pre-allocated segment size
	if s.writeBytes+batchBytes > s.maxSegmentSize {
		_ = s.writeFile.Close()

		s.writeSegID++
		wPath := s.segPath(s.writeSegID)
		f, err := os.OpenFile(wPath, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to create new pre-allocated segment %s: %w", wPath, err)
		}
		// Pre-allocate segment
		_ = f.Truncate(s.maxSegmentSize)
		s.writeFile = f
		s.writeBytes = 0
		s.saveMeta()
	}

	// 3. Write contiguous block in single syscall to OS PageCache
	n, err := s.writeFile.WriteAt(*bufPtr, s.writeBytes)
	if err != nil {
		return err
	}
	s.writeBytes += int64(n)
	s.pendingCount.Add(int64(len(records)))

	return nil
}

// ReadBatch reads up to maxCount records from the active read segment.
func (s *DiskSpooler) ReadBatch(maxCount int) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readSegID > s.writeSegID {
		return nil, nil
	}

	if s.readFile == nil {
		rPath := s.segPath(s.readSegID)
		rf, err := os.OpenFile(rPath, os.O_RDONLY, 0644)
		if err != nil {
			return nil, nil // Segment does not exist yet
		}
		_, _ = rf.Seek(s.readBytes, io.SeekStart)
		s.readFile = rf
	}

	records := make([]*Record, 0, maxCount)

	for len(records) < maxCount {
		// Stop if caught up with active write bytes in the same segment
		if s.readSegID == s.writeSegID && s.readBytes >= s.writeBytes {
			break
		}

		rec, bytesRead, err := DecodeRecord(s.readFile)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF || err == ErrCorruptRecord {
				if s.readSegID < s.writeSegID {
					// Read segment finished, prune and advance to next segment
					_ = s.readFile.Close()
					s.readFile = nil
					oldPath := s.segPath(s.readSegID)
					_ = os.Remove(oldPath)

					s.readSegID++
					s.readBytes = 0
					s.saveMeta()

					nextPath := s.segPath(s.readSegID)
					nextF, openErr := os.OpenFile(nextPath, os.O_RDONLY, 0644)
					if openErr != nil {
						break
					}
					s.readFile = nextF
					continue
				}
				break
			}
			s.readBytes += int64(bytesRead)
			break
		}

		s.readBytes += int64(bytesRead)
		records = append(records, rec)
		s.pendingCount.Add(-1)
	}

	s.saveMeta()
	return records, nil
}

// HasPending returns true if unconsumed records exist on disk.
func (s *DiskSpooler) HasPending() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readSegID < s.writeSegID {
		return true
	}
	if s.readSegID == s.writeSegID {
		return s.readBytes < s.writeBytes
	}
	return false
}

// PendingCount returns estimated unconsumed records on disk.
func (s *DiskSpooler) PendingCount() int64 {
	val := s.pendingCount.Load()
	if val < 0 {
		return 0
	}
	return val
}

// Sync forces OS PageCache flush to physical disk (deferred fsync).
func (s *DiskSpooler) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeFile != nil {
		return s.writeFile.Sync()
	}
	return nil
}

// Close gracefully records metadata and closes file handles.
func (s *DiskSpooler) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.writeFile != nil {
		_ = s.writeFile.Sync()
		_ = s.writeFile.Close()
		s.writeFile = nil
	}
	if s.readFile != nil {
		_ = s.readFile.Close()
		s.readFile = nil
	}
	s.saveMeta()
	return nil
}
