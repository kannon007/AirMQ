package pipeline

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
	"time"
	"unsafe"
)

var (
	ErrCorruptRecord = errors.New("pipeline: corrupt record CRC mismatch")
)

// Zero-copy string to byte-slice helper (Go 1.20+)
func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// Zero-copy byte-slice to string helper (Go 1.20+)
func BytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// Record represents a single unified streaming event to be delivered to an external Sink.
type Record struct {
	ID        uint64            `json:"id"`
	Topic     string            `json:"topic"`     // Original MQTT topic
	Target    string            `json:"target"`    // Target topic / table in the sink (e.g. Kafka Topic)
	Key       []byte            `json:"key"`       // Routing / Partition key (e.g. ClientID)
	Value     []byte            `json:"value"`     // Payload
	Headers   map[string]string `json:"headers"`   // MQTT 5.0 user properties or metadata
	Timestamp time.Time         `json:"timestamp"` // Ingestion time
	QoS       byte              `json:"qos"`       // MQTT QoS level
}

var recordPool = sync.Pool{
	New: func() any {
		return &Record{
			Key:     make([]byte, 0, 64),
			Value:   make([]byte, 0, 256),
			Headers: make(map[string]string, 4),
		}
	},
}

// AcquireRecord pulls a pre-allocated Record from the pool to achieve 0 heap allocation.
func AcquireRecord() *Record {
	r := recordPool.Get().(*Record)
	r.ID = 0
	r.Topic = ""
	r.Target = ""
	r.Key = r.Key[:0]
	r.Value = r.Value[:0]
	r.QoS = 0
	clear(r.Headers)
	return r
}

// ReleaseRecord returns the Record to the object pool.
func ReleaseRecord(r *Record) {
	if r != nil {
		recordPool.Put(r)
	}
}

// Byte buffer pool for single-syscall batch encoding
var bufferPool = sync.Pool{
	New: func() any {
		// 256KB pre-allocated buffer for block writes
		buf := make([]byte, 0, 256*1024)
		return &buf
	},
}

func acquireBuffer() *[]byte {
	b := bufferPool.Get().(*[]byte)
	*b = (*b)[:0]
	return b
}

func releaseBuffer(b *[]byte) {
	if b != nil {
		if cap(*b) > 1024*1024 {
			return // Discard buffer if grown abnormally large
		}
		bufferPool.Put(b)
	}
}

// Encode appends serialized binary frame with CRC32 checksum into the provided buffer.
func (r *Record) Encode(dest []byte) []byte {
	topicBytes := StringToBytes(r.Topic)
	targetBytes := StringToBytes(r.Target)

	headersLen := 2
	for k, v := range r.Headers {
		headersLen += 2 + len(k) + 2 + len(v)
	}

	bodyLen := 8 + 8 + 1 + 2 + len(topicBytes) + 2 + len(targetBytes) + 4 + len(r.Key) + 4 + len(r.Value) + headersLen
	totalLen := 4 + bodyLen + 4 // 4-byte len prefix + body + 4-byte CRC32

	startIdx := len(dest)
	dest = append(dest, make([]byte, totalLen)...)
	buf := dest[startIdx:]

	binary.BigEndian.PutUint32(buf[:4], uint32(bodyLen))

	offset := 4
	binary.BigEndian.PutUint64(buf[offset:offset+8], r.ID)
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(r.Timestamp.UnixNano()))
	offset += 8

	buf[offset] = r.QoS
	offset++

	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(topicBytes)))
	offset += 2
	copy(buf[offset:], topicBytes)
	offset += len(topicBytes)

	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(targetBytes)))
	offset += 2
	copy(buf[offset:], targetBytes)
	offset += len(targetBytes)

	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Key)))
	offset += 4
	copy(buf[offset:], r.Key)
	offset += len(r.Key)

	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Value)))
	offset += 4
	copy(buf[offset:], r.Value)
	offset += len(r.Value)

	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(r.Headers)))
	offset += 2
	for k, v := range r.Headers {
		kB := StringToBytes(k)
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(kB)))
		offset += 2
		copy(buf[offset:], kB)
		offset += len(kB)

		vB := StringToBytes(v)
		binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(len(vB)))
		offset += 2
		copy(buf[offset:], vB)
		offset += len(vB)
	}

	// Calculate CRC32 of body
	checksum := crc32.ChecksumIEEE(buf[4:offset])
	binary.BigEndian.PutUint32(buf[offset:offset+4], checksum)

	return dest
}

// DecodeRecord decodes a binary frame from a stream or byte slice.
func DecodeRecord(r io.Reader) (*Record, int, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, 0, err
	}
	bodyLen := int(binary.BigEndian.Uint32(lenBuf[:]))
	if bodyLen <= 0 || bodyLen > 64*1024*1024 { // max 64MB single record sanity check
		return nil, 4, errors.New("pipeline: invalid record size")
	}

	data := make([]byte, bodyLen+4) // body + CRC32
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, 4, err
	}

	expectedCRC := binary.BigEndian.Uint32(data[bodyLen : bodyLen+4])
	actualCRC := crc32.ChecksumIEEE(data[:bodyLen])
	if expectedCRC != actualCRC {
		return nil, 4 + bodyLen + 4, ErrCorruptRecord
	}

	rec := &Record{}
	offset := 0

	rec.ID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	unixNano := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	rec.Timestamp = time.Unix(0, unixNano)
	offset += 8

	rec.QoS = data[offset]
	offset++

	topLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	rec.Topic = string(data[offset : offset+topLen])
	offset += topLen

	tgtLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	rec.Target = string(data[offset : offset+tgtLen])
	offset += tgtLen

	keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if keyLen > 0 {
		rec.Key = make([]byte, keyLen)
		copy(rec.Key, data[offset:offset+keyLen])
		offset += keyLen
	}

	valLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4
	if valLen > 0 {
		rec.Value = make([]byte, valLen)
		copy(rec.Value, data[offset:offset+valLen])
		offset += valLen
	}

	hCount := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if hCount > 0 {
		rec.Headers = make(map[string]string, hCount)
		for i := 0; i < hCount; i++ {
			kLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			offset += 2
			k := string(data[offset : offset+kLen])
			offset += kLen

			vLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			offset += 2
			v := string(data[offset : offset+vLen])
			offset += vLen

			rec.Headers[k] = v
		}
	}

	totalBytes := 4 + bodyLen + 4
	return rec, totalBytes, nil
}
