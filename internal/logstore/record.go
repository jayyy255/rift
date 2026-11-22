package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

// On-disk frame layout, all integers big-endian. See docs/storage-format.md.
//
//	frame  := bodyLen:uint32 crc:uint32 body
//	body   := magic:uint8 attributes:uint8 offset:int64 timestamp:int64
//	          keyLen:int32 key headerCount:uint16 header* valueLen:uint32 value
//	header := keyLen:uint16 key valueLen:uint32 value
//
// crc is CRC-32C (Castagnoli) over body. keyLen is -1 for a keyless record.
const (
	frameHeaderSize = 8
	magicV1         = 1
	// magic + attributes + offset + timestamp + keyLen + headerCount + valueLen
	minBodySize = 1 + 1 + 8 + 8 + 4 + 2 + 4
	// maxBodySize bounds a single frame so a corrupt length cannot trigger a
	// huge allocation during recovery.
	maxBodySize = 64 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Header is an application-defined key/value pair carried with a record.
type Header struct {
	Key   string
	Value []byte
}

// Record is a single log entry. Offset and TimestampMs are assigned by the log
// on append; values set by callers are ignored.
type Record struct {
	Offset      int64
	TimestampMs int64
	// Key is nil for keyless records. A non-nil empty key is a valid key.
	Key     []byte
	Value   []byte
	Headers []Header
}

// EncodedSize returns the number of bytes the record occupies on disk.
func (r *Record) EncodedSize() int {
	n := frameHeaderSize + minBodySize + len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += 2 + len(h.Key) + 4 + len(h.Value)
	}
	return n
}

// ErrRecordTooLarge is returned when a record cannot be encoded in one frame.
var ErrRecordTooLarge = errors.New("logstore: record too large")

func validateRecord(r *Record) error {
	if r.EncodedSize()-frameHeaderSize > maxBodySize {
		return ErrRecordTooLarge
	}
	if len(r.Headers) > math.MaxUint16 {
		return fmt.Errorf("logstore: %d headers exceeds limit %d", len(r.Headers), math.MaxUint16)
	}
	for _, h := range r.Headers {
		if len(h.Key) > math.MaxUint16 {
			return fmt.Errorf("logstore: header key length %d exceeds limit %d", len(h.Key), math.MaxUint16)
		}
	}
	return nil
}

// appendFrame encodes r (which must have passed validateRecord) onto dst.
func appendFrame(dst []byte, r *Record) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, frameHeaderSize)...)
	bodyStart := len(dst)

	dst = append(dst, magicV1, 0)
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.Offset))
	dst = binary.BigEndian.AppendUint64(dst, uint64(r.TimestampMs))
	if r.Key == nil {
		dst = binary.BigEndian.AppendUint32(dst, math.MaxUint32) // -1
	} else {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Key)))
		dst = append(dst, r.Key...)
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(r.Headers)))
	for _, h := range r.Headers {
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(h.Key)))
		dst = append(dst, h.Key...)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(h.Value)))
		dst = append(dst, h.Value...)
	}
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.Value)))
	dst = append(dst, r.Value...)

	body := dst[bodyStart:]
	binary.BigEndian.PutUint32(dst[start:], uint32(len(body)))
	binary.BigEndian.PutUint32(dst[start+4:], crc32.Checksum(body, crcTable))
	return dst
}

// frameError classifies why bytes at a position are not a valid frame.
type frameError struct {
	// incomplete is true when the bytes end before the frame does, the normal
	// signature of a torn write at the log tail.
	incomplete bool
	reason     string
}

func (e *frameError) Error() string { return e.reason }

// decodeFrame decodes the frame at the start of buf. The returned record's
// slices alias buf. n is the total frame size.
func decodeFrame(buf []byte) (rec Record, n int, err error) {
	if len(buf) < frameHeaderSize {
		return Record{}, 0, &frameError{incomplete: true, reason: "truncated frame header"}
	}
	bodyLen := binary.BigEndian.Uint32(buf)
	if bodyLen < minBodySize || bodyLen > maxBodySize {
		return Record{}, 0, &frameError{reason: fmt.Sprintf("invalid frame length %d", bodyLen)}
	}
	n = frameHeaderSize + int(bodyLen)
	if len(buf) < n {
		return Record{}, 0, &frameError{incomplete: true, reason: fmt.Sprintf("truncated frame: have %d of %d bytes", len(buf), n)}
	}
	body := buf[frameHeaderSize:n]
	if want, got := binary.BigEndian.Uint32(buf[4:]), crc32.Checksum(body, crcTable); want != got {
		return Record{}, 0, &frameError{reason: fmt.Sprintf("checksum mismatch: stored %08x, computed %08x", want, got)}
	}
	rec, err = decodeBody(body)
	if err != nil {
		return Record{}, 0, &frameError{reason: err.Error()}
	}
	return rec, n, nil
}

func decodeBody(b []byte) (Record, error) {
	if b[0] != magicV1 {
		return Record{}, fmt.Errorf("unsupported record format version %d", b[0])
	}
	var r Record
	r.Offset = int64(binary.BigEndian.Uint64(b[2:]))
	r.TimestampMs = int64(binary.BigEndian.Uint64(b[10:]))
	p := 18
	need := func(k int) error {
		if len(b)-p < k {
			return errors.New("malformed record body")
		}
		return nil
	}
	keyLen := int32(binary.BigEndian.Uint32(b[p:]))
	p += 4
	if keyLen >= 0 {
		if err := need(int(keyLen)); err != nil {
			return Record{}, err
		}
		r.Key = b[p : p+int(keyLen) : p+int(keyLen)]
		p += int(keyLen)
	} else if keyLen != -1 {
		return Record{}, errors.New("malformed record key length")
	}
	if err := need(2); err != nil {
		return Record{}, err
	}
	hc := int(binary.BigEndian.Uint16(b[p:]))
	p += 2
	if hc > 0 {
		r.Headers = make([]Header, hc)
		for i := range hc {
			if err := need(2); err != nil {
				return Record{}, err
			}
			kl := int(binary.BigEndian.Uint16(b[p:]))
			p += 2
			if err := need(kl + 4); err != nil {
				return Record{}, err
			}
			r.Headers[i].Key = string(b[p : p+kl])
			p += kl
			vl := int(binary.BigEndian.Uint32(b[p:]))
			p += 4
			if err := need(vl); err != nil {
				return Record{}, err
			}
			r.Headers[i].Value = b[p : p+vl : p+vl]
			p += vl
		}
	}
	if err := need(4); err != nil {
		return Record{}, err
	}
	vl := int(binary.BigEndian.Uint32(b[p:]))
	p += 4
	if err := need(vl); err != nil {
		return Record{}, err
	}
	r.Value = b[p : p+vl : p+vl]
	p += vl
	if p != len(b) {
		return Record{}, errors.New("trailing bytes in record body")
	}
	return r, nil
}
