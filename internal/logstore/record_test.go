package logstore

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := map[string]Record{
		"keyless":      {Offset: 7, TimestampMs: 1700000000000, Value: []byte("v")},
		"empty key":    {Offset: 1, Key: []byte{}, Value: []byte("v")},
		"empty value":  {Offset: 2, Key: []byte("k"), Value: []byte{}},
		"with headers": {Offset: 3, Key: []byte("k"), Value: []byte("v"), Headers: []Header{{"a", []byte("1")}, {"a", []byte{}}, {"b", []byte("2")}}},
		"large value":  {Offset: 4, Value: bytes.Repeat([]byte("x"), 300_000)},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			buf := appendFrame(nil, &in)
			if len(buf) != in.EncodedSize() {
				t.Fatalf("encoded %d bytes, EncodedSize says %d", len(buf), in.EncodedSize())
			}
			out, n, err := decodeFrame(buf)
			if err != nil {
				t.Fatal(err)
			}
			if n != len(buf) {
				t.Fatalf("consumed %d of %d bytes", n, len(buf))
			}
			if (in.Key == nil) != (out.Key == nil) {
				t.Fatalf("key presence changed: in nil=%v out nil=%v", in.Key == nil, out.Key == nil)
			}
			if out.Value == nil {
				out.Value = []byte{}
			}
			if in.Value == nil {
				in.Value = []byte{}
			}
			if !reflect.DeepEqual(normalize(in), normalize(out)) {
				t.Fatalf("round trip mismatch:\n in  %+v\n out %+v", in, out)
			}
		})
	}
}

func normalize(r Record) Record {
	for i := range r.Headers {
		if r.Headers[i].Value == nil {
			r.Headers[i].Value = []byte{}
		}
	}
	return r
}

// Every single-bit flip anywhere in a frame must be rejected: length flips
// produce invalid or incomplete frames, and body or CRC flips fail the CRC.
func TestFrameDetectsEverySingleBitFlip(t *testing.T) {
	rec := Record{Offset: 42, TimestampMs: 123, Key: []byte("key"), Value: []byte("value"), Headers: []Header{{"h", []byte("x")}}}
	frame := appendFrame(nil, &rec)
	for i := range frame {
		for bit := range 8 {
			buf := bytes.Clone(frame)
			buf[i] ^= 1 << bit
			if _, _, err := decodeFrame(buf); err == nil {
				t.Fatalf("flip of byte %d bit %d was not detected", i, bit)
			}
		}
	}
}

func TestDecodeIncompleteFrames(t *testing.T) {
	frame := appendFrame(nil, &Record{Value: []byte("hello")})
	for cut := range len(frame) {
		_, _, err := decodeFrame(frame[:cut])
		var fe *frameError
		if !errors.As(err, &fe) || !fe.incomplete {
			t.Fatalf("cut at %d: err = %v, want incomplete frame error", cut, err)
		}
	}
}

func TestValidateRecordLimits(t *testing.T) {
	if err := validateRecord(&Record{Value: make([]byte, maxBodySize)}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("err = %v, want ErrRecordTooLarge", err)
	}
	if err := validateRecord(&Record{Headers: []Header{{Key: string(make([]byte, 70000))}}}); err == nil {
		t.Fatal("oversized header key accepted")
	}
}

func FuzzDecodeFrame(f *testing.F) {
	f.Add(appendFrame(nil, &Record{Offset: 1, Key: []byte("k"), Value: []byte("v"), Headers: []Header{{"a", []byte("b")}}}))
	f.Add(appendFrame(nil, &Record{}))
	f.Add([]byte{0, 0, 0, 28, 1, 2, 3, 4})
	f.Fuzz(func(t *testing.T, data []byte) {
		rec, n, err := decodeFrame(data)
		if err != nil {
			return
		}
		// A frame that decodes must re-encode to exactly the same bytes.
		if got := appendFrame(nil, &rec); !bytes.Equal(got, data[:n]) {
			t.Fatalf("re-encoding differs:\n got  %x\n want %x", got, data[:n])
		}
	})
}
