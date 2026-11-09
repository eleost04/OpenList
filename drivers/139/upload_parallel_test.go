package _139

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
)

func ensureTestHTTPClient(t *testing.T) {
	if base.HttpClient == nil {
		base.HttpClient = &http.Client{}
		t.Cleanup(func() { base.HttpClient = nil })
	}
}

// TestUploadPartsPipelinedHedgesSlowTail: the first PUT of the last part hangs; a hedge must
// re-upload it and win, every part must arrive intact, and progress must end at exactly 100%.
func TestUploadPartsPipelinedHedgesSlowTail(t *testing.T) {
	ensureTestHTTPClient(t)
	const partSize = 1 << 20
	const nParts = 12
	data := make([]byte, partSize*nParts-12345)
	for i := range data {
		data[i] = byte(i * 7)
	}

	var mu sync.Mutex
	got := map[int][]byte{}
	hits := map[int]int{}
	var slowCancelled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pn, _ := strconv.Atoi(r.URL.Query().Get("part"))
		mu.Lock()
		hits[pn]++
		first := hits[pn] == 1
		mu.Unlock()
		if pn == nParts && first {
			// take the body, then never answer: hang until the client gives up on this connection
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
				slowCancelled.Store(true)
			case <-time.After(30 * time.Second):
			}
			return
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			return
		}
		mu.Lock()
		got[pn] = b
		mu.Unlock()
	}))
	defer srv.Close()

	var parts []PartInfo
	for off, n := int64(0), int64(1); off < int64(len(data)); n++ {
		sz := min(int64(len(data))-off, partSize)
		parts = append(parts, PartInfo{PartNumber: n, PartSize: sz, ParallelHashCtx: ParallelHashCtx{PartOffset: off}})
		off += sz
	}
	urlFor := func(pn int) PersonalPartInfo {
		return PersonalPartInfo{PartNumber: pn, UploadUrl: fmt.Sprintf("%s/?part=%d", srv.URL, pn)}
	}
	var first []PersonalPartInfo
	for pn := 1; pn <= 5; pn++ {
		first = append(first, urlFor(pn))
	}
	fetch := func(i, end int) ([]PersonalPartInfo, error) {
		var l []PersonalPartInfo
		for pn := i + 1; pn <= end; pn++ {
			l = append(l, urlFor(pn))
		}
		return l, nil
	}
	var last float64
	var pmu sync.Mutex
	p := driver.NewProgress(int64(len(data)), func(v float64) { pmu.Lock(); last = v; pmu.Unlock() })

	d := &Yun139{}
	d.UploadThread = 4
	start := time.Now()
	if err := d.uploadPartsPipelined(context.Background(), parts, first, fetch, bytes.NewReader(data), p); err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	// The stuck body fits in loopback socket buffers, so this exercises the hedgeStuckAge path.
	if el := time.Since(start); el > hedgeStuckAge+5*time.Second {
		t.Fatalf("took %s; hedge should have finished the stuck part after ~%s", el, hedgeStuckAge)
	} else {
		t.Logf("finished in %s", el.Round(time.Millisecond))
	}
	for _, pi := range parts {
		pn := int(pi.PartNumber)
		want := data[pi.ParallelHashCtx.PartOffset : pi.ParallelHashCtx.PartOffset+pi.PartSize]
		if !bytes.Equal(got[pn], want) {
			t.Fatalf("part %d: got %d bytes, want %d (content mismatch)", pn, len(got[pn]), len(want))
		}
	}
	if hits[nParts] != 2 {
		t.Fatalf("stuck part hit %d times, want 2 (primary + hedge)", hits[nParts])
	}
	deadline := time.Now().Add(2 * time.Second)
	for !slowCancelled.Load() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !slowCancelled.Load() {
		t.Fatalf("stuck primary connection was not cancelled after the hedge won")
	}
	if p.Done != int64(len(data)) || last != 100 {
		t.Fatalf("progress done=%d/%d last=%.2f, want exact 100%%", p.Done, len(data), last)
	}
}

// TestUploadPartsPipelinedError: a part that always fails must fail the whole upload, not hang.
func TestUploadPartsPipelinedError(t *testing.T) {
	ensureTestHTTPClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Query().Get("part") == "2" {
			w.WriteHeader(400)
		}
	}))
	defer srv.Close()
	data := make([]byte, 3<<20)
	var parts []PartInfo
	var first []PersonalPartInfo
	for i := 0; i < 3; i++ {
		parts = append(parts, PartInfo{PartNumber: int64(i + 1), PartSize: 1 << 20, ParallelHashCtx: ParallelHashCtx{PartOffset: int64(i) << 20}})
		first = append(first, PersonalPartInfo{PartNumber: i + 1, UploadUrl: fmt.Sprintf("%s/?part=%d", srv.URL, i+1)})
	}
	d := &Yun139{}
	d.UploadThread = 2
	err := d.uploadPartsPipelined(context.Background(), parts, first, nil, bytes.NewReader(data), driver.NewProgress(int64(len(data)), func(float64) {}))
	if err == nil {
		t.Fatal("expected an error for a part that always returns 400")
	}
	t.Logf("got expected error: %v", err)
}

// throttledReaderAt reads the byte range [slowFrom, slowTo) at about rate bytes/s.
type throttledReaderAt struct {
	r                io.ReaderAt
	slowFrom, slowTo int64
	rate             int64
}

func (t *throttledReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= t.slowFrom && off < t.slowTo {
		if len(p) > 32<<10 {
			p = p[:32<<10]
		}
		time.Sleep(time.Duration(int64(len(p)) * int64(time.Second) / t.rate))
	}
	return t.r.ReadAt(p, off)
}

// TestUploadPartsPipelinedSlowPartCompletes: a part that is slow on every connection must be
// reconnected by the strict policy, then finish under the relaxed policy instead of failing the upload.
func TestUploadPartsPipelinedSlowPartCompletes(t *testing.T) {
	ensureTestHTTPClient(t)
	oTick, oStrict, oRelaxed := stallTick, strictStall, relaxedStall
	stallTick = 50 * time.Millisecond
	strictStall = stallPolicy{window: 200 * time.Millisecond, minBytes: 1 << 20}
	relaxedStall = stallPolicy{window: 2 * time.Second, minBytes: 1}
	t.Cleanup(func() { stallTick, strictStall, relaxedStall = oTick, oStrict, oRelaxed })

	const partSize = 1 << 20
	const nParts = 4
	data := make([]byte, partSize*nParts)
	for i := range data {
		data[i] = byte(i*13 + 5)
	}
	var mu sync.Mutex
	got := map[int][]byte{}
	hits := map[int]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pn, _ := strconv.Atoi(r.URL.Query().Get("part"))
		mu.Lock()
		hits[pn]++
		mu.Unlock()
		b, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		mu.Lock()
		got[pn] = b
		mu.Unlock()
	}))
	defer srv.Close()

	var parts []PartInfo
	var first []PersonalPartInfo
	for i := 0; i < nParts; i++ {
		parts = append(parts, PartInfo{PartNumber: int64(i + 1), PartSize: partSize, ParallelHashCtx: ParallelHashCtx{PartOffset: int64(i) * partSize}})
		first = append(first, PersonalPartInfo{PartNumber: i + 1, UploadUrl: fmt.Sprintf("%s/?part=%d", srv.URL, i+1)})
	}
	file := &throttledReaderAt{r: bytes.NewReader(data), slowFrom: partSize, slowTo: 2 * partSize, rate: 512 << 10}
	d := &Yun139{}
	d.UploadThread = 2
	start := time.Now()
	if err := d.uploadPartsPipelined(context.Background(), parts, first, nil, file, driver.NewProgress(int64(len(data)), func(float64) {})); err != nil {
		t.Fatalf("slow part must not fail the upload: %v", err)
	}
	for i := 0; i < nParts; i++ {
		if !bytes.Equal(got[i+1], data[i*partSize:(i+1)*partSize]) {
			t.Fatalf("part %d content mismatch (%d bytes)", i+1, len(got[i+1]))
		}
	}
	if hits[2] < strictAttempts+1 {
		t.Fatalf("slow part hit %d times, want >= %d (strict reconnects, then relaxed)", hits[2], strictAttempts+1)
	}
	t.Logf("finished in %s, slow part attempts=%d", time.Since(start).Round(time.Millisecond), hits[2])
}
