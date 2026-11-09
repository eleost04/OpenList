package _139

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	log "github.com/sirupsen/logrus"
)

// Parallel upload (ported from AlistGo/alist drivers/139/upload_parallel.go):
// with parallelUpload=true the server wants, for every part except the first,
// the SHA256 midstate at the part's start offset. It signs that into the part's
// upload URL so each part can be verified independently and uploaded out of order.

type parHashCtx struct {
	H          []uint32 `json:"h"`
	PartOffset int64    `json:"partOffset"`
}

type parPartInfo struct {
	PartNumber      int64       `json:"partNumber"`
	PartSize        int64       `json:"partSize"`
	ParallelHashCtx *parHashCtx `json:"parallelHashCtx,omitempty"`
}

const sha256MidstateLen = 4 + 8*4 + 8

func sha256Midstate(h hash.Hash) ([]uint32, int64, error) {
	m, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		return nil, 0, errors.New("sha256 hash state is not exportable")
	}
	state, err := m.MarshalBinary()
	if err != nil {
		return nil, 0, err
	}
	if len(state) < sha256MidstateLen {
		return nil, 0, fmt.Errorf("unexpected sha256 state length %d", len(state))
	}
	regs := make([]uint32, 8)
	for i := range regs {
		regs[i] = binary.BigEndian.Uint32(state[4+i*4:])
	}
	return regs, int64(binary.BigEndian.Uint64(state[len(state)-8:])), nil
}

// planParallelParts scans the file once, returning the full SHA256 and each part's start midstate.
func planParallelParts(f io.ReaderAt, size, partSize int64, up model.UpdateProgress) (string, []parPartInfo, error) {
	if partSize%64 != 0 {
		return "", nil, fmt.Errorf("parallel upload needs part size multiple of 64, got %d", partSize)
	}
	h := sha256.New()
	buf := make([]byte, 4*1024*1024)
	var parts []parPartInfo
	for offset, n := int64(0), int64(1); offset < size; n++ {
		byteSize := min(size-offset, partSize)
		pi := parPartInfo{PartNumber: n, PartSize: byteSize}
		if n > 1 {
			regs, hashed, err := sha256Midstate(h)
			if err != nil {
				return "", nil, err
			}
			if hashed != offset {
				return "", nil, fmt.Errorf("sha256 state covers %d bytes, expected %d", hashed, offset)
			}
			pi.ParallelHashCtx = &parHashCtx{H: regs, PartOffset: offset}
		}
		parts = append(parts, pi)
		if _, err := io.CopyBuffer(h, io.NewSectionReader(f, offset, byteSize), buf); err != nil {
			return "", nil, err
		}
		offset += byteSize
		if up != nil {
			up(float64(offset) / float64(size) * 100)
		}
	}
	if len(parts) == 0 {
		parts = append(parts, parPartInfo{PartNumber: 1})
	}
	return hex.EncodeToString(h.Sum(nil)), parts, nil
}

// partProgress counts one part's bytes into the shared Progress. reset() undoes them before a
// retry; finish() credits the whole part once it is uploaded (by whichever connection won).
type partProgress struct {
	mu       *sync.Mutex
	p        *driver.Progress
	n        int64
	finished bool
}

func (c *partProgress) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return len(b), nil
	}
	c.n += int64(len(b))
	return c.p.Write(b)
}

func (c *partProgress) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	c.p.Done -= c.n
	c.n = 0
}

func (c *partProgress) finish(size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	c.finished = true
	c.p.Done += size - c.n
	c.n = size
	_, _ = c.p.Write(nil) // push the updated percentage
}

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.n.Add(int64(n))
	return n, err
}

var (
	errPartStalled = errors.New("part upload stalled")
	errLostRace    = errors.New("part already uploaded by another connection")
)

// stallPolicy: the watchdog drops a connection when fewer than minBytes were read during the last
// window while unsent data remains.
type stallPolicy struct {
	window   time.Duration
	minBytes int64
}

// vars (not consts) so tests can shrink them.
var (
	stallTick = 2 * time.Second
	// strictStall replaces slow connections early (the first wave of parts often runs at 0.25-1.3 MB/s
	// while healthy ones run 10-35 MB/s). Used for the first strictAttempts tries of a part and for
	// hedges. A strict kill only means "slow", so it does not count as a failure.
	strictStall = stallPolicy{window: 8 * time.Second, minBytes: 16 << 20} // < 2 MB/s
	// relaxedStall only catches connections that stopped moving entirely. Later tries use it, so a
	// part that is merely slow on a congested link still completes instead of failing the upload.
	relaxedStall = stallPolicy{window: 60 * time.Second, minBytes: 1}
)

const (
	strictAttempts = 2
	// primaryAttempts counts real failures only (HTTP/network errors and relaxed-policy stalls).
	primaryAttempts = 5
	// Tail hedging: once every part has been handed out, idle workers re-upload the oldest in-flight
	// part that has run at least hedgeMinAge and still has >= 1/hedgeMinRemainDiv of its bytes unread,
	// or that has run at least hedgeStuckAge (body may sit in the socket buffer awaiting a response).
	hedgeMinAge       = 3 * time.Second
	hedgeStuckAge     = 10 * time.Second
	hedgeMinRemainDiv = 5
)

// putPart PUTs one part. sent is reset and then tracks bytes read from rd. A watchdog cancels the
// attempt (errPartStalled) when the stall policy is violated while unsent data remains.
func putPart(ctx context.Context, url string, rd io.Reader, size int64, counter io.Writer, sent *atomic.Int64, sp stallPolicy) error {
	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	sent.Store(0)
	tick := stallTick // read before the watchdog starts: it may outlive this call briefly
	w := max(int(sp.window/tick), 1)
	go func() {
		t := time.NewTicker(tick)
		defer t.Stop()
		hist := []int64{0}
		for {
			select {
			case <-actx.Done():
				return
			case <-t.C:
				cur := sent.Load()
				hist = append(hist, cur)
				if cur >= size || len(hist) <= w {
					continue
				}
				if cur-hist[len(hist)-1-w] < sp.minBytes {
					cancel(errPartStalled)
					return
				}
			}
		}
	}()
	req, err := http.NewRequestWithContext(actx, http.MethodPut, url, io.TeeReader(&countingReader{rd, sent}, counter))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Origin", "https://yun.139.com")
	req.Header.Set("Referer", "https://yun.139.com/")
	req.ContentLength = size
	res, err := base.HttpClient.Do(req)
	if err != nil {
		if c := context.Cause(actx); errors.Is(c, errPartStalled) || errors.Is(c, errLostRace) {
			return fmt.Errorf("%w after %d/%d bytes", c, sent.Load(), size)
		}
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", res.StatusCode, string(body))
	}
	_, _ = io.Copy(io.Discard, res.Body)
	return nil
}

// partJob is one part plus every connection currently uploading it (the primary and at most one hedge).
type partJob struct {
	pn      int
	url     string
	part    PartInfo
	counter *partProgress
	sent    atomic.Int64 // bytes sent by the current primary attempt
	started atomic.Int64 // unix nanos when the current primary attempt began, 0 when idle

	done     chan struct{}
	doneOnce sync.Once
	mu       sync.Mutex
	cancels  map[int]context.CancelCauseFunc
	nextID   int
	hedged   bool
	hedgeEnd chan struct{} // closed when the hedge attempt returns; nil if never hedged
}

func (j *partJob) isDone() bool {
	select {
	case <-j.done:
		return true
	default:
		return false
	}
}

// markDone records the first successful upload of the part and cancels any other connection still sending it.
func (j *partJob) markDone() {
	j.doneOnce.Do(func() {
		j.mu.Lock()
		close(j.done)
		for _, c := range j.cancels {
			c(errLostRace)
		}
		j.mu.Unlock()
		j.counter.finish(j.part.PartSize)
	})
}

// attempt runs one PUT of the part on its own cancelable context. It returns errLostRace if the
// part was completed by another connection meanwhile.
func (j *partJob) attempt(ctx context.Context, file io.ReaderAt, counter io.Writer, sent *atomic.Int64, sp stallPolicy) error {
	actx, cancel := context.WithCancelCause(ctx)
	j.mu.Lock()
	if j.isDone() {
		j.mu.Unlock()
		cancel(nil)
		return errLostRace
	}
	id := j.nextID
	j.nextID++
	j.cancels[id] = cancel
	j.mu.Unlock()
	defer func() {
		j.mu.Lock()
		delete(j.cancels, id)
		j.mu.Unlock()
		cancel(nil)
	}()
	err := putPart(actx, j.url, io.NewSectionReader(file, j.part.ParallelHashCtx.PartOffset, j.part.PartSize), j.part.PartSize, counter, sent, sp)
	if err == nil {
		j.markDone()
		return nil
	}
	if j.isDone() {
		return errLostRace
	}
	return err
}

// uploadPartsPipelined uploads all parts through a fixed pool of workers. Upload URLs for the next
// batch of 100 are prefetched while earlier parts are still in flight, so there is no barrier
// between batches. Once every part has been handed out, idle workers hedge the slowest in-flight
// parts with a second connection; whichever finishes first wins and the other is cancelled.
func (d *Yun139) uploadPartsPipelined(ctx context.Context, partInfos []PartInfo, first []PersonalPartInfo,
	fetch func(i, end int) ([]PersonalPartInfo, error), file io.ReaderAt, p *driver.Progress) error {
	threads := max(d.UploadThread, 1)
	total := len(partInfos)
	urls := make(map[int]string, total)
	for _, u := range first {
		urls[u.PartNumber] = u.UploadUrl
	}
	gctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	begin := time.Now()
	var progMu sync.Mutex

	var inflightMu sync.Mutex
	inflight := make(map[*partJob]struct{})

	runPrimary := func(j *partJob) error {
		var err error
		fails := 0
		for attempt := 1; fails < primaryAttempts; attempt++ {
			if j.isDone() {
				return nil
			}
			sp := relaxedStall
			if attempt <= strictAttempts {
				sp = strictStall
			}
			if attempt > 1 {
				// A hedge already sending this part makes a parallel retry pointless: wait for its outcome.
				j.mu.Lock()
				he := j.hedgeEnd
				j.mu.Unlock()
				if he != nil {
					select {
					case <-gctx.Done():
						return context.Cause(gctx)
					case <-he:
					}
					if j.isDone() {
						return nil
					}
				}
				select {
				case <-gctx.Done():
					return context.Cause(gctx)
				case <-time.After(time.Second << min(fails, 4)):
				}
			}
			j.counter.reset()
			j.started.Store(time.Now().UnixNano())
			t := time.Now()
			err = j.attempt(gctx, file, j.counter, &j.sent, sp)
			j.started.Store(0)
			dur := time.Since(t)
			switch {
			case err == nil:
				log.Infof("[139] part %d/%d attempt %d ok: %dMB in %s (%.1f MB/s) t+%s", j.pn, total, attempt,
					j.part.PartSize>>20, dur.Round(time.Millisecond), float64(j.part.PartSize)/(1<<20)/dur.Seconds(),
					time.Since(begin).Round(time.Second))
				return nil
			case errors.Is(err, errLostRace):
				log.Infof("[139] part %d/%d primary cancelled after %s: hedge finished first", j.pn, total, dur.Round(time.Millisecond))
				return nil
			case gctx.Err() != nil:
				return context.Cause(gctx)
			}
			if sp == strictStall && errors.Is(err, errPartStalled) {
				log.Infof("[139] part %d/%d attempt %d too slow after %s, reconnecting: %v", j.pn, total, attempt, dur.Round(time.Millisecond), err)
				continue
			}
			fails++
			log.Warnf("[139] part %d/%d attempt %d failed (%d/%d) after %s: %v", j.pn, total, attempt, fails, primaryAttempts, dur.Round(time.Millisecond), err)
		}
		// Out of retries; a hedge may still be sending this part.
		j.mu.Lock()
		he := j.hedgeEnd
		j.mu.Unlock()
		if he != nil {
			select {
			case <-he:
			case <-gctx.Done():
			}
		}
		if j.isDone() {
			return nil
		}
		return fmt.Errorf("part %d: %w", j.pn, err)
	}

	// pickHedge selects the oldest eligible in-flight part that has not been hedged yet.
	pickHedge := func() *partJob {
		inflightMu.Lock()
		defer inflightMu.Unlock()
		now := time.Now().UnixNano()
		var best *partJob
		var bestStart int64
		for j := range inflight {
			st := j.started.Load()
			if st == 0 || j.isDone() {
				continue
			}
			age := time.Duration(now - st)
			remain := j.part.PartSize - j.sent.Load()
			if age < hedgeMinAge || (remain < j.part.PartSize/hedgeMinRemainDiv && age < hedgeStuckAge) {
				continue
			}
			j.mu.Lock()
			hedged := j.hedged
			j.mu.Unlock()
			if hedged {
				continue
			}
			if best == nil || st < bestStart {
				best, bestStart = j, st
			}
		}
		if best == nil {
			return nil
		}
		best.mu.Lock()
		best.hedged = true
		best.hedgeEnd = make(chan struct{})
		best.mu.Unlock()
		return best
	}

	hedgeLoop := func() {
		var sent atomic.Int64
		for gctx.Err() == nil {
			inflightMu.Lock()
			n := len(inflight)
			inflightMu.Unlock()
			if n == 0 {
				return
			}
			j := pickHedge()
			if j == nil {
				select {
				case <-gctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
				continue
			}
			left := j.part.PartSize - j.sent.Load()
			t := time.Now()
			err := j.attempt(gctx, file, io.Discard, &sent, strictStall)
			close(j.hedgeEnd)
			dur := time.Since(t).Round(time.Millisecond)
			switch {
			case err == nil:
				log.Infof("[139] part %d/%d hedge won in %s (primary had %dMB unread)", j.pn, total, dur, left>>20)
			case errors.Is(err, errLostRace):
				log.Infof("[139] part %d/%d hedge cancelled after %s: primary finished first", j.pn, total, dur)
			default:
				log.Warnf("[139] part %d/%d hedge failed after %s: %v", j.pn, total, dur, err)
			}
		}
	}

	jobs := make(chan *partJob)
	var wg sync.WaitGroup
	for w := 0; w < threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				inflightMu.Lock()
				inflight[j] = struct{}{}
				inflightMu.Unlock()
				err := runPrimary(j)
				inflightMu.Lock()
				delete(inflight, j)
				inflightMu.Unlock()
				if err != nil {
					cancel(err)
				}
			}
			hedgeLoop()
		}()
	}

	// Feeder: hand out parts in order, prefetching the next batch of upload URLs ~3 parts per thread early.
	type urlBatch struct {
		list []PersonalPartInfo
		err  error
	}
	var pending chan urlBatch
	pendingStart := -1
	prefetch := func(start int) {
		ch := make(chan urlBatch, 1)
		pending, pendingStart = ch, start
		go func() {
			t := time.Now()
			end := min(start+100, total)
			list, err := fetch(start, end)
			log.Infof("[139] got upload urls for parts %d-%d in %s (err=%v)", start+1, end, time.Since(t).Round(time.Millisecond), err)
			ch <- urlBatch{list, err}
		}()
	}
feed:
	for i := range partInfos {
		pn := i + 1
		if nb := (i/100 + 1) * 100; nb < total && pending == nil && urls[nb+1] == "" && nb-i <= 3*threads {
			prefetch(nb)
		}
		if _, ok := urls[pn]; !ok {
			if pending == nil || pendingStart != i {
				prefetch(i)
			}
			t := time.Now()
			var r urlBatch
			select {
			case r = <-pending:
			case <-gctx.Done():
				break feed
			}
			pending = nil
			if r.err != nil {
				cancel(fmt.Errorf("getUploadUrl for parts %d+: %w", pn, r.err))
				break
			}
			for _, u := range r.list {
				urls[u.PartNumber] = u.UploadUrl
			}
			if w := time.Since(t); w > 100*time.Millisecond {
				log.Infof("[139] waited %s for upload urls of part %d", w.Round(time.Millisecond), pn)
			}
		}
		url, ok := urls[pn]
		if !ok {
			cancel(fmt.Errorf("server returned no upload url for part %d", pn))
			break
		}
		j := &partJob{
			pn:      pn,
			url:     url,
			part:    partInfos[i],
			counter: &partProgress{mu: &progMu, p: p},
			done:    make(chan struct{}),
			cancels: make(map[int]context.CancelCauseFunc),
		}
		select {
		case jobs <- j:
		case <-gctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	return context.Cause(gctx)
}
