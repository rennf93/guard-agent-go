package guardagent

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
)

// mockIngest mirrors the Guard Core App ingestion contract for tests:
// gzip request bodies are decompressed, the HMAC signature is verified
// over the UNCOMPRESSED body (post-decompression, like
// telemetry_router.py), 413 fires when the decompressed body exceeds
// maxBody, and 200 can carry a partial failure with success:false.
type capturedCall struct {
	Path       string
	Body       []byte // decompressed
	Headers    http.Header
	Compressed bool
	BatchID    string
	Events     []SecurityEvent
	Metrics    []SecurityMetric
	Status     *agentStatusPayload
	Response   int
}

type mockIngest struct {
	*httptest.Server

	mu    sync.Mutex
	calls []capturedCall

	maxBody            int
	force429N          int
	retryAfter         string
	failNextN          int
	force400N          int
	statusCodeOverride int
	partialNext        bool
	requireSignature   bool
	signingSecret      string
}

func newMockIngest(t interface {
	Helper()
	Cleanup(func())
}) *mockIngest {
	m := &mockIngest{maxBody: 262144, retryAfter: "1"}
	m.Server = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.Close)
	return m
}

func (m *mockIngest) handle(w http.ResponseWriter, r *http.Request) {
	body, compressed, err := readMockBody(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	call := capturedCall{
		Path:       r.URL.Path,
		Body:       body,
		Headers:    r.Header.Clone(),
		Compressed: compressed,
	}
	switch r.URL.Path {
	case eventsPath, metricsPath:
		var batch eventBatch
		if err := json.Unmarshal(body, &batch); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		call.BatchID = batch.BatchID
		call.Events = batch.Events
		call.Metrics = batch.Metrics
	case statusPath:
		var st agentStatusPayload
		if err := json.Unmarshal(body, &st); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		call.Status = &st
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if m.requireSignature && !verifyPayloadSignature(body, r.Header.Get(signatureHeader), m.signingSecret) {
		call.Response = http.StatusUnauthorized
		m.calls = append(m.calls, call)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	code, retryAfter := m.decide(len(body))
	call.Response = code
	m.calls = append(m.calls, call)
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	if code != http.StatusOK {
		w.WriteHeader(code)
		return
	}
	if m.partialNext {
		m.partialNext = false
		writeMockJSON(w, batchResponse{Success: false, Errors: []string{"simulated partial failure"}})
		return
	}
	writeMockJSON(w, batchResponse{Success: true})
}

// decide applies the failure knobs in contract order: size guard first,
// then rate limiting, then server errors, then permanent rejection.
func (m *mockIngest) decide(bodyLen int) (int, string) {
	if bodyLen > m.maxBody {
		return http.StatusRequestEntityTooLarge, ""
	}
	if m.force429N > 0 {
		m.force429N--
		return http.StatusTooManyRequests, m.retryAfter
	}
	if m.failNextN > 0 {
		m.failNextN--
		return http.StatusInternalServerError, ""
	}
	if m.force400N > 0 {
		m.force400N--
		return http.StatusBadRequest, ""
	}
	if m.statusCodeOverride > 0 {
		return m.statusCodeOverride, ""
	}
	return http.StatusOK, ""
}

func readMockBody(r *http.Request) ([]byte, bool, error) {
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, true, err
		}
		defer zr.Close()
		body, err := io.ReadAll(zr)
		return body, true, err
	}
	body, err := io.ReadAll(r.Body)
	return body, false, err
}

func writeMockJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (m *mockIngest) callsTo(path string) []capturedCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]capturedCall, 0, len(m.calls))
	for _, c := range m.calls {
		if c.Path == path {
			out = append(out, c)
		}
	}
	return out
}

func (m *mockIngest) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockIngest) setMaxBody(n int) { m.mu.Lock(); m.maxBody = n; m.mu.Unlock() }
func (m *mockIngest) failNext(n int)   { m.mu.Lock(); m.failNextN = n; m.mu.Unlock() }
func (m *mockIngest) rateLimitNext(n int, retryAfter string) {
	m.mu.Lock()
	m.force429N = n
	m.retryAfter = retryAfter
	m.mu.Unlock()
}
func (m *mockIngest) reject400Next(n int) { m.mu.Lock(); m.force400N = n; m.mu.Unlock() }
func (m *mockIngest) respondPartialOnce() { m.mu.Lock(); m.partialNext = true; m.mu.Unlock() }
func (m *mockIngest) requireSignatures(secret string) {
	m.mu.Lock()
	m.requireSignature = true
	m.signingSecret = secret
	m.mu.Unlock()
}
func (m *mockIngest) clearKnobs() {
	m.mu.Lock()
	m.failNextN = 0
	m.force429N = 0
	m.force400N = 0
	m.statusCodeOverride = 0
	m.partialNext = false
	m.mu.Unlock()
}
