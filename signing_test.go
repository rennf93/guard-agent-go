package guardagent

import (
	"testing"
)

// TestSignPayloadVector pins the exact HMAC-SHA256 scheme the ingestion
// server verifies (payload_signature.py): "v1=" plus the lowercase hex
// HMAC-SHA256 digest of the body under the secret.
func TestSignPayloadVector(t *testing.T) {
	body := []byte(`{"events":[]}`)
	got := signPayload(body, "test-secret")
	want := "v1=55ad76252141f85b34debc7376f87b89c54b939e3a7663464ab3e49029495bf5"
	if got != want {
		t.Fatalf("signPayload got %q want %q", got, want)
	}
}

func TestSignPayloadBodySensitivity(t *testing.T) {
	a := signPayload([]byte(`{"events":[]}`), "test-secret")
	b := signPayload([]byte(`{"events":[1]}`), "test-secret")
	if a == b {
		t.Fatal("different bodies must produce different signatures")
	}
}

func TestVerifyPayloadSignature(t *testing.T) {
	body := []byte(`{"events":[]}`)
	sig := signPayload(body, "test-secret")
	if !verifyPayloadSignature(body, sig, "test-secret") {
		t.Fatal("valid signature must verify")
	}
	if verifyPayloadSignature([]byte(`{"events":[1]}`), sig, "test-secret") {
		t.Fatal("signature over a different body must fail")
	}
	if verifyPayloadSignature(body, "v1=deadbeef", "test-secret") {
		t.Fatal("wrong digest must fail")
	}
	if verifyPayloadSignature(body, "", "test-secret") || verifyPayloadSignature(body, sig, "") {
		t.Fatal("empty signature or secret must fail")
	}
	if verifyPayloadSignature(body, "v2="+sig[3:], "test-secret") {
		t.Fatal("wrong version prefix must fail")
	}
}
