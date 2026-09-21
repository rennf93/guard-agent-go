package guardagent

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// The ingestion API expects payloads signed with HMAC-SHA256 over the
// uncompressed JSON body, delivered as "v1=" plus the lowercase hex digest
// in the X-Payload-Signature header. The server verifies the signature
// after its gzip middleware decompresses the request, so the signature MUST
// cover the uncompressed bytes even when the body travels gzipped. This
// intentionally differs from the Python and TypeScript agents, which sign
// the post-compression wire bytes and fail verification whenever gzip
// kicks in.

const (
	signatureHeader = "X-Payload-Signature"
	signaturePrefix = "v1="
)

// signPayload returns the "v1=" + HMAC-SHA256 hex signature of body under
// secret.
func signPayload(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// verifyPayloadSignature mirrors the server-side check in
// guard-core-api payload_signature.py: strip the "v1=" prefix, recompute the
// HMAC-SHA256 hex digest, and compare in constant time.
func verifyPayloadSignature(body []byte, signature, secret string) bool {
	if signature == "" || secret == "" {
		return false
	}
	expected := signPayload(body, secret)
	return hmac.Equal([]byte(expected), []byte(signature))
}
