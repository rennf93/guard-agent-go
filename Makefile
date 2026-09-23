GO_IMAGE = golang:1.25-alpine
REDIS_HOST ?= 127.0.0.1

.PHONY: install
install:
	docker run --rm -v "$$PWD":/app -w /app -e GOCACHE=/tmp/gocache $(GO_IMAGE) sh -c "apk add --no-cache git >/dev/null && go mod download"

.PHONY: test
test:
	docker run --rm --network host -v "$$PWD":/app -w /app -e GOCACHE=/tmp/gocache -e REDIS_HOST=$(REDIS_HOST) $(GO_IMAGE) sh -c "apk add --no-cache git >/dev/null && go test ./... && go test -tags integration ./..."

.PHONY: lint
lint:
	docker run --rm -v "$$PWD":/app -w /app -e GOCACHE=/tmp/gocache $(GO_IMAGE) sh -c "gofmt -l . | tee /dev/stderr | (! grep .) && go vet ./..."

.PHONY: bump-version
bump-version:
	@if [ -z "$(VERSION)" ]; then echo "Usage: make bump-version VERSION=x.y.z"; exit 1; fi
	@python3 .github/scripts/bump_version.py $(VERSION)

.PHONY: clean
clean:
	@find . -name "*.test" -type f -delete
	@find . -type d -name "*.gotestchecksum" -exec rm -rf {} + 2>/dev/null || true
	@rm -rf coverage.out coverage.html
