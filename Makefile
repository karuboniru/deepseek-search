GO ?= go
PODMAN ?= podman
GO_IMAGE ?= docker.io/library/golang:1.26-bookworm
VERSION ?= dev
ARCH ?= $(shell $(GO) env GOARCH)
BUILD_DIR ?= build
DIST_DIR ?= dist

.PHONY: build test fmt package container-build container-test

build:
	mkdir -p "$(BUILD_DIR)"
	CGO_ENABLED=1 $(GO) build -trimpath -buildmode=c-shared -ldflags "-s -w -X main.version=$(VERSION)" -o "$(BUILD_DIR)/deepseek-search.so" ./cmd/plugin

test:
	$(GO) test -race ./...

fmt:
	gofmt -w adapter cmd

package: build
	bash scripts/package.sh "$(VERSION)" "$(ARCH)" "$(BUILD_DIR)/deepseek-search.so" "$(DIST_DIR)"

container-build:
	$(PODMAN) run --rm -v "$(CURDIR):/src:Z" -w /src $(GO_IMAGE) make build VERSION="$(VERSION)"

container-test:
	$(PODMAN) run --rm -v "$(CURDIR):/src:Z" -w /src $(GO_IMAGE) make test
