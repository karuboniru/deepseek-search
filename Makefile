GO ?= go
PODMAN ?= podman
GO_IMAGE ?= docker.io/library/golang:1.26-bookworm
VERSION ?= dev
TARGET_OS ?= $(shell $(GO) env GOOS)
ARCH ?= $(shell $(GO) env GOARCH)
BUILD_DIR ?= build
DIST_DIR ?= dist
LIBRARY_EXT_linux = so
LIBRARY_EXT_darwin = dylib
LIBRARY_EXT_windows = dll
LIBRARY_NAME = deepseek-search.$(LIBRARY_EXT_$(TARGET_OS))

.PHONY: build test fmt package container-build container-test

build:
	@case "$(TARGET_OS)" in linux|darwin|windows) ;; *) echo "Unsupported target OS: $(TARGET_OS)" >&2; exit 1 ;; esac
	mkdir -p "$(BUILD_DIR)"
	CGO_ENABLED=1 GOOS="$(TARGET_OS)" GOARCH="$(ARCH)" $(GO) build -trimpath -buildmode=c-shared -ldflags "-s -w -X main.version=$(VERSION)" -o "$(BUILD_DIR)/$(LIBRARY_NAME)" ./cmd/plugin

test:
	$(GO) test -race ./...

fmt:
	gofmt -w adapter cmd

package: build
	bash scripts/package.sh "$(VERSION)" "$(ARCH)" "$(BUILD_DIR)/$(LIBRARY_NAME)" "$(DIST_DIR)" "$(TARGET_OS)"

container-build:
	$(PODMAN) run --rm -v "$(CURDIR):/src:Z" -w /src $(GO_IMAGE) make build VERSION="$(VERSION)"

container-test:
	$(PODMAN) run --rm -v "$(CURDIR):/src:Z" -w /src $(GO_IMAGE) make test
