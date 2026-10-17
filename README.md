# DeepSeek search adapter

An independently built CLIProxyAPI C ABI plugin that adds server-managed web
search to configured non-native Codex models. This repository is a standalone Go
module that builds without the
CLIProxyAPI source tree or Go module. It uses only the public JSON/C ABI and a
YAML parser.

The plugin replaces Responses `web_search` / `web_search_preview` declarations
with a private function before calling the original model through the host.
It executes DeepSeek Search/Fetch requests, appends matching tool results, and
continues the same model. Other client tools remain client-executed.

## Build

Requires Go 1.26+, a C compiler, and a target compatible with the host's libc.

```sh
make build
make test
# Or use the Go toolchain and compiler in Podman:
make container-build
make container-test
```

The Linux artifact is `build/deepseek-search.so`. Build on the target platform
for macOS/Windows and use its appropriate shared-library extension. A library
built in the Debian Go image requires glibc; an Alpine host needs a musl build.
The generated C header is not required to load the plugin.

## Release distribution

GitHub Actions tests and builds Linux `amd64` and `arm64` on main pushes, pull
requests, and manual runs. Pushing a version tag such as `v0.1.0` additionally
creates a GitHub Release after both architectures pass. The release assets use
the CLIProxyAPI plugin-store convention:

- `deepseek-search_0.1.0_linux_amd64.zip`
- `deepseek-search_0.1.0_linux_arm64.zip`
- `checksums.txt` containing SHA-256 hashes for both archives.

Each ZIP contains `deepseek-search.so` at its root, plus the license, README,
and configuration example. These Linux builds require glibc >= 2.34; they do
not target musl/Alpine. Verify the downloaded archives with
`sha256sum -c checksums.txt`. For manual installation, place the library in
`<plugins.dir>/linux/<architecture>/deepseek-search.so` and configure the plugin
as shown below. Non-tag runs keep the packages as Actions artifacts and do not
publish a release. Publishing release assets does not automatically add this
repository to a plugin-store registry.

The packaging follows the host's `internal/pluginstore` contract and the
[cliproxy-costs release workflow](https://github.com/MelonSmasher/cliproxy-costs/blob/main/.github/workflows/ci.yml).

## Configure

Merge [config.example.yaml](config.example.yaml) into the host configuration,
replace `plugins.dir` with the directory containing the library, replace the
model name, and provide `DEEPSEEK` in the host
environment (or its `.env`). Alternatively set `api_key: "sk-your-key"` under
`plugins.configs.deepseek-search` in a private config. A non-empty `api_key`
takes precedence over `api_key_env`; an empty key falls back to that environment
variable (default: `DEEPSEEK`).
Do not commit credentials. The model must already be registered by the host.

Each `models` entry accepts `name`, optional `upstream` (defaults to `name`), and
optional `provider`. Only allowlisted model names are intercepted. All Responses
requests for those models pass through the adapter so continuation requests can
restore search state even when they omit `tools`. Do not include native Codex
models unless you explicitly intend to replace their native search.

The accompanying small host extension recognizes the optional C ABI
`capabilities.web_search_models` declaration. It advertises
`cpa_capabilities.web_search` for CPA catalog clients only when the active router
selects a declaring executor in both execution modes. It does not change
`supports_search_tool`, which describes tool discovery rather than web search.
Older hosts can execute the plugin when the client sends a search declaration,
but cannot advertise its added capability. CLIProxyAPIHome direct plugin
execution is currently unsupported; the extension does not advertise it there.

## Execution and context

- Multiple searches per response and successive search/model rounds are
  supported, including up to four queries in one search call. Searches run
  sequentially under service rate limiting.
- Mixed client tools pause model continuation until their results arrive;
  function and custom tool call IDs are validated before replay.
- Both `previous_response_id` and full-history replay are supported. Public
  search items have random IDs and map to the hidden call/result history.
  Replaying an item does not repeat its search. Full transcripts replace prior
  history instead of appending stale state.
- `/responses/compact` hydrates hidden search history before delegating to the
  host's nonstreaming compaction path, and retains the compact output for later
  continuation. This requires an upstream that supports host compaction.
- State is isolated by the host-authenticated `caller_scope`. Hosts without
  frontend authentication share an anonymous scope.
- State is bounded and in memory. Restarting, reconfiguring, or evicting state
  makes dependent response/search IDs unavailable; requests fail explicitly.
  Do not route a conversation across independent plugin processes without
  affinity. A compacted transcript that no longer references old search IDs
  can start fresh.
- Original request fields and raw reasoning/output items are retained. Each
  nested model request goes through host translation, auth scheduling and final
  payload configuration; the plugin does not modify the resulting wire payload.
- SSE output is synthesized after internal rounds finish. It is a single
  Responses lifecycle with stable output indexes and accumulated token usage,
  rather than concatenated upstream streams. Keep-alive comments are sent while
  waiting. This initial release buffers text instead of forwarding token deltas
  immediately. Reasoning summaries and reasoning text include their delta/done
  events so event-driven clients can display them; encrypted reasoning remains
  opaque. Early upstream EOF is an error, not a fabricated completion.
- Search errors become tool results that the model can handle. Limits and
  missing context fail explicitly. There are no post-connect network deadlines;
  cancellation closes nested streams and cancels search requests. Nonstreaming
  calls retain the host callback's request cancellation context.

Search transport uses the host HTTP callbacks for cancellation, proxy policy,
and request logging. Model transport uses the host's configured credentials and
proxy. Search and Fetch are independently rate-limited per plugin client.
Multiple processes or clients sharing a DeepSeek account must coordinate its
account-wide limit externally. Retries are bounded for HTTP 408, 429 and 5xx.

### Streaming limitation

For a client request with `stream: true`, every internal model round uses the
host's streaming API, including rounds after a search. However, the plugin
collects each upstream stream into a completed Responses object and waits until
the search loop finishes (or pauses for a client-managed tool). It then emits
one synthesized SSE response to the client. Text and reasoning delta events
contain buffered content; they do not arrive as the model generates tokens.
While waiting, the client receives only keep-alive comments. Public search-call
items are also delayed. This applies to every request routed through the
plugin, even when the model ultimately makes no search call. A client request
with `stream: false` uses non-streaming upstream calls throughout.

This is a limitation of the current adapter design, not a DeepSeek Search API
or Responses protocol requirement. The orchestration engine consumes complete
response objects: it extracts private search function calls, executes them,
injects results into history, and combines multiple upstream responses into one
public response. The current SSE serializer runs only after that work returns.
It therefore cannot forward live deltas, and a later upstream failure prevents
earlier buffered content from being delivered as a successful response.

True incremental output is possible but requires an event-aware orchestration
path: map item IDs and output indexes across rounds, keep a single public
response lifecycle, hide private search arguments, emit public search events,
and handle cancellation or errors after partial output has already been sent.
Simply forwarding upstream SSE would expose internal calls and multiple
response completion events. The reasoning-event fix restores client rendering
but does not remove this buffering limitation.

## Search scope

Search results include source URLs for the model. `open_page` uses DeepSeek
Fetch; `find_in_page` supports textual page inspection. No persistent search
cache, browser automation, screenshots, or exact native citation rendering is
provided. Existing model-generated citation metadata is preserved. Nonempty
domain filters are rejected rather than silently ignored.
`external_web_access: false` is accepted silently and still uses DeepSeek search;
the adapter does not emulate a cached-only mode. Search queries must fit
DeepSeek's 128 Unicode-character limit. Token counting through this plugin returns an explicit
unsupported error instead of an inaccurate zero count.

API references: [Search and Fetch](https://api-docs.deepseek.com/zh-cn/guides/search/).

## Validation

Unit and race tests use scripted model responses and local HTTP servers. An
optional real-service smoke test uses the key only from the environment:

```sh
DEEPSEEK_LIVE_TEST=1 go test -v ./adapter -run '^TestDeepSeekLive$' -count=1
```

The host repository also includes a real dynamic-library integration test:

```sh
DEEPSEEK_PLUGIN_TEST_LIBRARY="$PWD/standalone/deepseek-search/build/deepseek-search.so" \
  go test -v ./internal/pluginhost -run TestDeepSeekSharedLibraryRoundTrip
```

That test loads the actual shared library, crosses the host callback ABI,
executes local fake search requests, and verifies mixed tools, continuation,
and the synthesized Responses stream. It does not require a DeepSeek key.

`TestDeepSeekSharedLibraryPayloadRulesEverySearchRound` additionally sends each
model round through the real OpenAI-compatible executor and checks final payload
overrides and filters on the wire. Run both with `-run TestDeepSeekSharedLibrary`.

Validated on Linux with the Go 1.26 container: standalone race tests, affected
host package tests, shared-library integration tests, and host compilation.
Live checks used `deepseek-flash` through a temporary proxy and covered search
with `external_web_access: false`, previous-response continuation, and streaming
full-history replay with two search queries. Other upstream model families have
not been live-tested.
