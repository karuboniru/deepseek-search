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

The artifact is `build/deepseek-search.so` on Linux,
`build/deepseek-search.dylib` on macOS, or `build/deepseek-search.dll` on Windows.
`make build` selects the Go target OS and architecture. For cross-compilation,
provide a matching C toolchain, for example:

```sh
make package TARGET_OS=windows ARCH=arm64 CC=aarch64-w64-mingw32-clang VERSION=0.4.2
```

A library built in the Debian Go image requires glibc; an Alpine host needs a
musl build.
The generated C header is not required to load the plugin.

## Release distribution

GitHub Actions builds Linux, macOS (`darwin`), and Windows for both `amd64` and
`arm64` on main pushes, PRs, and manual runs. Pushing a version tag such as
`v0.4.2` publishes a release only after all six builds and runtime checks pass.

- Linux builds use Debian Bookworm; arm64 tests and library loading run under QEMU.
- Windows builds cross-compile on Linux using a pinned, SHA-256-verified
  LLVM-MinGW UCRT toolchain. The compiled tests and actual DLL are then exercised
  on Windows amd64 and Windows arm64 runners.
- macOS builds run tests and load the dylib on native Intel and Apple Silicon
  runners with a macOS 12 deployment target.
- Packaging checks each binary's format, architecture, and all four exported
  C ABI functions. A C smoke test loads the library, initializes the ABI,
  registers the disabled plugin, calls its identifier, and releases its buffers.

The plugin-store assets are named:

```text
deepseek-search_<version>_linux_amd64.zip
deepseek-search_<version>_linux_arm64.zip
deepseek-search_<version>_darwin_amd64.zip
deepseek-search_<version>_darwin_arm64.zip
deepseek-search_<version>_windows_amd64.zip
deepseek-search_<version>_windows_arm64.zip
checksums.txt
```

Each ZIP contains exactly one platform library at its root, plus the license,
README, and configuration example. `checksums.txt` covers all six ZIPs in
sha256sum format. Linux builds require glibc >= 2.34 and do not target musl/Alpine;
macOS builds target macOS 12 or later; Windows builds use UCRT and target Windows
10 or later (ARM64 runtime tests use Windows 11). macOS libraries are not
Developer ID signed or notarized.

For manual installation, place the matching library in
`<plugins.dir>/<goos>/<goarch>/deepseek-search.<extension>` and configure the
plugin as shown below. Non-tag runs keep packages as Actions artifacts.
Publishing release assets does not automatically add this
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

### Per-search defaults and overrides

`count` and `need_content` in the plugin configuration are defaults (5 and
false when omitted). The model can optionally override either field in a
single `action: "search"` call, for example:

```json
{"action":"search","query":"Go release notes","count":3,"need_content":true}
```

Omitted fields use the configuration defaults independently. Explicit
`need_content: false` overrides a configured true. Overrides apply only to that
call and do not change defaults for later searches, rounds, or conversations.
When a call contains multiple `queries`, its options apply to each query;
`count` is the per-query result count. Separate calls can choose different
options. These fields do not apply to `open_page` or `find_in_page`.

`count` must be an integer from 1 to 50; `need_content` must be a boolean.
Invalid values (including null) become tool errors without issuing a search,
so the model can correct its arguments. The model-facing tool description
explicitly warns that including webpage content can consume a large amount of
context and recommends enabling it only when summaries are insufficient.
Existing response and history size limits still apply.

### Search execution budgets

Omitting `max_rounds` and `max_calls` uses 8 model rounds and 16 search/page
calls per client request. Set either field to `0` to disable that limit;
positive values enforce a limit and negative configuration values are rejected.
For unlimited rounds and calls:

```yaml
max_rounds: 0
max_calls: 0
```

This does not disable history item/byte limits, upstream stream size limits,
search rate limiting, or cancellation on client disconnect. The fallback search
cache remains bounded (16 search slots per `max_states` when calls are unlimited).
A model that continues searching may keep using upstream tokens and search calls
until it finishes, the client cancels, or another limit is reached.

## Execution and context

- Multiple searches per response and successive search/model rounds are
  supported, including up to four queries in one search call. Searches run
  sequentially under service rate limiting.
- Mixed client tools pause model continuation until their results arrive;
  function and custom tool call IDs are validated before replay.
- Both `previous_response_id` and full-history replay are supported. Public
  search items have random IDs and carry their call/result history in
  `web_search_call.encrypted_content`, with server memory as a fallback.
  Replaying an item does not repeat its search. Full transcripts replace prior
  history instead of appending stale state.
- `/responses/compact` hydrates hidden search history before delegating to the
  host's nonstreaming compaction path, and retains the compact output for later
  continuation. This requires an upstream that supports host compaction.
- State is isolated by the host-authenticated `caller_scope`. Hosts without
  frontend authentication share an anonymous scope.
- State is bounded and in memory. Full-history replay first uses client-returned
  search context, then cached search records. When both are missing, a placeholder
  tells the model the historical search result is unavailable; replay does not
  repeat the search or fail just because that search record was evicted.
  Requests containing only `previous_response_id` still need the saved
  conversation and process affinity; a missing response ID cannot reconstruct
  an entire conversation. Full-history replay can recover independently of it.
- Original request fields and raw reasoning/output items are retained. Each
  nested model request goes through host translation, auth scheduling and final
  payload configuration; the plugin does not modify the resulting wire payload.
- Streaming requests forward public text, reasoning, and client-tool argument
  deltas as upstream events arrive. Multiple model rounds share one public
  Responses lifecycle with stable item IDs, global output indexes, monotonically
  increasing sequence numbers, and accumulated token usage. Private search
  calls are not exposed as executable client tools. Their complete replay data
  is carried in the final search item as described below. Keep-alive
  comments continue during otherwise idle periods.
- Search errors become tool results that the model can handle. Invalid payloads
  and exceeded limits fail explicitly. There are no post-connect network deadlines;
  cancellation closes nested streams and cancels search requests. Nonstreaming
  calls retain the host callback's request cancellation context.

Search transport uses the host HTTP callbacks for cancellation, proxy policy,
and request logging. Model transport uses the host's configured credentials and
proxy. Search and Fetch are independently rate-limited per plugin client.
Multiple processes or clients sharing a DeepSeek account must coordinate its
account-wide limit externally. Retries are bounded for HTTP 408, 429 and 5xx.

### Streaming behavior and remaining limits

For `stream: true`, every internal model round uses the host's streaming API,
including rounds after searches. Public text and reasoning deltas are delivered
immediately, without waiting for that round or the whole search loop to finish.
Client-tool arguments are also streamed once the tool is identified as public.
Only one `response.created` / `response.in_progress` lifecycle is exposed, and
only the final response emits `response.completed` and `[DONE]`.

Once a model round supplies complete search calls, the plugin emits public
`web_search_call` items and `response.web_search_call.in_progress` / `searching`
events before executing each search, followed by completion and item-done
notifications. Multiple searches still execute sequentially. Search execution
waits for the model round's completed response so its calls, reasoning, and
history can be validated together. A function whose name is not yet available
is temporarily buffered to avoid leaking private search arguments.

The plugin still retains completed items for context replay and final response
assembly; retaining history does not delay live delivery. If an upstream only
provides completed snapshots, the plugin synthesizes missing events when those
snapshots arrive. It cannot create token-level latency that the upstream does
not provide. Reconciliation avoids duplicate text and rejects contradictions
between already emitted content and final snapshots.

If a later upstream round fails after partial output, the public response emits
`response.failed` rather than a successful completion. Already delivered content
cannot be retracted. A disconnected client cancels active model/search requests;
no additional error event can be guaranteed once the transport is disconnected.
Non-streaming requests continue to use non-streaming upstream calls.

### Client-carried search context

The `encrypted_content` field contains **plaintext, versioned JSON**, not
ciphertext. The field name is used for client compatibility. It includes the
original search call, result, and ordering metadata needed to rebuild upstream
history. Clients and anyone with access to their saved transcripts can read it;
it is not a signature or proof of authenticity.

A valid returned payload takes precedence over server memory and is decoded
without replacing its search results with cached data. Missing, null, or empty
payloads fall back to the tenant-scoped cache, then to an explicit unavailable
result placeholder. Malformed nonempty payloads are rejected rather than silently
substituted. After a valid client payload is decoded, the corresponding search
cache entry is evicted immediately to reserve capacity for clients that cannot
replay it. Saved response snapshots remain available for `previous_response_id`.
A request that restores one or more search entries from cache emits one info log
with event `search_context_cache_fallback`; it never logs per restored item or
includes search queries, results, or credentials. No recovery path automatically
repeats an old search. Decoded history and ordinary request fields retain their configured byte limits.
The wire request allows up to four times the history byte limit to accommodate
JSON escaping in replay payloads; this does not enlarge the model context budget.

Streaming includes the complete payload in `response.output_item.done` as well
as the final response. RikkaHub's Responses implementation preserves the raw
search item. The inspected Codex version clears this field for providers whose
`name` is not exactly `"OpenAI"`; set that display name on your custom provider
if you want client-carried replay. This is a version-dependent compatibility
workaround and also affects other OpenAI-specific client behavior. Clients that
strip the field continue to use the cache/placeholder fallback.

Compaction restores available search results before invoking upstream compaction.
Once the client replaces old history with a summary, subsequent requests depend
on that summary, not on indefinite retention of old search results.

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
and the Responses stream. `TestDeepSeekSharedLibraryLiveStream` gates upstream
completion until reasoning and text deltas cross the actual C ABI. It does not require a DeepSeek key.

`TestDeepSeekSharedLibraryPayloadRulesEverySearchRound` additionally sends each
model round through the real OpenAI-compatible executor and checks final payload
overrides and filters on the wire. Run both with `-run TestDeepSeekSharedLibrary`.

Validated on Linux with the Go 1.26 container: standalone race tests, affected
host package tests, shared-library integration tests, and host compilation.
Live checks used `deepseek-flash` through a temporary proxy and covered search
with `external_web_access: false`, previous-response continuation, and streaming
full-history replay with two search queries. Other upstream model families have
not been live-tested.

Live-stream regression tests use explicit synchronization barriers rather than
sleep-based timing: upstream completion and search responses remain blocked
until the client has received the expected early events. Coverage includes
multiple searches/rounds, partial client-tool results, previous-response and
full-history replay, final output/event consistency, fragmented SSE, duplicate
IDs, malformed/contradictory events, upstream failure, and client disconnects.
