package main

/*
#include <stdint.h>
#include <stdlib.h>

typedef struct { void* ptr; size_t len; } cliproxy_buffer;
typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);
typedef struct {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;
typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);
typedef struct {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);

static const cliproxy_host_api* stored_host;
static void store_host_api(const cliproxy_host_api* host) { stored_host = host; }
static int call_host_api(const char* method, const uint8_t* request, size_t len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) return 1;
	return stored_host->call(stored_host->host_ctx, method, request, len, response);
}
static void free_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) stored_host->free_buffer(ptr, len);
}
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/karuboniru/deepseek-search/adapter"
	"gopkg.in/yaml.v3"
)

const pluginID = "deepseek-search"

// version is set by the release build.
var version = "0.1.0-dev"

type rpcEnvelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

type modelConfig struct {
	Name     string `yaml:"name"`
	Upstream string `yaml:"upstream"`
	Provider string `yaml:"provider"`
}

type pluginConfig struct {
	Enabled          bool          `yaml:"enabled"`
	APIKey           string        `yaml:"api_key"`
	APIKeyEnv        string        `yaml:"api_key_env"`
	BaseURL          string        `yaml:"base_url"`
	Models           []modelConfig `yaml:"models"`
	MaxRounds        int           `yaml:"max_rounds"`
	MaxCalls         int           `yaml:"max_calls"`
	MaxStates        int           `yaml:"max_states"`
	MaxHistoryItems  int           `yaml:"max_history_items"`
	MaxHistoryBytes  int           `yaml:"max_history_bytes"`
	MaxResponseBytes int64         `yaml:"max_response_bytes"`
	Count            int           `yaml:"count"`
	NeedContent      bool          `yaml:"need_content"`
}

type runtimeConfig struct {
	pluginConfig
	models map[string]modelConfig
	engine *adapter.Engine
}

var activeConfig atomic.Pointer[runtimeConfig]

type lifecycleRequest struct {
	ConfigYAML []byte `json:"config_yaml"`
}

type routeRequest struct {
	SourceFormat   string
	RequestedModel string
	Metadata       map[string]any
}

type executorRequest struct {
	Model           string
	Format          string
	Stream          bool
	Payload         []byte
	OriginalRequest []byte
	Headers         http.Header
	Query           url.Values
	Alt             string
	Metadata        map[string]any
	HostCallbackID  string `json:"host_callback_id"`
	StreamID        string `json:"stream_id"`
}

type executorResponse struct {
	Payload []byte
	Headers http.Header
}

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if host == nil || plugin == nil || host.abi_version != 1 {
		return 1
	}
	C.store_host_api(host)
	plugin.abi_version = 1
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response == nil || method == nil {
		return 1
	}
	response.ptr = nil
	response.len = 0
	var data []byte
	if request != nil && requestLen > 0 {
		data = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	raw, err := handleMethod(C.GoString(method), data)
	if err != nil {
		raw = errorEnvelope("plugin_error", err.Error(), 0)
	}
	if len(raw) > 0 {
		response.ptr = C.CBytes(raw)
		response.len = C.size_t(len(raw))
	}
	if err != nil {
		return 1
	}
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {}

func handleMethod(method string, raw []byte) ([]byte, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if err := configure(raw); err != nil {
			return nil, err
		}
		return okEnvelope(registration())
	case "model.route":
		return routeModel(raw)
	case "executor.identifier":
		return okEnvelope(map[string]string{"identifier": pluginID})
	case "executor.execute":
		return execute(raw)
	case "executor.execute_stream":
		return executeStream(raw)
	case "executor.count_tokens":
		return errorEnvelope("unsupported", "token counting is unavailable for the DeepSeek search executor", http.StatusNotImplemented), nil
	default:
		return errorEnvelope("unknown_method", "unknown method", http.StatusNotFound), nil
	}
}

func configure(raw []byte) error {
	var request lifecycleRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return fmt.Errorf("decode plugin configuration: %w", err)
	}
	cfg := pluginConfig{APIKeyEnv: "DEEPSEEK"}
	if len(request.ConfigYAML) > 0 {
		if err := yaml.Unmarshal(request.ConfigYAML, &cfg); err != nil {
			return fmt.Errorf("decode plugin YAML: %w", err)
		}
	}
	if strings.TrimSpace(cfg.APIKeyEnv) == "" {
		cfg.APIKeyEnv = "DEEPSEEK"
	}
	for name, value := range map[string]int{
		"max_rounds": cfg.MaxRounds, "max_calls": cfg.MaxCalls,
		"max_states": cfg.MaxStates, "max_history_items": cfg.MaxHistoryItems,
		"max_history_bytes": cfg.MaxHistoryBytes, "count": cfg.Count,
	} {
		if value < 0 {
			return fmt.Errorf("%s must be nonnegative", name)
		}
	}
	if cfg.MaxResponseBytes < 0 {
		return errors.New("max_response_bytes must be nonnegative")
	}
	models := make(map[string]modelConfig, len(cfg.Models))
	for i := range cfg.Models {
		entry := cfg.Models[i]
		entry.Name = strings.TrimSpace(entry.Name)
		entry.Upstream = strings.TrimSpace(entry.Upstream)
		entry.Provider = strings.TrimSpace(entry.Provider)
		if entry.Name == "" {
			return errors.New("model name is required")
		}
		if _, exists := models[entry.Name]; exists {
			return fmt.Errorf("duplicate configured model %q", entry.Name)
		}
		if entry.Upstream == "" {
			entry.Upstream = entry.Name
		}
		cfg.Models[i] = entry
		models[entry.Name] = entry
	}
	if cfg.Enabled && len(models) == 0 {
		return errors.New("enabled plugin requires at least one model")
	}
	runtime := &runtimeConfig{pluginConfig: cfg, models: models}
	if cfg.Enabled {
		key := strings.TrimSpace(cfg.APIKey)
		if key == "" {
			key = strings.TrimSpace(getenv(strings.TrimSpace(cfg.APIKeyEnv)))
		}
		if key == "" {
			return errors.New("DeepSeek API key is required")
		}
		client, err := adapter.NewDeepSeekClient(adapter.ClientOptions{
			APIKey: key, BaseURL: cfg.BaseURL, Count: cfg.Count,
			NeedContent: cfg.NeedContent, MaxResponseBytes: cfg.MaxResponseBytes,
			HTTPClient: &http.Client{
				Transport:     hostHTTPTransport{},
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			},
		})
		if err != nil {
			return errors.New("invalid DeepSeek client configuration")
		}
		runtime.engine = adapter.NewEngine(client, adapter.Options{
			MaxRounds: cfg.MaxRounds, MaxCalls: cfg.MaxCalls,
			MaxStates: cfg.MaxStates, MaxHistoryItems: cfg.MaxHistoryItems,
			MaxHistoryBytes: cfg.MaxHistoryBytes,
		})
	}
	activeConfig.Store(runtime)
	return nil
}

func currentConfig() *runtimeConfig {
	cfg := activeConfig.Load()
	if cfg == nil {
		return &runtimeConfig{models: map[string]modelConfig{}}
	}
	return cfg
}

func registration() any {
	cfg := currentConfig()
	models := make([]string, 0, len(cfg.Models))
	if cfg.Enabled && cfg.engine != nil {
		for _, model := range cfg.Models {
			models = append(models, model.Name)
		}
	}
	return struct {
		SchemaVersion uint32 `json:"schema_version"`
		Metadata      any    `json:"metadata"`
		Capabilities  any    `json:"capabilities"`
	}{
		SchemaVersion: 6,
		Metadata: struct {
			Name             string
			Version          string
			Author           string
			GitHubRepository string
			ConfigFields     []any
		}{pluginID, version, "karuboniru", "https://github.com/karuboniru/deepseek-search", []any{}},
		Capabilities: struct {
			ModelRouter           bool     `json:"model_router"`
			Executor              bool     `json:"executor"`
			ExecutorModelScope    string   `json:"executor_model_scope"`
			ExecutorInputFormats  []string `json:"executor_input_formats"`
			ExecutorOutputFormats []string `json:"executor_output_formats"`
			WebSearchModels       []string `json:"web_search_models,omitempty"`
		}{true, true, "static", []string{"openai-response"}, []string{"openai-response"}, models},
	}
}

func routeModel(raw []byte) ([]byte, error) {
	var req routeRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode route request: %w", err)
	}
	cfg := currentConfig()
	_, allowed := cfg.models[req.RequestedModel]
	return okEnvelope(struct {
		Handled    bool
		TargetKind string
		Reason     string
	}{cfg.Enabled && allowed && isResponsesFormat(req.SourceFormat), "self", "deepseek_search"})
}

func isResponsesFormat(format string) bool {
	return format == "openai-response" || format == "responses"
}

func okEnvelope(v any) ([]byte, error) {
	result, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rpcEnvelope{OK: true, Result: result})
}

func errorEnvelope(code, message string, status int) []byte {
	raw, _ := json.Marshal(rpcEnvelope{OK: false, Error: &rpcError{Code: code, Message: message, HTTPStatus: status}})
	return raw
}

var getenv = os.Getenv

var hostCall = callHost

func callHost(method string, request any) (json.RawMessage, error) {
	rawRequest, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode %s request: %w", method, err)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	cRequest := C.CBytes(rawRequest)
	defer C.free(cRequest)
	var response C.cliproxy_buffer
	code := C.call_host_api(cMethod, (*C.uint8_t)(cRequest), C.size_t(len(rawRequest)), &response)
	var rawResponse []byte
	if response.ptr != nil {
		rawResponse = C.GoBytes(response.ptr, C.int(response.len))
		C.free_host_buffer(response.ptr, response.len)
	}
	var env rpcEnvelope
	if err := json.Unmarshal(rawResponse, &env); err != nil {
		return nil, fmt.Errorf("decode %s host response: %w", method, err)
	}
	if code != 0 || !env.OK {
		if env.Error != nil {
			return nil, fmt.Errorf("%s: %s", method, env.Error.Message)
		}
		return nil, fmt.Errorf("%s host callback failed", method)
	}
	return env.Result, nil
}

func runEngine(ctx context.Context, cfg *runtimeConfig, req executorRequest) ([]byte, error) {
	model, ok := cfg.models[req.Model]
	if !cfg.Enabled || !ok || !isResponsesFormat(req.Format) {
		return nil, errors.New("model is not enabled for DeepSeek search")
	}
	if cfg.engine == nil {
		return nil, errors.New("DeepSeek search engine is unavailable")
	}
	body := req.OriginalRequest
	if len(body) == 0 {
		body = req.Payload
	}
	ctx = context.WithValue(ctx, callbackIDContextKey{}, req.HostCallbackID)
	if req.Alt == "responses/compact" {
		return cfg.engine.Compact(ctx, tenantIdentity(req), body, func(ctx context.Context, body []byte) ([]byte, error) {
			return executeHostModelNonstream(ctx, req, model, body)
		})
	}
	return cfg.engine.Run(ctx, tenantIdentity(req), body, func(ctx context.Context, body []byte) ([]byte, error) {
		if !req.Stream {
			return executeHostModelNonstream(ctx, req, model, body)
		}
		return executeHostModel(ctx, req, model, body)
	})
}

func execute(raw []byte) ([]byte, error) {
	var req executorRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, fmt.Errorf("decode executor request: %w", err)
	}
	result, err := runEngine(context.Background(), currentConfig(), req)
	if err != nil {
		return errorEnvelope("executor_error", err.Error(), http.StatusBadGateway), nil
	}
	return okEnvelope(executorResponse{Payload: result, Headers: http.Header{"Content-Type": {"application/json"}}})
}

func tenantIdentity(req executorRequest) string {
	if scope, ok := req.Metadata["caller_scope"].(string); ok && scope != "" {
		return scope
	}
	return "anonymous"
}
