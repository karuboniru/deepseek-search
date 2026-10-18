#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
typedef HMODULE library_handle;
#else
#include <dlfcn.h>
typedef void *library_handle;
#endif

/* Keep these definitions in sync with cmd/plugin/main.go. */
typedef struct { void *ptr; size_t len; } cliproxy_buffer;
typedef int (*cliproxy_host_call_fn)(void *, const char *, const uint8_t *, size_t, cliproxy_buffer *);
typedef void (*cliproxy_host_free_fn)(void *, size_t);
typedef struct {
    uint32_t abi_version;
    void *host_ctx;
    cliproxy_host_call_fn call;
    cliproxy_host_free_fn free_buffer;
} cliproxy_host_api;
typedef int (*cliproxy_plugin_call_fn)(char *, uint8_t *, size_t, cliproxy_buffer *);
typedef void (*cliproxy_plugin_free_fn)(void *, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);
typedef struct {
    uint32_t abi_version;
    cliproxy_plugin_call_fn call;
    cliproxy_plugin_free_fn free_buffer;
    cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;
typedef int (*cliproxy_plugin_init_fn)(cliproxy_host_api *, cliproxy_plugin_api *);

static int load_symbol(library_handle library, const char *name, void *target,
                       size_t target_size) {
#ifdef _WIN32
    FARPROC symbol = GetProcAddress(library, name);
#else
    void *symbol = dlsym(library, name);
#endif
    if (!symbol || sizeof(symbol) != target_size) {
        fprintf(stderr, "cannot load required ABI export: %s\n", name);
        return 0;
    }
    memcpy(target, &symbol, target_size);
    return 1;
}

static int host_call(void *ctx, const char *method, const uint8_t *request,
                     size_t len, cliproxy_buffer *response) {
    int *calls = (int *)ctx;
    (void)request;
    (void)len;
    ++*calls;
    fprintf(stderr, "unexpected host callback: %s\n", method ? method : "(null)");
    if (response) {
        response->ptr = NULL;
        response->len = 0;
    }
    return 1;
}

static void host_free(void *ptr, size_t len) {
    (void)len;
    free(ptr);
}

static int call_and_check(cliproxy_plugin_call_fn call, cliproxy_plugin_free_fn release,
                          char *method) {
    uint8_t request[] = "{}";
    cliproxy_buffer response = {0};
    int status = call(method, request, sizeof(request) - 1, &response);
    char *json = NULL;
    int valid = 0;
    if (response.ptr && response.len > 0 && response.len < SIZE_MAX) {
        json = (char *)malloc(response.len + 1);
        if (json) {
            memcpy(json, response.ptr, response.len);
            json[response.len] = '\0';
            valid = status == 0 && strstr(json, "\"ok\":true") != NULL &&
                    strstr(json, "deepseek-search") != NULL;
        }
    }
    if (!valid) {
        fprintf(stderr, "%s failed (status %d): %s\n", method, status,
                json ? json : "missing or invalid response buffer");
    }
    if (response.ptr) {
        release(response.ptr, response.len);
    }
    free(json);
    return valid;
}

int main(int argc, char **argv) {
    library_handle library;
    cliproxy_plugin_init_fn init;
    cliproxy_plugin_call_fn call;
    cliproxy_plugin_free_fn release;
    cliproxy_plugin_shutdown_fn shutdown;
    cliproxy_plugin_api plugin = {0};
    int host_calls = 0;
    cliproxy_host_api host = {1, &host_calls, host_call, host_free};
    int valid;

    if (argc != 2) {
        fprintf(stderr, "usage: %s <plugin-library>\n", argv[0]);
        return 1;
    }
#ifdef _WIN32
    library = LoadLibraryA(argv[1]);
    if (!library) {
        fprintf(stderr, "LoadLibraryA failed: %lu\n", (unsigned long)GetLastError());
        return 1;
    }
#else
    library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
    if (!library) {
        fprintf(stderr, "dlopen failed: %s\n", dlerror());
        return 1;
    }
#endif
    if (!load_symbol(library, "cliproxy_plugin_init", &init, sizeof(init)) ||
        !load_symbol(library, "cliproxyPluginCall", &call, sizeof(call)) ||
        !load_symbol(library, "cliproxyPluginFree", &release, sizeof(release)) ||
        !load_symbol(library, "cliproxyPluginShutdown", &shutdown, sizeof(shutdown))) {
        return 1;
    }
    if (init(&host, &plugin) != 0 || plugin.abi_version != 1 ||
        !plugin.call || !plugin.free_buffer) {
        fprintf(stderr, "plugin initialization returned an invalid ABI table\n");
        return 1;
    }

    /* An empty configuration keeps registration disabled and requires no credentials. */
    valid = call_and_check(plugin.call, plugin.free_buffer, "plugin.register");
    if (valid) {
        valid = call_and_check(call, release, "executor.identifier");
    }
    shutdown();
    if (!valid || host_calls != 0) {
        return 1;
    }
    /* Do not unload a Go shared library: its runtime lives until process exit. */
    puts("DeepSeek search plugin ABI smoke test passed");
    return 0;
}
