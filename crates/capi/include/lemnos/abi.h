/*
 * Lemnos ABI types, version 1.0: the vocabulary every Lemnos C surface shares
 * (status codes, handles, the version handshake, message sinks, buffer and
 * fence descriptors). A surface's own header includes this one; a program
 * using two surfaces sees each type once.
 *
 * Generated from, and checked against, the lemnos-abi crate: its tests
 * compare every value, size and offset here with the crate's, and the layout
 * hash with lemnos_abi::LAYOUT_HASH. Stability rules: the crate's README.
 * A copy of this file ships in each surface crate's include/lemnos/; the
 * surface's tests check the copy is byte for byte this one.
 */
#ifndef LEMNOS_ABI_H
#define LEMNOS_ABI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The version of these types (lemnos_abi::VERSION), not of any surface. */
#define LEMNOS_ABI_TYPES_MAJOR 1
#define LEMNOS_ABI_TYPES_MINOR 0
#define LEMNOS_ABI_TYPES_LAYOUT_HASH 0xa02cfb4a8dd4545bULL

/*
 * Status codes (lemnos_abi::Status), an int32_t: 0 success, -1 to -4095 a
 * negated Linux errno, LEMNOS_E_BASE and below Lemnos's own codes, positive a
 * count where a call says so. A value not named here is valid.
 */
typedef int32_t LemnosStatus;
#define LEMNOS_OK 0
#define LEMNOS_E_BASE (-4096)
#define LEMNOS_E_STALE_HANDLE (LEMNOS_E_BASE)
#define LEMNOS_E_VERSION_MISMATCH (LEMNOS_E_BASE - 1)
#define LEMNOS_E_BUFFER_TOO_SMALL (LEMNOS_E_BASE - 2)
#define LEMNOS_E_PANIC (LEMNOS_E_BASE - 3)

/* A handle: slot index in the low 32 bits, generation in the high 32; 0 is null. */
typedef uint64_t LemnosHandle;
#define LEMNOS_NULL_HANDLE ((LemnosHandle)0)

/* A surface's optional features, from its own <prefix>_features() call. */
typedef uint64_t LemnosFeatures;

/* The version handshake. Each surface defines its own major, minor and hash. */
typedef struct LemnosAbiVersion {
    uint32_t struct_size;
    uint16_t major;
    uint16_t minor;
    uint64_t layout;
} LemnosAbiVersion;

/* Receives a message (UTF-8, not NUL-terminated, valid during the callback). */
typedef struct LemnosMessageSink {
    void *context;
    void (*write)(void *context, const uint8_t *text, size_t len);
} LemnosMessageSink;

#define LEMNOS_NO_SINK ((LemnosMessageSink){ NULL, NULL })

/* One memory plane of a buffer; fd -1 for a plane not used. */
#define LEMNOS_MAX_PLANES 4
typedef struct LemnosPlaneDesc {
    int32_t fd;
    uint32_t reserved;
    uint64_t offset;
    uint64_t stride;
    uint64_t size;
} LemnosPlaneDesc;

/*
 * A buffer by dma-buf: an image (DRM fourcc and modifier) or bytes (fourcc 0,
 * one plane). The descriptors are borrowed for the call unless
 * LEMNOS_BUFFER_OWNED is set, when the receiver closes them.
 */
#define LEMNOS_BUFFER_OWNED (1u << 0)
typedef struct LemnosBufferDesc {
    uint32_t struct_size;
    uint32_t flags;
    uint32_t width;
    uint32_t height;
    uint32_t fourcc;
    uint32_t plane_count;
    uint64_t modifier;
    LemnosPlaneDesc planes[LEMNOS_MAX_PLANES];
} LemnosBufferDesc;

/* A fence by file descriptor; ownership as for a buffer. */
#define LEMNOS_FENCE_NONE 0
#define LEMNOS_FENCE_SYNC_FILE 1
#define LEMNOS_FENCE_SYNCOBJ 2
typedef struct LemnosFenceDesc {
    uint32_t kind;
    int32_t fd;
    /* A syncobj's timeline point (0: binary); 0 for a sync file. */
    uint64_t point;
} LemnosFenceDesc;

#ifdef __cplusplus
}
#endif

#endif /* LEMNOS_ABI_H */
