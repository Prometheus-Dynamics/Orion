/*
 * liborion from C (scripts/capi.sh check): the header's layouts hash to the library's, the
 * version handshake, features, and a failed connect with its error text. Linked once against
 * liborion.a and once against liborion.so.
 */
#include <orion/orion.h>

#include <stdalign.h>
#include <stdio.h>
#include <string.h>

static uint64_t hash_words(const uint64_t *words, size_t count) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    for (size_t i = 0; i < count; i++) {
        uint64_t word = words[i];
        for (int byte = 0; byte < 8; byte++) {
            hash ^= word & 0xff;
            hash *= 0x00000100000001b3ULL;
            word >>= 8;
        }
    }
    return hash;
}

static char last[256];

static void keep(void *context, const uint8_t *text, size_t len) {
    (void)context;
    size_t used = strlen(last);
    if (used + len >= sizeof last) {
        len = sizeof last - used - 1;
    }
    memcpy(last + used, text, len);
    last[used + len] = 0;
}

#define FAIL(...)                                                                                  \
    do {                                                                                           \
        fprintf(stderr, __VA_ARGS__);                                                              \
        fputc('\n', stderr);                                                                       \
        return 1;                                                                                  \
    } while (0)

int main(void) {
    const uint64_t words[] = {
        LEMNOS_ABI_TYPES_LAYOUT_HASH,
        sizeof(OrionEvent), alignof(OrionEvent),
        offsetof(OrionEvent, int_value), offsetof(OrionEvent, float_value),
        offsetof(OrionEvent, subject_len), offsetof(OrionEvent, reserved),
        sizeof(OrionLatestFrame), alignof(OrionLatestFrame),
        offsetof(OrionLatestFrame, sequence), offsetof(OrionLatestFrame, payload_len),
        offsetof(OrionLatestFrame, fds), offsetof(OrionLatestFrame, reserved),
    };
    uint64_t computed = hash_words(words, sizeof words / sizeof words[0]);
    if (computed != ORION_LAYOUT_HASH) {
        FAIL("header layouts hash to %#llx, ORION_LAYOUT_HASH is %#llx",
             (unsigned long long)computed, (unsigned long long)ORION_LAYOUT_HASH);
    }

    LemnosAbiVersion mine = {sizeof(LemnosAbiVersion), ORION_ABI_MAJOR, ORION_ABI_MINOR,
                             ORION_LAYOUT_HASH};
    if (orion_check_version(&mine) != LEMNOS_OK) {
        FAIL("the library refuses this header's version");
    }
    LemnosAbiVersion library = orion_version();
    if (library.major != ORION_ABI_MAJOR || library.layout != ORION_LAYOUT_HASH) {
        FAIL("orion_version() is %u.%u %#llx", library.major, library.minor,
             (unsigned long long)library.layout);
    }
    LemnosAbiVersion newer = mine;
    newer.minor = ORION_ABI_MINOR + 1;
    if (orion_check_version(&newer) != LEMNOS_E_VERSION_MISMATCH) {
        FAIL("a caller built against a newer minor is accepted");
    }
    if (!(orion_features() & (1ULL << ORION_FEATURE_LINK))) {
        FAIL("no link client");
    }

    LemnosHandle link = LEMNOS_NULL_HANDLE;
    LemnosStatus status = orion_link_connect("/nonexistent/orion-link.sock", "smoke", 100, &link);
    if (status != -2 /* ENOENT */) {
        FAIL("connect to a missing socket returned %d", status);
    }
    LemnosMessageSink sink = {NULL, keep};
    orion_last_error(sink);
    if (strstr(last, "link socket") == NULL) {
        FAIL("last error: \"%s\"", last);
    }
    if (orion_close(link) != LEMNOS_E_STALE_HANDLE) {
        FAIL("closing the null handle");
    }
    printf("liborion %u.%u ok (layout %#llx)\n", library.major, library.minor,
           (unsigned long long)library.layout);
    return 0;
}
