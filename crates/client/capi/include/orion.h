/*
 * The Orion client for C.
 *
 * Connect to a comm core, publish values to a topic, subscribe to one and read it. The calls
 * are the same whether the comm core is on this board, across the network or over a link.
 *
 * Rules that hold for every call:
 *
 * - A call returns ORION_OK (0), a positive count where it says so, or a negative
 *   OrionStatus. orion_last_error() gives the calling thread's last failure in words.
 * - Nothing here allocates memory the caller frees, other than the handles, which are freed
 *   with their own close or free call. Bytes go into buffers the caller supplies.
 * - No thread is started. Blocking calls block the caller, for as long as they are told.
 * - A handle belongs to one thread: the one that made the Orion it came from.
 * - A handle that has been closed or freed must not be used again.
 */
#ifndef ORION_H
#define ORION_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The version of this surface. The shared library is liborion_c.so.<major>. */
#define ORION_ABI_MAJOR 1
#define ORION_ABI_MINOR 0

typedef enum OrionStatus {
  ORION_OK = 0,
  /* An argument is null, not text, not a name, or out of range. */
  ORION_E_ARGUMENT = -1,
  /* The connection broke or the comm core went quiet. orion_reconnect may help. */
  ORION_E_LOST = -2,
  /* The comm core said no. Asking again does not change it. */
  ORION_E_REFUSED = -3,
  /* The time allowed ran out. */
  ORION_E_TIMEOUT = -4,
  /* The caller's buffer is too small. The sample's len says what it needs. */
  ORION_E_BUFFER = -5,
  /* The library was built for another major version than the caller. */
  ORION_E_VERSION = -6,
  /* Something inside the library went wrong. The handle should be closed. */
  ORION_E_INTERNAL = -7
} OrionStatus;

/* The type of a field. */
typedef enum OrionFieldType {
  ORION_BOOL = 1,
  ORION_INT = 2,
  ORION_FLOAT = 3,
  ORION_TEXT = 4,
  ORION_BYTES = 5
} OrionFieldType;

/* One field of a topic. */
typedef struct OrionField {
  const char *name;
  uint8_t type; /* OrionFieldType */
  const char *unit; /* may be null */
} OrionField;

/* One sample. Its bytes are in the buffer the caller gave. */
typedef struct OrionSample {
  uint16_t field;   /* which field, by its place in the declaration */
  uint64_t time_ns; /* the time the sample belongs to */
  size_t len;       /* bytes of the sample, also when the buffer was too small */
  uint64_t skipped; /* samples lost before this one, for reading too slowly */
} OrionSample;

typedef struct Orion Orion;
typedef struct OrionPublisher OrionPublisher;
typedef struct OrionSubscriber OrionSubscriber;

/* ORION_OK if this library can serve a caller built against major.minor. Call it first. */
int32_t orion_check_version(uint32_t major, uint32_t minor);

/* The calling thread's last failure, in words. Valid until its next failing call. */
const char *orion_last_error(void);

/*
 * Connects to the comm core at `address` as the program `name`. An address is a socket path
 * ("/run/orion/orion.sock"), "orion+tcp://host[:port]" or "serial:<path>[@baud]"; null is the
 * board's own socket. `timeout_ms` is how long connecting and each request may take.
 */
int32_t orion_connect(const char *address, const char *name, uint32_t timeout_ms, Orion **out);

/* Connects again after ORION_E_LOST, trying for up to `timeout_ms`, and says everything again. */
int32_t orion_reconnect(Orion *orion, uint32_t timeout_ms);

/* Keeps the session and takes in what the comm core sends, waiting up to `wait_ms`. */
int32_t orion_pump(Orion *orion, uint32_t wait_ms);

/* Closes the session. Its publishers and subscribers must be freed first. */
void orion_close(Orion *orion);

/* Declares a topic of `field_count` fields and returns its publisher. */
int32_t orion_declare(Orion *orion, const char *topic, const OrionField *fields, size_t field_count,
                      uint32_t history, uint32_t max_payload, OrionPublisher **out);

/* Publishes one sample of `field`, belonging to `time_ns`. */
int32_t orion_publish(OrionPublisher *publisher, uint16_t field, uint64_t time_ns, const uint8_t *payload,
                      size_t len);

/* How many subscribers the topic has. */
int32_t orion_publisher_subscribers(const OrionPublisher *publisher, uint32_t *out);

void orion_publisher_free(OrionPublisher *publisher);

/* Subscribes to a topic. One nobody has declared yet is waited for. */
int32_t orion_subscribe(Orion *orion, const char *topic, OrionSubscriber **out);

/* Sleeps until there is something to read, for up to `wait_ms`. 1 if there is, 0 if not. */
int32_t orion_wait(OrionSubscriber *subscriber, uint32_t wait_ms);

/* The next sample, in the order published. 1 with a sample, 0 with nothing new. */
int32_t orion_next(OrionSubscriber *subscriber, OrionSample *sample, uint8_t *buffer, size_t capacity);

/* The newest sample of `field`. 1 with a sample, 0 if there is none yet. */
int32_t orion_latest(OrionSubscriber *subscriber, uint16_t field, OrionSample *sample, uint8_t *buffer,
                     size_t capacity);

void orion_subscriber_free(OrionSubscriber *subscriber);

#ifdef __cplusplus
}
#endif

#endif /* ORION_H */
