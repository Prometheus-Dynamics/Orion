/*
 * A C program that publishes and subscribes through liborion_c.
 *
 * Usage: pubsub <socket>. It declares a topic from one session, subscribes to it from
 * another, publishes a hundred values and reads them back in order. It prints "ok" and
 * exits 0 if everything was as it should be.
 */
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#include "orion.h"

#define CHECK(call)                                                                              \
  do {                                                                                           \
    int32_t status_ = (call);                                                                    \
    if (status_ < 0) {                                                                           \
      fprintf(stderr, "%s: %d: %s\n", #call, status_, orion_last_error());                      \
      return 1;                                                                                  \
    }                                                                                            \
  } while (0)

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: %s <socket>\n", argv[0]);
    return 2;
  }
  CHECK(orion_check_version(ORION_ABI_MAJOR, ORION_ABI_MINOR));
  /* A library of another major is refused, in words. */
  if (orion_check_version(ORION_ABI_MAJOR + 1, 0) != ORION_E_VERSION || strlen(orion_last_error()) == 0) {
    fprintf(stderr, "another major was not refused\n");
    return 1;
  }

  Orion *sensor = NULL, *reader = NULL;
  CHECK(orion_connect(argv[1], "c-sensor", 2000, &sensor));
  CHECK(orion_connect(argv[1], "c-reader", 2000, &reader));

  /* What cannot work says so and leaves nothing behind. */
  Orion *nobody = NULL;
  if (orion_connect("/nonexistent/orion.sock", "c", 200, &nobody) != ORION_E_LOST || nobody != NULL) {
    fprintf(stderr, "a missing socket was not reported as lost\n");
    return 1;
  }
  if (orion_connect("not an address", "c", 200, &nobody) != ORION_E_ARGUMENT) {
    fprintf(stderr, "a bad address was not refused\n");
    return 1;
  }

  const OrionField fields[] = {
      {.name = "count", .type = ORION_INT, .unit = NULL},
      {.name = "label", .type = ORION_TEXT, .unit = ""},
  };
  OrionPublisher *counter = NULL;
  CHECK(orion_declare(sensor, "c.counter", fields, 2, 256, 32, &counter));
  OrionPublisher *twice = NULL;
  if (orion_declare(reader, "c.counter", fields, 2, 256, 32, &twice) != ORION_E_REFUSED) {
    fprintf(stderr, "a second publisher was not refused\n");
    return 1;
  }

  OrionSubscriber *reading = NULL;
  CHECK(orion_subscribe(reader, "c.counter", &reading));
  /* Nothing published yet: the wait ends on time with nothing. */
  int32_t waited = orion_wait(reading, 50);
  if (waited != 0) {
    fprintf(stderr, "a wait with nothing to read returned %d\n", waited);
    return 1;
  }
  /* The publisher hears of its subscriber. */
  uint32_t subscribers = 0;
  for (int tries = 0; tries < 200 && subscribers == 0; tries++) {
    CHECK(orion_pump(sensor, 5));
    CHECK(orion_publisher_subscribers(counter, &subscribers));
  }
  if (subscribers != 1) {
    fprintf(stderr, "%u subscribers\n", subscribers);
    return 1;
  }

  for (int64_t value = 1; value <= 100; value++) {
    CHECK(orion_publish(counter, 0, (uint64_t)value * 1000, (const uint8_t *)&value, sizeof value));
  }
  CHECK(orion_publish(counter, 1, 100000, (const uint8_t *)"done", 4));
  /* A sample that does not fit the declaration is refused where it is made. */
  uint8_t big[64] = {0};
  if (orion_publish(counter, 0, 1, big, sizeof big) != ORION_E_ARGUMENT) {
    fprintf(stderr, "an oversized sample was published\n");
    return 1;
  }

  int64_t expected = 1;
  int got_label = 0;
  uint8_t buffer[32];
  OrionSample sample;
  while (expected <= 100 || !got_label) {
    waited = orion_wait(reading, 2000);
    CHECK(waited);
    if (waited == 0) {
      fprintf(stderr, "nothing arrived; waiting for %" PRId64 "\n", expected);
      return 1;
    }
    for (;;) {
      int32_t found = orion_next(reading, &sample, buffer, sizeof buffer);
      CHECK(found);
      if (found == 0) {
        break;
      }
      if (sample.field == 0) {
        int64_t value;
        memcpy(&value, buffer, sizeof value);
        if (sample.len != sizeof value || value != expected || sample.time_ns != (uint64_t)value * 1000 ||
            sample.skipped != 0) {
          fprintf(stderr, "got %" PRId64 ", expected %" PRId64 "\n", value, expected);
          return 1;
        }
        expected++;
      } else if (sample.len == 4 && memcmp(buffer, "done", 4) == 0) {
        got_label = 1;
      }
    }
  }

  /* The newest value of a field, and a buffer too small for it. */
  CHECK(orion_latest(reading, 0, &sample, buffer, sizeof buffer));
  int64_t newest;
  memcpy(&newest, buffer, sizeof newest);
  if (newest != 100) {
    fprintf(stderr, "latest is %" PRId64 "\n", newest);
    return 1;
  }
  if (orion_latest(reading, 1, &sample, buffer, 2) != ORION_E_BUFFER || sample.len != 4) {
    fprintf(stderr, "a small buffer was not reported with the size needed\n");
    return 1;
  }
  if (orion_next(NULL, &sample, buffer, sizeof buffer) != ORION_E_ARGUMENT) {
    fprintf(stderr, "a null handle was not refused\n");
    return 1;
  }

  orion_subscriber_free(reading);
  orion_publisher_free(counter);
  orion_close(reader);
  orion_close(sensor);
  orion_close(NULL);
  puts("ok");
  return 0;
}
