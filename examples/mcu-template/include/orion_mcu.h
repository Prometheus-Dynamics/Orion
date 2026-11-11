/*
 * The MCU template's C API (examples/mcu-template/README.md).
 *
 * Link liborion_mcu_template.a and call from one main loop, never from an interrupt. Define
 * ORION_MCU_UART (the default library, feature ffi-uart) or ORION_MCU_CAN (feature ffi-can)
 * to match the library, and ORION_MCU_SLOTS as well for feature ffi-slots.
 *
 * At start-up, check (orion_api_version() >> 16) == ORION_MCU_API_MAJOR and
 * (orion_api_version() & 0xffff) >= ORION_MCU_API_MINOR.
 *
 * Every string the tables are given (names, units, descriptions, the prefix) must stay as
 * it is for as long as the firmware runs: a string literal. Nothing is copied.
 *
 * tests/header.rs checks every value here against the library.
 */
#ifndef ORION_MCU_H
#define ORION_MCU_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Versions: the major moves for a breaking change, the minor for an addition. */
#define ORION_MCU_API_MAJOR 1u
#define ORION_MCU_API_MINOR 0u
#define ORION_MCU_API_VERSION ((ORION_MCU_API_MAJOR << 16) | ORION_MCU_API_MINOR)

/* The most topics and actions a device has. */
#define ORION_MAX_TOPICS 8
#define ORION_MAX_ACTIONS 8

/* What orion_poll returns: what happened since the last call. */
#define ORION_EVENT_CONNECTED (1u << 0)    /* the host took the device */
#define ORION_EVENT_DISCONNECTED (1u << 1) /* the session ended; hello again */
#define ORION_EVENT_REJECTED (1u << 2)     /* the host said no; hello again later */
#define ORION_EVENT_RUN (1u << 3)          /* a run waits: orion_run */
#define ORION_EVENT_CANCEL (1u << 4)       /* a run is to stop: orion_cancelled */
#define ORION_EVENT_DEMAND (1u << 5)       /* listeners changed: orion_subscribers */
#define ORION_EVENT_FIRMWARE (1u << 6)     /* new firmware on trial: reset when ready */

/* A topic's field. */
#define ORION_FIELD_BOOL 1
#define ORION_FIELD_INT 2
#define ORION_FIELD_FLOAT 3

/* Who may run an action. */
#define ORION_ADMITS_LOCAL (1u << 0)    /* programs on the board */
#define ORION_ADMITS_OPERATOR (1u << 1) /* operators, once granted */
#define ORION_ADMITS_PEER (1u << 2)     /* other nodes */

/* How a run ended. */
#define ORION_OUTCOME_SUCCEEDED 1
#define ORION_OUTCOME_FAILED 2
#define ORION_OUTCOME_REJECTED 3

/* Errors. */
#define ORION_ERR_STATE (-1)  /* not now: before orion_init, a table after orion_poll */
#define ORION_ERR_ARG (-2)    /* a string or value that is not one */
#define ORION_ERR_FULL (-3)   /* the table is full */
#define ORION_ERR_CLOSED (-4) /* no such topic, or the host has not taken it */
#define ORION_ERR_BUSY (-5)   /* the link is sending something else: again after orion_tx */
#define ORION_ERR_LINK (-6)   /* no session, or too long for the link */

/* Starts over as the device `name`, with no topics or actions. */
bool orion_init(const char *name);

/* Adds a topic of one field (ORION_FIELD_*); unit may be NULL. Its number, or ORION_ERR_*. */
int32_t orion_topic(const char *name, uint8_t type, const char *unit);

/* Adds an action on the device, run by whoever `admits` names (ORION_ADMITS_*). */
int32_t orion_action(const char *name, const char *description, uint8_t admits);

/* Moves the session on to `now_ms` (may wrap). Returns ORION_EVENT_* bits. The first call
 * fixes the tables. */
uint32_t orion_poll(uint32_t now_ms);

/* 0 saying hello, 1 turned away for now, 2 in a session. */
uint32_t orion_state(void);

#if defined(ORION_MCU_CAN)
/* The data of one CAN frame sent to this device. */
void orion_can_rx(const uint8_t *data, size_t len);
/* The data of the next CAN frame to send, into out[64]; its length, 0 for none. */
size_t orion_can_tx(uint8_t *out);
#else
/* Bytes that arrived on the UART. */
void orion_rx(const uint8_t *data, size_t len);
/* Up to `cap` bytes to write to the UART; how many. Call until it returns 0. */
size_t orion_tx(uint8_t *out, size_t cap);
#endif

/* Publishes a sample at `time_ns`. 0, or ORION_ERR_*; a sample that does not go is dropped,
 * so the next is the newest. */
int32_t orion_publish(uint32_t topic, uint64_t time_ns, double value);
int32_t orion_publish_int(uint32_t topic, uint64_t time_ns, int64_t value);
int32_t orion_publish_bool(uint32_t topic, uint64_t time_ns, bool value);

/* How many listen to `topic`. */
uint32_t orion_subscribers(uint32_t topic);

/* Takes the run that waits: its number, up to `cap` bytes of arguments and their whole
 * length. Returns the action's number, or -1 if none waits. */
int32_t orion_run(uint64_t *call, uint8_t *args, size_t cap, size_t *len);

/* The run the host asked to stop, or 0. */
uint64_t orion_cancelled(void);

/* How run `call` is doing: percent done (255 unknown) and what it is doing. */
int32_t orion_progress(uint64_t call, uint8_t percent, const char *message);

/* Ends run `call` (ORION_OUTCOME_*), with its output and why if it did not succeed. On
 * ORION_ERR_BUSY nothing was sent: call again. */
int32_t orion_finish(uint64_t call, uint8_t outcome, const uint8_t *output, size_t len,
                     const char *detail);

/* This library's ORION_MCU_API_VERSION. */
uint32_t orion_api_version(void);

/* The revision of the wire the device says it speaks. */
uint16_t orion_interface_revision(void);

#if defined(ORION_MCU_SLOTS)
/* New firmware for files under `prefix`, such as "devices/imu-board/firmware/". Before the
 * first orion_poll. 0 or ORION_ERR_*. */
int32_t orion_firmware(const char *prefix);

/* The firmware on trial works: keep it. Until then a reset goes back. */
int32_t orion_firmware_confirm(void);

/* For the bootloader after every reset: the slot to start, or 255 for none. `fault` after a
 * watchdog or fault reset. */
uint8_t orion_boot(bool fault);

/* The port defines these over its flash. Slots are 0 and 1; a record copy is 96 bytes. True
 * if it worked. */
uint32_t orion_port_slot_capacity(void);
bool orion_port_erase_slot(uint8_t slot);
bool orion_port_write_slot(uint8_t slot, uint32_t offset, const uint8_t *data, size_t len);
bool orion_port_read_slot(uint8_t slot, uint32_t offset, uint8_t *out, size_t len);
bool orion_port_read_record(uint8_t copy, uint8_t *out);
bool orion_port_write_record(uint8_t copy, const uint8_t *data);
#endif

#ifdef __cplusplus
}
#endif

#endif /* ORION_MCU_H */
