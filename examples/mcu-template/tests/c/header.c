/*
 * The header compiles, under each of its configurations (tests/header.rs), and its
 * declarations are what a firmware's main loop calls.
 */
#include <orion_mcu.h>

_Static_assert(ORION_MCU_API_VERSION == ((ORION_MCU_API_MAJOR << 16) | ORION_MCU_API_MINOR),
               "the packed version");

int firmware_main(uint32_t now_ms) {
    uint32_t version = orion_api_version();
    if ((version >> 16) != ORION_MCU_API_MAJOR || (version & 0xffff) < ORION_MCU_API_MINOR) {
        return -1;
    }
    if (!orion_init("c-board")) {
        return -1;
    }
    int32_t yaw = orion_topic("c-board.yaw", ORION_FIELD_FLOAT, "deg");
    int32_t zero = orion_action("zero", "Sets yaw to zero", ORION_ADMITS_LOCAL | ORION_ADMITS_OPERATOR);
#if defined(ORION_MCU_SLOTS)
    (void)orion_firmware("devices/c-board/firmware/");
#endif
    uint32_t events = orion_poll(now_ms);
    if (events & ORION_EVENT_RUN) {
        uint64_t call;
        uint8_t args[32];
        size_t len;
        if (orion_run(&call, args, sizeof args, &len) == zero) {
            (void)orion_finish(call, ORION_OUTCOME_SUCCEEDED, NULL, 0, NULL);
        }
    }
    if (orion_subscribers((uint32_t)yaw) > 0) {
        (void)orion_publish((uint32_t)yaw, (uint64_t)now_ms * 1000000u, 1.5);
    }
#if defined(ORION_MCU_CAN)
    uint8_t frame[64];
    size_t n = orion_can_tx(frame);
    orion_can_rx(frame, n);
#else
    uint8_t out[64];
    size_t n = orion_tx(out, sizeof out);
    orion_rx(out, n);
#endif
#if defined(ORION_MCU_SLOTS)
    if (events & ORION_EVENT_FIRMWARE) {
        (void)orion_boot(false);
    }
#endif
    return (int)orion_state() + (int)orion_interface_revision();
}
