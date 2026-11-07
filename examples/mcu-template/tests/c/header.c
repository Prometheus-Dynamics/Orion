/*
 * The MCU template's header compiles in each build (scripts/capi.sh check, with -fsyntax-only
 * and one of -DORION_MCU_UART, -DORION_MCU_CAN, or -DORION_MCU_UART -DORION_MCU_XFER
 * -DORION_MCU_SLOTS), and its declarations are what a firmware main loop calls.
 */
#include <orion_mcu.h>

_Static_assert(ORION_MCU_API_VERSION == ((ORION_MCU_API_MAJOR << 16) | ORION_MCU_API_MINOR),
               "the packed version");

int firmware_main(uint64_t now_ms) {
    uint32_t version = orion_api_version();
    if ((version >> 16) != ORION_MCU_API_MAJOR || (version & 0xffff) < ORION_MCU_API_MINOR) {
        return -1;
    }
    static const uint8_t name[] = "c-board";
    if (!orion_init(name, sizeof name - 1)) {
        return -1;
    }
    uint32_t events = orion_poll(now_ms);
    if (events & ORION_EVENT_CONNECTED) {
        (void)orion_session_revision();
    }
#if defined(ORION_MCU_CAN)
    uint8_t frame[64];
    (void)orion_can_tx(frame);
    orion_can_rx(frame, 8);
#else
    uint8_t out[64];
    (void)orion_tx(out, sizeof out);
    orion_rx(out, 0);
#endif
#if defined(ORION_MCU_SLOTS)
    (void)orion_boot(false);
#endif
    return (int)orion_lease_count() + (int)orion_link_revision();
}
