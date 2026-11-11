/* STM32G431x6: 32 KiB flash, 16 KiB SRAM1 (the smallest G431; larger parts work unchanged). */
MEMORY
{
  FLASH : ORIGIN = 0x08000000, LENGTH = 32K
  RAM   : ORIGIN = 0x20000000, LENGTH = 16K
}
