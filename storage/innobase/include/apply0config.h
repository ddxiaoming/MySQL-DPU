#ifndef apply0config_h
#define apply0config_h
#include <cinttypes>
#include "page0size.h"

namespace MYSQL_DPU {
  using page_no_t = uint64_t;
  using space_no_t = uint64_t;
  using lsn_t = uint64_t;

  // log block size in bytes
  static constexpr uint32_t LOG_BLOCK_SIZE = 512;

  // data page size in bytes
  static constexpr uint32_t DATA_PAGE_SIZE = 16 * 1024 * 1024;

  static constexpr space_no_t REDO_LOG_SPACE_ID = 0xFFFFFFF0UL;

  static const page_size_t DEFAULT_PAGE_SIZE(0, 0, false);
}
#endif
