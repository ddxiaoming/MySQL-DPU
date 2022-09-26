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
  static constexpr uint32_t DATA_PAGE_SIZE = 16 * 1024; // 16KB

  static constexpr space_no_t REDO_LOG_SPACE_ID = 0xFFFFFFF0UL;

  static const page_size_t DEFAULT_PAGE_SIZE(0, 0, false);

  // 第一个LOG_BLOCK_HDR_NO从18开始
  static constexpr uint32_t FIRST_LOG_BLOCK_HDR_NO = 18;

  // 有4个 redo log block 放的是metadata
  static constexpr uint32_t N_LOG_METADATA_BLOCKS = 4;

  static constexpr uint32_t MY_LOG_START_LSN = 8716;

  static constexpr uint32_t PER_LOG_FILE_SIZE = 48 * 1024 * 1204; // 48M

  static constexpr uint32_t N_BLOCKS_IN_A_PAGE = DATA_PAGE_SIZE / LOG_BLOCK_SIZE; // 48M

  static constexpr uint32_t BUFFER_POOL_SIZE = 8 * 1024 * 1024; // buffer pool size in data_page_size
}
#endif
