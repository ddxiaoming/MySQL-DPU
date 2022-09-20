#include "apply0apply.h"
#include "fil0fil.h"
#include "buf0buf.h"
namespace MYSQL_DPU {
ApplySystem::ApplySystem() :
hash_map_(),
parse_buf_size_(2 * 1024 * 1024), // 2M
parse_buf_(new unsigned char(parse_buf_size_)),
parse_buf_free_ptr(parse_buf_),
meta_data_buf_size_(LOG_BLOCK_SIZE * 4), // 4 blocks
meta_data_buf_(new unsigned char(meta_data_buf_size_)),
checkpoint_lsn_(0),
checkpoint_no_(0),
checkpoint_offset_(0),
log_buf_size_(2 * 1024 * 1024), // 2M
log_buf_(new unsigned char(log_buf_size_))
{
  // 填充meta_data_buf
  page_id_t metadata_page_id(REDO_LOG_SPACE_ID, 0);
  fil_io(IORequestLogRead, true,
         metadata_page_id, DEFAULT_PAGE_SIZE,
         0, meta_data_buf_size_, static_cast<void *>(meta_data_buf_), NULL);
  // 设置checkpoint_lsn和checkpoint_no
  uint32_t checkpoint_no_1 = mach_read_from_8(meta_data_buf_ + 1 * LOG_BLOCK_SIZE + LOG_CHECKPOINT_NO);
  uint32_t checkpoint_no_2 = mach_read_from_8(meta_data_buf_ + 3 * LOG_BLOCK_SIZE + LOG_CHECKPOINT_NO);
  if (checkpoint_no_1 > checkpoint_no_2) {
    checkpoint_no_ = checkpoint_no_1;
    checkpoint_lsn_ = mach_read_from_8(meta_data_buf_ + 1 * LOG_BLOCK_SIZE + LOG_CHECKPOINT_LSN);
    checkpoint_offset_ = mach_read_from_8(meta_data_buf_ + 1 * LOG_BLOCK_SIZE + LOG_CHECKPOINT_OFFSET);
  } else {
    checkpoint_no_ = checkpoint_no_2;
    checkpoint_lsn_ = mach_read_from_8(meta_data_buf_ + 3 * LOG_BLOCK_SIZE + LOG_CHECKPOINT_LSN);
    checkpoint_offset_ = mach_read_from_8(meta_data_buf_ + 3 * LOG_BLOCK_SIZE + LOG_CHECKPOINT_OFFSET);
  }
}

ApplySystem::~ApplySystem() {
  delete parse_buf_;
  parse_buf_ = nullptr;
  delete meta_data_buf_;
  meta_data_buf_ = nullptr;
  delete log_buf_;
  log_buf_ = nullptr;
}

bool ApplySystem::PopulateHashMap() {
  page_id_t metadata_page_id(REDO_LOG_SPACE_ID, 0);
  fil_io(IORequestLogRead, true,
         metadata_page_id, DEFAULT_PAGE_SIZE,
         2048, DATA_PAGE_SIZE - 2048, static_cast<void *>(log_buf_), NULL);
  return true;
}
}
