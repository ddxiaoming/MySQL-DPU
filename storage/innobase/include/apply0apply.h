#ifndef apply0apply_h
#define apply0apply_h
#include "apply0config.h"
#include "mtr0types.h"
#include <unordered_map>
#include <list>
#include <fstream>

namespace MYSQL_DPU {

// 一条redo log
class LogEntry {
public:
  LogEntry(mlog_id_t type, space_no_t space_no,
           page_no_t page_no, lsn_t lsn,
           unsigned char *log_body_start_ptr, unsigned char *log_body_end_ptr) :
           type_(type), space_no_(space_no), page_no_(page_no), lsn_(lsn),
           log_body_start_ptr_(log_body_start_ptr), log_body_end_ptr_(log_body_end_ptr)
           {}
  mlog_id_t type_;
  space_no_t space_no_;
  page_no_t page_no_;
  lsn_t lsn_;
  unsigned char *log_body_start_ptr_;
  unsigned char *log_body_end_ptr_;
};

class ApplySystem {
public:
  ApplySystem();
  ~ApplySystem();
  lsn_t GetCheckpointLSN() const {
    return checkpoint_lsn_;
  }
  uint32_t GetCheckpointNo() const {
    return checkpoint_no_;
  }
  uint32_t GetCheckpointOffset() const {
    return checkpoint_offset_;
  }
  bool PopulateHashMap();
  bool ApplyHashLogs();
  bool ApplyOneLog(unsigned char *page, const LogEntry &log);
private:
  // 在恢复page时使用的哈希表
  std::unordered_map<space_no_t, std::unordered_map<page_no_t, std::list<LogEntry>>> hash_map_;

  // parse buffer size in bytes
  uint32_t parse_buf_size_;

  // 存放log block中掐头去尾后的redo日志，这些日志必须是完整的MTR
  unsigned char *parse_buf_;

  // parse buffer中有效日志的长度
  uint32_t parse_buf_content_size_;

  // meta buffer size in bytes
  uint32_t meta_data_buf_size_;

  // 存储redo log file的前4个字节
  unsigned char *meta_data_buf_;

  lsn_t checkpoint_lsn_;

  uint32_t checkpoint_no_;

  uint32_t checkpoint_offset_;

  // 下次取出log file中这个log block no代表的block到log_buf_中
  uint32_t next_fetch_page_id;

  // 产生的所有日志都已经apply完了
  bool finished_;

  // 下一条日志的LSN
  lsn_t next_lsn_;

  // 数据目录的path
  std::string data_path_;

  // space_id -> file name的映射表
  std::unordered_map<uint32_t, std::string> space_id_2_file_name_;
};
}
#endif