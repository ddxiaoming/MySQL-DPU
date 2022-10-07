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
           page_no_t page_no, lsn_t lsn, uint32_t log_body_len,
           unsigned char *log_body_start_ptr, unsigned char *log_body_end_ptr) :
           type_(type), space_no_(space_no), page_no_(page_no), lsn_(lsn), log_body_len_(log_body_len),
           log_body_start_ptr_(log_body_start_ptr), log_body_end_ptr_(log_body_end_ptr)
           {}
  mlog_id_t type_;
  space_no_t space_no_;
  page_no_t page_no_;
  lsn_t lsn_;
  uint32_t log_body_len_;
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

  uint32_t log_file_size_;

  // 下次取出log file中这个log block no代表的block到log_buf_中
  uint32_t next_fetch_page_id_;
  // 如果这个值不为-1，说明某一次解析日志时，page内有剩余没有解析完成的日志，下次需要把这个page解析完成
  int next_fetch_block_;

  uint32_t log_max_page_id_;
  // 产生的所有日志都已经apply完了
  bool finished_;

  // 下一条日志的LSN
  lsn_t next_lsn_;

  // redo log file path
  std::string log_file_path_;

  // 用来读取log文件的流
  std::ifstream log_stream_;

  std::ofstream ofs{"/home/lemon/redolog3.txt"};
};
}
#endif