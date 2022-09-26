#ifndef apply0buffer_h
#define apply0buffer_h
#include <cstdint>
#include <unordered_map>
#include <list>
#include <utility>
#include "mach0data.h"
#include "fil0fil.h"
#include "apply0config.h"
namespace MYSQL_DPU {
class PageAddress {
public:
  bool valid_; // 该page是否有效
  space_no_t space_id_;
  page_no_t page_id_;
};
class PageReader {
public:
  PageReader() = default;
  explicit PageReader(std::string file_name) :
      file_name_(std::move(file_name)),
      stream_(file_name_, std::ios::in | std::ios::binary) {}
  std::string file_name_;
  std::ifstream stream_;
private:
};
class Page {
public:
  enum class State {
    INVALID = 0, // 刚被初始化，data_还未被填充，不可用
    FROM_BUFFER = 1, // 刚从buffer pool中被创建出来
    FROM_DISK = 2, // 从磁盘中读上来的
  };
  Page();
  ~Page();
  lsn_t GetLSN() const {
    return lsn_;
//    return mach_read_from_8(data_ + FIL_PAGE_LSN);
  }
  lsn_t GetSpaceId() const {
    return space_id_;
  }
  lsn_t GetPageId() const {
    return page_id_;
  }
  void SetLSN() {
    lsn_ = mach_read_from_8(data_ + FIL_PAGE_LSN);
  }
  void SetSpaceId() {
    space_id_ = mach_read_from_4(data_ + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
  }
  void SetPageId() {
    page_id_ = mach_read_from_4(data_ + FIL_PAGE_OFFSET);
  }
  unsigned char *GetData() const {
    return data_;
  }
  State GetState() {
    return state_;
  }
  void SetState(State state) {
    state_ = state;
  }
private:
  space_no_t space_id_;
  page_no_t page_id_;
  lsn_t lsn_;
  unsigned char *data_;
  State state_;
};

class BufferPool {
public:
  BufferPool();
  ~BufferPool();
  // 在buffer pool中新建一个page
  Page *NewPage(space_no_t space_id, page_no_t page_id);

  // 从buffer pool中获取一个page，不存在的话从磁盘获取
  Page *GetPage(space_no_t space_id, page_no_t page_id);
private:
  std::list<int> lru_list_;
  std::unordered_map<space_no_t, std::unordered_map<page_no_t, int>> hash_map_;
  Page *buffer_;
  // 数据目录的path
  std::string data_path_;
  // space_id -> file name的映射表
  std::unordered_map<uint32_t, PageReader> space_id_2_file_name_;

  // 指示buffer_中哪个下标是可以用的
  std::list<int> free_list_;

  std::vector<PageAddress> frame_id_2_page_address_;
  // 随机淘汰一些页面
  void Evict(int n);

  Page *ReadPageFromDisk(space_no_t space_id, page_no_t page_id);
};

extern BufferPool buffer_pool;
}


#endif
