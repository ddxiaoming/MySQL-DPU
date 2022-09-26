#include <fstream>
#include "apply0buffer.h"
#include "apply0config.h"
#include "apply0utility.h"
#include <random>
namespace MYSQL_DPU {
Page::Page() :
space_id_(),
page_id_(),
lsn_(),
data_(new unsigned char[DATA_PAGE_SIZE]),
state_(State::INVALID) {

}

Page::~Page() {
  if (data_ != nullptr) {
    delete[] data_;
    data_ = nullptr;
  }
}

BufferPool::BufferPool() :
lru_list_(),
hash_map_(),
buffer_(new Page[BUFFER_POOL_SIZE]),
data_path_("/home/lemon/mysql/data/"),
space_id_2_file_name_(),
free_list_(), frame_id_2_page_address_(BUFFER_POOL_SIZE) {
  // 1. 构建映射表
  std::vector<std::string> filenames;
  TravelDirectory(data_path_, ".ibd", filenames);
  std::ifstream ifs;
  unsigned char page_buf[DATA_PAGE_SIZE];
  for (auto & filename : filenames) {
    ifs.open(filename, std::ios::in | std::ios::binary);
    ifs.read(reinterpret_cast<char *>(page_buf), DATA_PAGE_SIZE);
    uint32_t space_id = mach_read_from_4(page_buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
    space_id_2_file_name_.insert({space_id, PageReader(filename)});
    ifs.close();
  }

  // 2. 初始化free_list_
  for (int i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); ++i) {
    free_list_.emplace_back(i);
  }
}


BufferPool::~BufferPool() {
  if (buffer_ != nullptr) {
    delete[] buffer_;
    buffer_ = nullptr;
  }
}

Page *BufferPool::NewPage(space_no_t space_id, page_no_t page_id) {
  if (hash_map_.find(space_id) != hash_map_.end()
  && hash_map_[space_id].find(page_id) != hash_map_[space_id].end()) {
    std::cerr << "the page(space_id = " << space_id
              << ", page_id = " << page_id << ") was already in buffer pool"
              << std::endl;
    return nullptr;
  }
  if (free_list_.empty()) {
    // buffer pool 空间不够
    Evict(10);
  }
  int frame_id = free_list_.front();
  free_list_.pop_front();
  buffer_[frame_id].SetState(Page::State::FROM_BUFFER);
  hash_map_[space_id][page_id] = frame_id;
  frame_id_2_page_address_[frame_id].valid_ = true;
  frame_id_2_page_address_[frame_id].space_id_ = space_id;
  frame_id_2_page_address_[frame_id].page_id_ = page_id;
  return &buffer_[frame_id];
}

void BufferPool::Evict(int n) {
  std::mt19937 mt(std::random_device{}());
  std::uniform_int_distribution<int> distribution(0, BUFFER_POOL_SIZE);
  int i = 0;
  while (i < n) {
    int frame_id = distribution(mt);
    if (buffer_[frame_id].GetState() == Page::State::INVALID) continue;
    free_list_.emplace_back(frame_id);
    PageAddress address = frame_id_2_page_address_[frame_id];
    if (hash_map_.find(address.space_id_) != hash_map_.end()
    && hash_map_[address.space_id_].find(address.page_id_) != hash_map_[address.space_id_].end()) {
      hash_map_[address.space_id_].erase(address.page_id_);
    }
    frame_id_2_page_address_[frame_id].valid_ = false;
    buffer_[frame_id].SetState(Page::State::INVALID);
    ++i;
  }
}

Page *BufferPool::GetPage(space_no_t space_id, page_no_t page_id) {
  if (hash_map_.find(space_id) != hash_map_.end()
      && hash_map_[space_id].find(page_id) != hash_map_[space_id].end()) {
    return &buffer_[hash_map_[space_id][page_id]];
  }

  // 不在buffer pool中，从磁盘读
  return ReadPageFromDisk(space_id, page_id);
}

Page *BufferPool::ReadPageFromDisk(space_no_t space_id, page_no_t page_id) {
  if (space_id_2_file_name_.find(space_id) == space_id_2_file_name_.end()) {
    std::cerr << "invalid space_id(" << space_id << std::endl;
    return nullptr;
  }
  if (free_list_.empty()) {
    // buffer pool 空间不够
    Evict(10);
  }
  // 从free list中分配一个frame
  int frame_id = free_list_.front();
  std::ifstream &ifs = space_id_2_file_name_[space_id].stream_;
  ifs.seekg(page_id * DATA_PAGE_SIZE);
  ifs.read(reinterpret_cast<char *>(buffer_[frame_id].GetData()), DATA_PAGE_SIZE);
  buffer_[frame_id].SetLSN();
  buffer_[frame_id].SetPageId();
  buffer_[frame_id].SetSpaceId();
  buffer_[frame_id].SetState(Page::State::FROM_DISK);
  free_list_.pop_front();
  frame_id_2_page_address_[frame_id].space_id_ = space_id;
  frame_id_2_page_address_[frame_id].page_id_ = page_id;
  frame_id_2_page_address_[frame_id].valid_ = true;
  hash_map_[space_id][page_id] = frame_id;
  return &buffer_[frame_id];
}

BufferPool buffer_pool;
}
