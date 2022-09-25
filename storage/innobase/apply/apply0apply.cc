#include "apply0apply.h"
#include "fil0fil.h"
#include "buf0buf.h"
#include "log0recv.h"
#include "mtr0log.h"
#include "fsp0fsp.h"
#include "page0page.h"
#include "btr0cur.h"
#include "sync0sync.h"
#include "trx0rec.h"
#include "trx0undo.h"
#include "ibuf0ibuf.h"
#include "apply0utility.h"
#include <cstring>
#include <fstream>

namespace MYSQL_DPU {

ApplySystem::ApplySystem() :
hash_map_(),
parse_buf_size_(2 * 1024 * 1024), // 2M
parse_buf_(new unsigned char[parse_buf_size_]),
parse_buf_content_size_(0),
meta_data_buf_size_(LOG_BLOCK_SIZE * N_LOG_METADATA_BLOCKS), // 4 blocks
meta_data_buf_(new unsigned char[meta_data_buf_size_]),
checkpoint_lsn_(0),
checkpoint_no_(0),
checkpoint_offset_(0),
next_fetch_page_id(0),
finished_(false),
next_lsn_(MY_LOG_START_LSN),
data_path_("/home/lemon/mysql/data/"),
space_id_2_file_name_()
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

  // 构建映射表
  std::vector<std::string> filenames;
  TravelDirectory(data_path_, ".ibd", filenames);
  std::ifstream ifs;
  unsigned char page_buf[DATA_PAGE_SIZE];
  for (auto & filename : filenames) {
    ifs.open(filename, std::ios::in | std::ios::binary);
    ifs.read(reinterpret_cast<char *>(page_buf), DATA_PAGE_SIZE);
    uint32_t space_id = mach_read_from_4(page_buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
    space_id_2_file_name_.insert({space_id, filename});
    ifs.close();
  }
//  for (const auto &item: space_id_2_file_name_) {
//    std::cerr << "space_id = " << item.first << ", filename = " << item.second << std::endl;
//  }
}

ApplySystem::~ApplySystem() {
  delete[] parse_buf_;
  parse_buf_ = nullptr;
  delete[] meta_data_buf_;
  meta_data_buf_ = nullptr;
}

bool ApplySystem::PopulateHashMap() {
  // 当前所有日志都处理完毕了
  if (finished_) return false;

  hash_map_.clear();
  unsigned char buf[DATA_PAGE_SIZE];
  // 1.填充parse buffer
  for (uint32_t i = 0; i < parse_buf_size_ / DATA_PAGE_SIZE; ++i) {
    page_id_t page_id(REDO_LOG_SPACE_ID, i);
    fil_io(IORequestLogRead, true,
           page_id, DEFAULT_PAGE_SIZE,
           0, DATA_PAGE_SIZE,
           static_cast<void *>(buf), NULL);
    for (uint32_t j = 0; j < DATA_PAGE_SIZE / LOG_BLOCK_SIZE; ++j) {
      if (i == 0 && j < 4) continue; // 跳过前面4个block
      auto hdr_no = ~LOG_BLOCK_FLUSH_BIT_MASK & mach_read_from_4(buf + j * LOG_BLOCK_SIZE);
      auto data_len = mach_read_from_2(buf + j * LOG_BLOCK_SIZE + LOG_BLOCK_HDR_DATA_LEN);
      auto first_rec = mach_read_from_2(buf + j * LOG_BLOCK_SIZE + LOG_BLOCK_FIRST_REC_GROUP);
      auto checkpoint_no = mach_read_from_4(buf + j * LOG_BLOCK_SIZE + LOG_BLOCK_CHECKPOINT_NO);
      auto checksum = mach_read_from_4(buf + j * LOG_BLOCK_SIZE + LOG_BLOCK_CHECKSUM);
      // 每个block的日志掐头去尾放到parse buffer中
      uint32_t len = data_len - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE;
//      std::cerr << "hdr_no = " << hdr_no << ", data_len = " << data_len << ", "
//                << "first_rec = " << first_rec << ", checkpoint_no = " << checkpoint_no << ", "
//                << "checksum = " << checksum << std::endl;

      std::memcpy(parse_buf_ + parse_buf_content_size_,
                  buf + j * LOG_BLOCK_SIZE + LOG_BLOCK_HDR_SIZE, len);
      parse_buf_content_size_ += len;
      if (data_len != 512) {
        finished_ = true;
        break;
      }
    }
  }

  bool single_rec = false;


  // 2.从parse buffer中循环解析日志，放到哈希表中
  uint32_t parsed_len = 0; // 已经解析出来的日志的总长度
  unsigned char *end_ptr = parse_buf_ + parse_buf_content_size_;
loop:
  unsigned char *start_ptr = parse_buf_ + parsed_len;
  if (start_ptr == end_ptr) return true;
  ulint len = 0, space_id, page_id;
  mlog_id_t	type;
  byte *log_body_ptr = nullptr;
  len = recv_parse_log_rec(&type, start_ptr, end_ptr, &space_id,
                           &page_id, false, &log_body_ptr);

  parsed_len += len;
  if (len != 0 && log_body_ptr != nullptr) {
    // 有些类型的log跳过，不是对page所做的修改，不能加入哈希表
    switch (type) {
      case MLOG_MULTI_REC_END:
      case MLOG_DUMMY_RECORD:
      case MLOG_FILE_DELETE:
      case MLOG_FILE_CREATE2:
      case MLOG_FILE_RENAME2:
      case MLOG_FILE_NAME:
      case MLOG_CHECKPOINT:
        goto loop;
    }
    // 加入哈希表
    hash_map_[space_id][page_id].emplace_back(type,space_id,page_id,
                                              next_lsn_,log_body_ptr,
                                              start_ptr + len);

  }
  assert(parsed_len <= parse_buf_size_);
  if (len != 0) {
    goto loop;
  }

  return true;
}

bool ApplySystem::ApplyHashLogs() {
  if (hash_map_.empty()) return false;
  for (const auto &spaces_logs: hash_map_) {

    for (const auto &pages_logs: spaces_logs.second) {

      // 获取需要的page
      unsigned char page_buf[DATA_PAGE_SIZE];
      memset(page_buf, 0, DATA_PAGE_SIZE);
      std::ifstream ifs;
      ifs.open(space_id_2_file_name_[spaces_logs.first], std::ios::in | std::ios::binary);
      ifs.seekg(pages_logs.first * DATA_PAGE_SIZE);
      ifs.read(reinterpret_cast<char *>(page_buf), DATA_PAGE_SIZE);
      for (const auto &log: pages_logs.second) {
        ApplyOneLog(page_buf, log);
//        std::cerr << "applied" <<
      }
//      buf_block_t buf_block;
    }
  }
}

bool ApplySystem::ApplyOneLog(unsigned char *page, const LogEntry &log) {
  auto *block = static_cast<buf_block_t *>(malloc(sizeof(buf_block_t)));
  mtr_t mtr;
  mtr.set_log_mode(MTR_LOG_NONE);
  InitBufBlock(block, page);
  mlog_id_t type = log.type_;
  std::cout << "applying " << GetLogString(type);
  uint32_t space_id = log.space_no_;
  dict_index_t*	index	= NULL;
  page_zip_des_t*	page_zip;
  ulint		page_type;
  byte*	ptr = log.log_body_start_ptr_;
  byte*	end_ptr = log.log_body_end_ptr_;
  byte*	old_ptr = log.log_body_start_ptr_;
  uint32_t page_no = log.page_no_;
  switch (type) {
#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN:
		/* The LSN is checked in recv_parse_log_rec(). */
		break;
#endif /* UNIV_LOG_LSN_DEBUG */
    case MLOG_1BYTE: case MLOG_2BYTES: case MLOG_4BYTES: case MLOG_8BYTES:
      ptr = mlog_parse_nbytes(type, ptr, end_ptr, page, page_zip);
      if (ptr != NULL && page != NULL
          && page_no == 0 && type == MLOG_4BYTES) {
        ulint	offs = mach_read_from_2(old_ptr);
        switch (offs) {
          fil_space_t*	space;
          ulint		val;
          default:
            break;
          case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
          case FSP_HEADER_OFFSET + FSP_SIZE:
          case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
          case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
            space = fil_space_get(space_id);
            ut_a(space != nullptr);
            val = mach_read_from_4(page + offs);

            switch (offs) {
              case FSP_HEADER_OFFSET + FSP_SPACE_FLAGS:
                space->flags = val;
                break;
              case FSP_HEADER_OFFSET + FSP_SIZE:
                space->size_in_header = val;
                break;
              case FSP_HEADER_OFFSET + FSP_FREE_LIMIT:
                space->free_limit = val;
                break;
              case FSP_HEADER_OFFSET + FSP_FREE + FLST_LEN:
                space->free_len = val;
                ut_ad(val == flst_get_len(
                    page + offs));
                break;
            }
        }
      }
      break;
    case MLOG_REC_INSERT: case MLOG_COMP_REC_INSERT:
      ut_ad(!page || fil_page_type_is_index(page_type));

      if (NULL != (ptr = mlog_parse_index(
          ptr, end_ptr,
          type == MLOG_COMP_REC_INSERT,
          &index))) {
        ut_a(!page
             || (ibool)!!page_is_comp(page)
                       == dict_table_is_comp(index->table));
//        std::cout << "We have no ability to apply this log now! Skipped!" << std::endl;
        ptr = page_cur_parse_insert_rec(FALSE, ptr, end_ptr,
                                        block, index, &mtr);
      }
      break;
    case MLOG_REC_CLUST_DELETE_MARK: case MLOG_COMP_REC_CLUST_DELETE_MARK:
      ut_ad(!page || fil_page_type_is_index(page_type));

      if (NULL != (ptr = mlog_parse_index(
          ptr, end_ptr,
          type == MLOG_COMP_REC_CLUST_DELETE_MARK,
          &index))) {
        ut_a(!page
             || (ibool)!!page_is_comp(page)
                       == dict_table_is_comp(index->table));
        ptr = btr_cur_parse_del_mark_set_clust_rec(
            ptr, end_ptr, page, page_zip, index);
      }
      break;
    case MLOG_COMP_REC_SEC_DELETE_MARK:
      ut_ad(!page || fil_page_type_is_index(page_type));
      /* This log record type is obsolete, but we process it for
      backward compatibility with MySQL 5.0.3 and 5.0.4. */
      ut_a(!page || page_is_comp(page));
      ut_a(!page_zip);
      ptr = mlog_parse_index(ptr, end_ptr, TRUE, &index);
      if (!ptr) {
        break;
      }
      /* Fall through */
    case MLOG_REC_SEC_DELETE_MARK:
      ut_ad(!page || fil_page_type_is_index(page_type));
      ptr = btr_cur_parse_del_mark_set_sec_rec(ptr, end_ptr,
                                               page, page_zip);
      break;
    case MLOG_REC_UPDATE_IN_PLACE: case MLOG_COMP_REC_UPDATE_IN_PLACE:
      ut_ad(!page || fil_page_type_is_index(page_type));

      if (NULL != (ptr = mlog_parse_index(
          ptr, end_ptr,
          type == MLOG_COMP_REC_UPDATE_IN_PLACE,
          &index))) {
        ut_a(!page
             || (ibool)!!page_is_comp(page)
                       == dict_table_is_comp(index->table));
        ptr = btr_cur_parse_update_in_place(ptr, end_ptr, page,
                                            page_zip, index);
      }
      break;
    case MLOG_LIST_END_DELETE: case MLOG_COMP_LIST_END_DELETE:
    case MLOG_LIST_START_DELETE: case MLOG_COMP_LIST_START_DELETE:
      ut_ad(!page || fil_page_type_is_index(page_type));

      if (NULL != (ptr = mlog_parse_index(
          ptr, end_ptr,
          type == MLOG_COMP_LIST_END_DELETE
          || type == MLOG_COMP_LIST_START_DELETE,
          &index))) {
        ut_a(!page
             || (ibool)!!page_is_comp(page)
                       == dict_table_is_comp(index->table));
//        std::cout << "We have no ability to apply this log now! Skipped!" << std::endl;
        ptr = page_parse_delete_rec_list(type, ptr, end_ptr,
                                         block, index, &mtr);
      }
      break;
    case MLOG_LIST_END_COPY_CREATED: case MLOG_COMP_LIST_END_COPY_CREATED:
      ut_ad(!page || fil_page_type_is_index(page_type));

      if (NULL != (ptr = mlog_parse_index(
          ptr, end_ptr,
          type == MLOG_COMP_LIST_END_COPY_CREATED,
          &index))) {
        ut_a(!page
             || (ibool)!!page_is_comp(page)
                       == dict_table_is_comp(index->table));
        std::cout << "We have no ability to apply this log now! Skipped!" << std::endl;
//        ptr = page_parse_copy_rec_list_to_created_page(
//            ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_PAGE_REORGANIZE:
    case MLOG_COMP_PAGE_REORGANIZE:
    case MLOG_ZIP_PAGE_REORGANIZE:
      ut_ad(!page || fil_page_type_is_index(page_type));

      if (NULL != (ptr = mlog_parse_index(
          ptr, end_ptr,
          type != MLOG_PAGE_REORGANIZE,
          &index))) {
        ut_a(!page
             || (ibool)!!page_is_comp(page)
                       == dict_table_is_comp(index->table));
        ptr = btr_parse_page_reorganize(
            ptr, end_ptr, index,
            type == MLOG_ZIP_PAGE_REORGANIZE,
            block, &mtr);
      }
      break;
    case MLOG_PAGE_CREATE: case MLOG_COMP_PAGE_CREATE:
      /* Allow anything in page_type when creating a page. */
      ut_a(!page_zip);
      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE, false);
      break;
    case MLOG_PAGE_CREATE_RTREE: case MLOG_COMP_PAGE_CREATE_RTREE:
      page_parse_create(block, type == MLOG_COMP_PAGE_CREATE_RTREE,
                        true);
      break;
    case MLOG_UNDO_INSERT:
      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
      ptr = trx_undo_parse_add_undo_rec(ptr, end_ptr, page);
      break;
    case MLOG_UNDO_ERASE_END:
      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
      ptr = trx_undo_parse_erase_page_end(ptr, end_ptr, page, &mtr);
      break;
    case MLOG_UNDO_INIT:
      /* Allow anything in page_type when creating a page. */
      ptr = trx_undo_parse_page_init(ptr, end_ptr, page, &mtr);
      break;
    case MLOG_UNDO_HDR_DISCARD:
      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
      ptr = trx_undo_parse_discard_latest(ptr, end_ptr, page, &mtr);
      break;
    case MLOG_UNDO_HDR_CREATE:
    case MLOG_UNDO_HDR_REUSE:
      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
      ptr = trx_undo_parse_page_header(type, ptr, end_ptr,
                                       page, &mtr);
      break;
    case MLOG_REC_MIN_MARK: case MLOG_COMP_REC_MIN_MARK:
      ut_ad(!page || fil_page_type_is_index(page_type));
      /* On a compressed page, MLOG_COMP_REC_MIN_MARK
      will be followed by MLOG_COMP_REC_DELETE
      or MLOG_ZIP_WRITE_HEADER(FIL_PAGE_PREV, FIL_NULL)
      in the same mini-transaction. */
      ut_a(type == MLOG_COMP_REC_MIN_MARK || !page_zip);
      ptr = btr_parse_set_min_rec_mark(
          ptr, end_ptr, type == MLOG_COMP_REC_MIN_MARK,
          page, &mtr);
      break;
    case MLOG_REC_DELETE: case MLOG_COMP_REC_DELETE:
      ut_ad(!page || fil_page_type_is_index(page_type));

      if (NULL != (ptr = mlog_parse_index(
          ptr, end_ptr,
          type == MLOG_COMP_REC_DELETE,
          &index))) {
        ut_a(!page
             || (ibool)!!page_is_comp(page)
                       == dict_table_is_comp(index->table));
        ptr = page_cur_parse_delete_rec(ptr, end_ptr,
                                        block, index, &mtr);
      }
      break;
    case MLOG_IBUF_BITMAP_INIT:
      /* Allow anything in page_type when creating a page. */
      ptr = ibuf_parse_bitmap_init(ptr, end_ptr, block, &mtr);
      break;
    case MLOG_INIT_FILE_PAGE:
    case MLOG_INIT_FILE_PAGE2:
      /* Allow anything in page_type when creating a page. */
      ptr = fsp_parse_init_file_page(ptr, end_ptr, block);
      break;
    case MLOG_WRITE_STRING:
      ut_ad(!page || page_type != FIL_PAGE_TYPE_ALLOCATED
            || page_no == 0);
      ptr = mlog_parse_string(ptr, end_ptr, page, page_zip);
      break;
    case MLOG_ZIP_WRITE_NODE_PTR:
      ut_ad(!page || fil_page_type_is_index(page_type));
      ptr = page_zip_parse_write_node_ptr(ptr, end_ptr,
                                          page, page_zip);
      break;
    case MLOG_ZIP_WRITE_BLOB_PTR:
      ut_ad(!page || fil_page_type_is_index(page_type));
      ptr = page_zip_parse_write_blob_ptr(ptr, end_ptr,
                                          page, page_zip);
      break;
    case MLOG_ZIP_WRITE_HEADER:
      ut_ad(!page || fil_page_type_is_index(page_type));
      ptr = page_zip_parse_write_header(ptr, end_ptr,
                                        page, page_zip);
      break;
    case MLOG_ZIP_PAGE_COMPRESS:
      /* Allow anything in page_type when creating a page. */
      ptr = page_zip_parse_compress(ptr, end_ptr,
                                    page, page_zip);
      break;
    case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
      if (NULL != (ptr = mlog_parse_index(
          ptr, end_ptr, TRUE, &index))) {

        ut_a(!page || ((ibool)!!page_is_comp(page)
                              == dict_table_is_comp(index->table)));
        ptr = page_zip_parse_compress_no_data(
            ptr, end_ptr, page, page_zip, index);
      }
      break;
    default:
      ptr = NULL;
      recv_sys->found_corrupt_log = true;
  }

  if (index) {
    dict_table_t*	table = index->table;

    dict_mem_index_free(index);
    dict_mem_table_free(table);
  }
}

}
