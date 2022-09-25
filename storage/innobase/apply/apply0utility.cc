#include "apply0utility.h"
#include <dirent.h>
#include <iostream>
#include "fil0fil.h"
#include "buf0buf.h"
#include "btr0cur.h"
#include "sync0sync.h"
#include "trx0rec.h"
#include "trx0undo.h"
namespace MYSQL_DPU {
/**
 * 递归遍历文件夹下以suffix结尾的文件
 * @param dir_path 文件夹路径，不要以/结尾
 * @param suffix 文件结尾
 * @return path目录下以suffix结尾的所有文件名
 */
bool TravelDirectory(const std::string &dir_path, const std::string &suffix, std::vector<std::string> &files) {
  DIR *d;
  struct dirent *file;
  if(!(d = opendir(dir_path.data()))) {
    std::cout << "open directory " << dir_path << " failed!!!" << std::endl;
    closedir(d);
    return false;
  }

  while((file = readdir(d)) != nullptr) {
    std::string file_name(file->d_name);
    // 跳过当前目录（.）和上一级目录（..）
    if(file_name.empty() || file_name == "." || file_name == "..") {
      continue;
    }
    // 如果是普通的文件
    if(file->d_type == 8) {
      // 跳过不是以suffix结尾的文件
      if (suffix.size() > file_name.size()
          || file_name.substr(file_name.size() - suffix.size()) != suffix) {
        continue;
      }
      std::string path(dir_path);
      path.append("/").append(file_name);
      files.push_back(path);
    }
    //如果是文件夹
    if(file->d_type == 4) {
      std::string sub_dir_path;
      sub_dir_path.append(dir_path).append("/").append(file_name);
      TravelDirectory(sub_dir_path, suffix, files);
    }
  }
  closedir(d);
  return true;
}

void InitBufBlock(buf_block_t *block, unsigned char *page_frame) {
  block->frame = page_frame;
  block->page.flush_type = BUF_FLUSH_LRU;
  block->page.io_fix = BUF_IO_NONE;
  block->page.buf_fix_count = 0;
  block->page.freed_page_clock = 0;
  block->page.access_time = 0;
  block->page.newest_modification = 0;
  block->page.oldest_modification = 0;
  block->page.buf_pool_index = -1;
  block->page.state = BUF_BLOCK_NOT_USED;
  block->page.buf_fix_count = 0;
  block->page.io_fix = BUF_IO_NONE;
  block->page.flush_observer = nullptr;

  block->modify_clock = 0;
  block->index = nullptr;
  block->made_dirty_with_no_latch = false;
  block->skip_flush_check = false;

  mutex_create(LATCH_ID_BUF_BLOCK_MUTEX, &block->mutex);

  rw_lock_create(PFS_NOT_INSTRUMENTED, &block->lock, SYNC_LEVEL_VARYING);

  block->lock.is_block_lock = 1;
}


const char* GetLogString(mlog_id_t type) {
  switch (type) {
    case MLOG_SINGLE_REC_FLAG:
      return("MLOG_SINGLE_REC_FLAG");

    case MLOG_1BYTE:
      return("MLOG_1BYTE");

    case MLOG_2BYTES:
      return("MLOG_2BYTES");

    case MLOG_4BYTES:
      return("MLOG_4BYTES");

    case MLOG_8BYTES:
      return("MLOG_8BYTES");

    case MLOG_REC_INSERT:
      return("MLOG_REC_INSERT");

    case MLOG_REC_CLUST_DELETE_MARK:
      return("MLOG_REC_CLUST_DELETE_MARK");

    case MLOG_REC_SEC_DELETE_MARK:
      return("MLOG_REC_SEC_DELETE_MARK");

    case MLOG_REC_UPDATE_IN_PLACE:
      return("MLOG_REC_UPDATE_IN_PLACE");

    case MLOG_REC_DELETE:
      return("MLOG_REC_DELETE");

    case MLOG_LIST_END_DELETE:
      return("MLOG_LIST_END_DELETE");

    case MLOG_LIST_START_DELETE:
      return("MLOG_LIST_START_DELETE");

    case MLOG_LIST_END_COPY_CREATED:
      return("MLOG_LIST_END_COPY_CREATED");

    case MLOG_PAGE_REORGANIZE:
      return("MLOG_PAGE_REORGANIZE");

    case MLOG_PAGE_CREATE:
      return("MLOG_PAGE_CREATE");

    case MLOG_UNDO_INSERT:
      return("MLOG_UNDO_INSERT");

    case MLOG_UNDO_ERASE_END:
      return("MLOG_UNDO_ERASE_END");

    case MLOG_UNDO_INIT:
      return("MLOG_UNDO_INIT");

    case MLOG_UNDO_HDR_DISCARD:
      return("MLOG_UNDO_HDR_DISCARD");

    case MLOG_UNDO_HDR_REUSE:
      return("MLOG_UNDO_HDR_REUSE");

    case MLOG_UNDO_HDR_CREATE:
      return("MLOG_UNDO_HDR_CREATE");

    case MLOG_REC_MIN_MARK:
      return("MLOG_REC_MIN_MARK");

    case MLOG_IBUF_BITMAP_INIT:
      return("MLOG_IBUF_BITMAP_INIT");

    case MLOG_INIT_FILE_PAGE:
      return("MLOG_INIT_FILE_PAGE");

    case MLOG_WRITE_STRING:
      return("MLOG_WRITE_STRING");

    case MLOG_MULTI_REC_END:
      return("MLOG_MULTI_REC_END");

    case MLOG_DUMMY_RECORD:
      return("MLOG_DUMMY_RECORD");

    case MLOG_FILE_DELETE:
      return("MLOG_FILE_DELETE");

    case MLOG_COMP_REC_MIN_MARK:
      return("MLOG_COMP_REC_MIN_MARK");

    case MLOG_COMP_PAGE_CREATE:
      return("MLOG_COMP_PAGE_CREATE");

    case MLOG_COMP_REC_INSERT:
      return("MLOG_COMP_REC_INSERT");

    case MLOG_COMP_REC_CLUST_DELETE_MARK:
      return("MLOG_COMP_REC_CLUST_DELETE_MARK");

    case MLOG_COMP_REC_SEC_DELETE_MARK:
      return("MLOG_COMP_REC_SEC_DELETE_MARK");

    case MLOG_COMP_REC_UPDATE_IN_PLACE:
      return("MLOG_COMP_REC_UPDATE_IN_PLACE");

    case MLOG_COMP_REC_DELETE:
      return("MLOG_COMP_REC_DELETE");

    case MLOG_COMP_LIST_END_DELETE:
      return("MLOG_COMP_LIST_END_DELETE");

    case MLOG_COMP_LIST_START_DELETE:
      return("MLOG_COMP_LIST_START_DELETE");

    case MLOG_COMP_LIST_END_COPY_CREATED:
      return("MLOG_COMP_LIST_END_COPY_CREATED");

    case MLOG_COMP_PAGE_REORGANIZE:
      return("MLOG_COMP_PAGE_REORGANIZE");

    case MLOG_FILE_CREATE2:
      return("MLOG_FILE_CREATE2");

    case MLOG_ZIP_WRITE_NODE_PTR:
      return("MLOG_ZIP_WRITE_NODE_PTR");

    case MLOG_ZIP_WRITE_BLOB_PTR:
      return("MLOG_ZIP_WRITE_BLOB_PTR");

    case MLOG_ZIP_WRITE_HEADER:
      return("MLOG_ZIP_WRITE_HEADER");

    case MLOG_ZIP_PAGE_COMPRESS:
      return("MLOG_ZIP_PAGE_COMPRESS");

    case MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
      return("MLOG_ZIP_PAGE_COMPRESS_NO_DATA");

    case MLOG_ZIP_PAGE_REORGANIZE:
      return("MLOG_ZIP_PAGE_REORGANIZE");

    case MLOG_FILE_RENAME2:
      return("MLOG_FILE_RENAME2");

    case MLOG_FILE_NAME:
      return("MLOG_FILE_NAME");

    case MLOG_CHECKPOINT:
      return("MLOG_CHECKPOINT");

    case MLOG_PAGE_CREATE_RTREE:
      return("MLOG_PAGE_CREATE_RTREE");

    case MLOG_COMP_PAGE_CREATE_RTREE:
      return("MLOG_COMP_PAGE_CREATE_RTREE");

    case MLOG_INIT_FILE_PAGE2:
      return("MLOG_INIT_FILE_PAGE2");

    case MLOG_INDEX_LOAD:
      return("MLOG_INDEX_LOAD");

    case MLOG_TRUNCATE:
      return("MLOG_TRUNCATE");
  }
}
}
