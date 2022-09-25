#ifndef apply0utility_h
#define apply0utility_h
#include <vector>
#include <string>
#include "buf0buf.h"

namespace MYSQL_DPU {
bool TravelDirectory(const std::string &dir_path, const std::string &suffix, std::vector<std::string> &files);

void InitBufBlock(buf_block_t *block, unsigned char *page_frame);

const char* GetLogString(mlog_id_t type);
}

#endif
