/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

#include <memory>
#include <set>
#include <vector>

#include "execution/executor_utils.h"
#include "record/rm_scan.h"

namespace {

std::string table_name_from_log(const char *table_name, size_t table_name_size) {
    return std::string(table_name, table_name_size);
}

bool record_exists(RmFileHandle *fh, const Rid &rid) {
    auto file_hdr = fh->get_file_hdr();
    if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr.num_pages || rid.slot_no < 0 ||
        rid.slot_no >= file_hdr.num_records_per_page) {
        return false;
    }
    try {
        return fh->is_record(rid);
    } catch (...) {
        return false;
    }
}

void ensure_page_exists(RmFileHandle *fh, BufferPoolManager *buffer_pool_manager, const Rid &rid) {
    while (rid.page_no >= fh->get_file_hdr().num_pages) {
        auto page_handle = fh->create_new_page_handle();
        buffer_pool_manager->unpin_page(page_handle.page->get_page_id(), true);
    }
}

void put_record_at(RmFileHandle *fh, BufferPoolManager *buffer_pool_manager, const Rid &rid, RmRecord &record) {
    ensure_page_exists(fh, buffer_pool_manager, rid);
    if (record_exists(fh, rid)) {
        fh->update_record(rid, record.data, nullptr);
    } else {
        fh->insert_record(rid, record.data);
    }
}

std::unique_ptr<LogRecord> parse_log_record(const char *data) {
    auto log_type = *reinterpret_cast<const LogType *>(data + OFFSET_LOG_TYPE);
    std::unique_ptr<LogRecord> log_record;
    switch (log_type) {
        case LogType::UPDATE:
            log_record = std::make_unique<UpdateLogRecord>();
            break;
        case LogType::INSERT:
            log_record = std::make_unique<InsertLogRecord>();
            break;
        case LogType::DELETE:
            log_record = std::make_unique<DeleteLogRecord>();
            break;
        case LogType::begin:
            log_record = std::make_unique<BeginLogRecord>();
            break;
        case LogType::commit:
            log_record = std::make_unique<CommitLogRecord>();
            break;
        case LogType::ABORT:
            log_record = std::make_unique<AbortLogRecord>();
            break;
        default:
            return nullptr;
    }
    log_record->deserialize(data);
    return log_record;
}

std::vector<std::unique_ptr<LogRecord>> read_all_logs(DiskManager *disk_manager) {
    std::vector<std::unique_ptr<LogRecord>> logs;
    int file_size = disk_manager->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        return logs;
    }

    std::vector<char> data(file_size);
    int read_size = disk_manager->read_log(data.data(), file_size, 0);
    int offset = 0;
    while (read_size > 0 && offset + LOG_HEADER_SIZE <= read_size) {
        uint32_t log_len = *reinterpret_cast<const uint32_t *>(data.data() + offset + OFFSET_LOG_TOT_LEN);
        if (log_len < LOG_HEADER_SIZE || offset + static_cast<int>(log_len) > read_size) {
            break;
        }
        auto log_record = parse_log_record(data.data() + offset);
        if (log_record == nullptr) {
            break;
        }
        logs.push_back(std::move(log_record));
        offset += static_cast<int>(log_len);
    }
    return logs;
}

}  // namespace

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    int file_size = disk_manager_->get_file_size(LOG_FILE_NAME);
    if (file_size <= 0) {
        return;
    }
    int read_size = disk_manager_->read_log(buffer_.buffer_, std::min(file_size, LOG_BUFFER_SIZE), 0);
    buffer_.offset_ = std::max(0, read_size);
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    auto logs = read_all_logs(disk_manager_);
    std::set<txn_id_t> committed_txns;
    for (const auto &log_record : logs) {
        if (log_record->log_type_ == LogType::commit) {
            committed_txns.insert(log_record->log_tid_);
        }
    }

    for (const auto &log_record : logs) {
        if (committed_txns.count(log_record->log_tid_) == 0) {
            continue;
        }
        if (log_record->log_type_ == LogType::INSERT) {
            auto *insert_log = dynamic_cast<InsertLogRecord *>(log_record.get());
            auto table_name = table_name_from_log(insert_log->table_name_, insert_log->table_name_size_);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            put_record_at(fh, buffer_pool_manager_, insert_log->rid_, insert_log->insert_value_);
        } else if (log_record->log_type_ == LogType::DELETE) {
            auto *delete_log = dynamic_cast<DeleteLogRecord *>(log_record.get());
            auto table_name = table_name_from_log(delete_log->table_name_, delete_log->table_name_size_);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            if (record_exists(fh, delete_log->rid_)) {
                fh->delete_record(delete_log->rid_, nullptr);
            }
        } else if (log_record->log_type_ == LogType::UPDATE) {
            auto *update_log = dynamic_cast<UpdateLogRecord *>(log_record.get());
            auto table_name = table_name_from_log(update_log->table_name_, update_log->table_name_size_);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            put_record_at(fh, buffer_pool_manager_, update_log->rid_, update_log->new_value_);
        }
    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    auto logs = read_all_logs(disk_manager_);
    std::set<txn_id_t> finished_txns;
    std::set<txn_id_t> all_txns;
    for (const auto &log_record : logs) {
        if (log_record->log_tid_ != INVALID_TXN_ID) {
            all_txns.insert(log_record->log_tid_);
        }
        if (log_record->log_type_ == LogType::commit || log_record->log_type_ == LogType::ABORT) {
            finished_txns.insert(log_record->log_tid_);
        }
    }

    for (auto it = logs.rbegin(); it != logs.rend(); ++it) {
        auto &log_record = *it;
        if (all_txns.count(log_record->log_tid_) == 0 || finished_txns.count(log_record->log_tid_) != 0) {
            continue;
        }
        if (log_record->log_type_ == LogType::INSERT) {
            auto *insert_log = dynamic_cast<InsertLogRecord *>(log_record.get());
            auto table_name = table_name_from_log(insert_log->table_name_, insert_log->table_name_size_);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            if (record_exists(fh, insert_log->rid_)) {
                fh->delete_record(insert_log->rid_, nullptr);
            }
        } else if (log_record->log_type_ == LogType::DELETE) {
            auto *delete_log = dynamic_cast<DeleteLogRecord *>(log_record.get());
            auto table_name = table_name_from_log(delete_log->table_name_, delete_log->table_name_size_);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            put_record_at(fh, buffer_pool_manager_, delete_log->rid_, delete_log->delete_value_);
        } else if (log_record->log_type_ == LogType::UPDATE) {
            auto *update_log = dynamic_cast<UpdateLogRecord *>(log_record.get());
            auto table_name = table_name_from_log(update_log->table_name_, update_log->table_name_size_);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            put_record_at(fh, buffer_pool_manager_, update_log->rid_, update_log->old_value_);
        }
    }
    sm_manager_->rebuild_indexes();
}
