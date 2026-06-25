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

#include <cstring>

#include "execution/executor_utils.h"

namespace {

std::string table_name_from_log(const char *table_name, size_t table_name_size) {
    return std::string(table_name, table_name_size);
}

void ensure_record_page_exists(RmFileHandle *fh, BufferPoolManager *bpm, const Rid &rid) {
    while (fh->get_file_hdr().num_pages <= rid.page_no) {
        RmPageHandle page_handle = fh->create_new_page_handle();
        bpm->unpin_page(page_handle.page->get_page_id(), true);
    }
}

bool record_exists(RmFileHandle *fh, const Rid &rid) {
    auto file_hdr = fh->get_file_hdr();
    if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr.num_pages || rid.slot_no < 0 ||
        rid.slot_no >= file_hdr.num_records_per_page) {
        return false;
    }
    return fh->is_record(rid);
}

void insert_index_entries(SmManager *sm_manager, const TabMeta &tab, const std::string &table_name,
                          const RmRecord &record, const Rid &rid) {
    for (const auto &index : tab.indexes) {
        auto index_name = sm_manager->get_ix_manager()->get_index_name(table_name, index.cols);
        auto ih_it = sm_manager->ihs_.find(index_name);
        if (ih_it == sm_manager->ihs_.end()) {
            continue;
        }
        auto key = build_index_key(index, record);
        std::vector<Rid> result;
        if (!ih_it->second->get_value(key.data(), &result, nullptr)) {
            ih_it->second->insert_entry(key.data(), rid, nullptr);
        }
    }
}

void delete_index_entries(SmManager *sm_manager, const TabMeta &tab, const std::string &table_name,
                          const RmRecord &record) {
    for (const auto &index : tab.indexes) {
        auto index_name = sm_manager->get_ix_manager()->get_index_name(table_name, index.cols);
        auto ih_it = sm_manager->ihs_.find(index_name);
        if (ih_it == sm_manager->ihs_.end()) {
            continue;
        }
        auto key = build_index_key(index, record);
        ih_it->second->delete_entry(key.data(), nullptr);
    }
}

std::unique_ptr<LogRecord> make_log_record(LogType log_type) {
    switch (log_type) {
        case LogType::begin:
            return std::make_unique<BeginLogRecord>();
        case LogType::commit:
            return std::make_unique<CommitLogRecord>();
        case LogType::ABORT:
            return std::make_unique<AbortLogRecord>();
        case LogType::INSERT:
            return std::make_unique<InsertLogRecord>();
        case LogType::DELETE:
            return std::make_unique<DeleteLogRecord>();
        case LogType::UPDATE:
            return std::make_unique<UpdateLogRecord>();
    }
    return nullptr;
}

}  // namespace

void RecoveryManager::analyze() {
    logs_.clear();
    active_txns_.clear();
    committed_txns_.clear();

    if (!disk_manager_->is_file(LOG_FILE_NAME)) {
        return;
    }

    int offset = 0;
    while (true) {
        char header[LOG_HEADER_SIZE];
        int header_size = disk_manager_->read_log(header, LOG_HEADER_SIZE, offset);
        if (header_size <= 0) {
            break;
        }
        if (header_size < LOG_HEADER_SIZE) {
            break;
        }

        LogType log_type = *reinterpret_cast<LogType*>(header + OFFSET_LOG_TYPE);
        uint32_t log_tot_len = *reinterpret_cast<uint32_t*>(header + OFFSET_LOG_TOT_LEN);
        if (log_tot_len < LOG_HEADER_SIZE || log_tot_len > LOG_BUFFER_SIZE) {
            break;
        }

        std::vector<char> raw(log_tot_len);
        int read_size = disk_manager_->read_log(raw.data(), log_tot_len, offset);
        if (read_size < static_cast<int>(log_tot_len)) {
            break;
        }

        auto log_record = make_log_record(log_type);
        if (log_record == nullptr) {
            break;
        }
        log_record->deserialize(raw.data());

        switch (log_record->log_type_) {
            case LogType::begin:
                active_txns_.insert(log_record->log_tid_);
                break;
            case LogType::commit:
                committed_txns_.insert(log_record->log_tid_);
                active_txns_.erase(log_record->log_tid_);
                break;
            case LogType::ABORT:
                active_txns_.erase(log_record->log_tid_);
                break;
            default:
                active_txns_.insert(log_record->log_tid_);
                break;
        }

        logs_.push_back(std::move(log_record));
        offset += log_tot_len;
    }
}

void RecoveryManager::redo() {
    for (const auto &log_record : logs_) {
        if (committed_txns_.count(log_record->log_tid_) == 0) {
            continue;
        }

        if (log_record->log_type_ == LogType::INSERT) {
            auto *insert_log = static_cast<InsertLogRecord*>(log_record.get());
            std::string table_name = table_name_from_log(insert_log->table_name_, insert_log->table_name_size_);
            auto &tab = sm_manager_->db_.get_table(table_name);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            ensure_record_page_exists(fh, buffer_pool_manager_, insert_log->rid_);
            fh->insert_record(insert_log->rid_, insert_log->insert_value_.data);
            insert_index_entries(sm_manager_, tab, table_name, insert_log->insert_value_, insert_log->rid_);
        } else if (log_record->log_type_ == LogType::DELETE) {
            auto *delete_log = static_cast<DeleteLogRecord*>(log_record.get());
            std::string table_name = table_name_from_log(delete_log->table_name_, delete_log->table_name_size_);
            auto &tab = sm_manager_->db_.get_table(table_name);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            if (record_exists(fh, delete_log->rid_)) {
                auto record = fh->get_record(delete_log->rid_, nullptr);
                delete_index_entries(sm_manager_, tab, table_name, *record);
                fh->delete_record(delete_log->rid_, nullptr);
            }
        } else if (log_record->log_type_ == LogType::UPDATE) {
            auto *update_log = static_cast<UpdateLogRecord*>(log_record.get());
            std::string table_name = table_name_from_log(update_log->table_name_, update_log->table_name_size_);
            auto &tab = sm_manager_->db_.get_table(table_name);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            ensure_record_page_exists(fh, buffer_pool_manager_, update_log->rid_);
            if (record_exists(fh, update_log->rid_)) {
                auto current = fh->get_record(update_log->rid_, nullptr);
                delete_index_entries(sm_manager_, tab, table_name, *current);
            }
            fh->insert_record(update_log->rid_, update_log->new_value_.data);
            insert_index_entries(sm_manager_, tab, table_name, update_log->new_value_, update_log->rid_);
        }
    }
}

void RecoveryManager::undo() {
    for (auto iter = logs_.rbegin(); iter != logs_.rend(); ++iter) {
        const auto &log_record = *iter;
        if (active_txns_.count(log_record->log_tid_) == 0) {
            continue;
        }

        if (log_record->log_type_ == LogType::INSERT) {
            auto *insert_log = static_cast<InsertLogRecord*>(log_record.get());
            std::string table_name = table_name_from_log(insert_log->table_name_, insert_log->table_name_size_);
            auto &tab = sm_manager_->db_.get_table(table_name);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            if (record_exists(fh, insert_log->rid_)) {
                auto record = fh->get_record(insert_log->rid_, nullptr);
                delete_index_entries(sm_manager_, tab, table_name, *record);
                fh->delete_record(insert_log->rid_, nullptr);
            }
        } else if (log_record->log_type_ == LogType::DELETE) {
            auto *delete_log = static_cast<DeleteLogRecord*>(log_record.get());
            std::string table_name = table_name_from_log(delete_log->table_name_, delete_log->table_name_size_);
            auto &tab = sm_manager_->db_.get_table(table_name);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            ensure_record_page_exists(fh, buffer_pool_manager_, delete_log->rid_);
            fh->insert_record(delete_log->rid_, delete_log->delete_value_.data);
            insert_index_entries(sm_manager_, tab, table_name, delete_log->delete_value_, delete_log->rid_);
        } else if (log_record->log_type_ == LogType::UPDATE) {
            auto *update_log = static_cast<UpdateLogRecord*>(log_record.get());
            std::string table_name = table_name_from_log(update_log->table_name_, update_log->table_name_size_);
            auto &tab = sm_manager_->db_.get_table(table_name);
            auto fh = sm_manager_->fhs_.at(table_name).get();
            ensure_record_page_exists(fh, buffer_pool_manager_, update_log->rid_);
            if (record_exists(fh, update_log->rid_)) {
                auto current = fh->get_record(update_log->rid_, nullptr);
                delete_index_entries(sm_manager_, tab, table_name, *current);
            }
            fh->insert_record(update_log->rid_, update_log->old_value_.data);
            insert_index_entries(sm_manager_, tab, table_name, update_log->old_value_, update_log->rid_);
        }
    }

    for (auto txn_id : active_txns_) {
        AbortLogRecord abort_log(txn_id);
        abort_log.log_tot_len_ = LOG_HEADER_SIZE;
        std::vector<char> raw(abort_log.log_tot_len_);
        abort_log.serialize(raw.data());
        disk_manager_->write_log(raw.data(), static_cast<int>(raw.size()));
    }
    active_txns_.clear();
}
