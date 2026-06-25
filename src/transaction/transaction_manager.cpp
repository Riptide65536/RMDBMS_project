/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "execution/executor_utils.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

#include <vector>

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

namespace {

void delete_index_entries(SmManager *sm_manager, const TabMeta &tab, const std::string &table_name,
                          const RmRecord &record, Transaction *txn) {
    for (const auto &index : tab.indexes) {
        auto index_name = sm_manager->get_ix_manager()->get_index_name(table_name, index.cols);
        auto ih_it = sm_manager->ihs_.find(index_name);
        if (ih_it == sm_manager->ihs_.end()) {
            continue;
        }
        auto key = build_index_key(index, record);
        ih_it->second->delete_entry(key.data(), txn);
    }
}

void insert_index_entries(SmManager *sm_manager, const TabMeta &tab, const std::string &table_name,
                          const RmRecord &record, const Rid &rid, Transaction *txn) {
    for (const auto &index : tab.indexes) {
        auto index_name = sm_manager->get_ix_manager()->get_index_name(table_name, index.cols);
        auto ih_it = sm_manager->ihs_.find(index_name);
        if (ih_it == sm_manager->ihs_.end()) {
            continue;
        }
        auto key = build_index_key(index, record);
        ih_it->second->insert_entry(key.data(), rid, txn);
    }
}

}  // namespace

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    (void)log_manager;
    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
    }
    txn->set_start_ts(next_timestamp_++);
    txn->set_state(TransactionState::GROWING);
    if (log_manager != nullptr) {
        BeginLogRecord log_record(txn->get_transaction_id());
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn = log_manager->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(lsn);
        log_manager->flush_log_to_disk();
    }

    std::unique_lock<std::mutex> lock(latch_);
    TransactionManager::txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTED || txn->get_state() == TransactionState::ABORTED) {
        return;
    }

    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        delete write_set->front();
        write_set->pop_front();
    }

    if (log_manager != nullptr) {
        CommitLogRecord log_record(txn->get_transaction_id());
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn = log_manager->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(lsn);
        log_manager->flush_log_to_disk();
    }

    auto lock_set = txn->get_lock_set();
    std::vector<LockDataId> locks_to_release(lock_set->begin(), lock_set->end());
    for (const auto &lock_data_id : locks_to_release) {
        lock_manager_->unlock(txn, lock_data_id);
    }
    lock_set->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    if (txn == nullptr) {
        return;
    }
    if (txn->get_state() == TransactionState::COMMITTED) {
        return;
    }

    auto write_set = txn->get_write_set();
    while (!write_set->empty()) {
        auto *write_record = write_set->back();
        write_set->pop_back();
        const std::string &table_name = write_record->GetTableName();
        auto &tab = sm_manager_->db_.get_table(table_name);
        auto fh = sm_manager_->fhs_.at(table_name).get();
        switch (write_record->GetWriteType()) {
            case WType::INSERT_TUPLE:
                if (fh->is_record(write_record->GetRid())) {
                    auto record = fh->get_record(write_record->GetRid(), nullptr);
                    delete_index_entries(sm_manager_, tab, table_name, *record, txn);
                    fh->delete_record(write_record->GetRid(), nullptr);
                }
                break;
            case WType::DELETE_TUPLE:
                fh->insert_record(write_record->GetRid(), write_record->GetRecord().data);
                insert_index_entries(sm_manager_, tab, table_name, write_record->GetRecord(), write_record->GetRid(),
                                     txn);
                break;
            case WType::UPDATE_TUPLE:
                if (fh->is_record(write_record->GetRid())) {
                    auto current_record = fh->get_record(write_record->GetRid(), nullptr);
                    for (const auto &index : tab.indexes) {
                        auto old_key = build_index_key(index, write_record->GetRecord());
                        auto current_key = build_index_key(index, *current_record);
                        if (memcmp(old_key.data(), current_key.data(), index.col_tot_len) != 0) {
                            auto index_name = sm_manager_->get_ix_manager()->get_index_name(table_name, index.cols);
                            auto ih_it = sm_manager_->ihs_.find(index_name);
                            if (ih_it == sm_manager_->ihs_.end()) {
                                continue;
                            }
                            ih_it->second->delete_entry(current_key.data(), txn);
                            ih_it->second->insert_entry(old_key.data(), write_record->GetRid(), txn);
                        }
                    }
                    fh->update_record(write_record->GetRid(), write_record->GetRecord().data, nullptr);
                }
                break;
            default:
                break;
        }
        delete write_record;
    }

    if (log_manager != nullptr) {
        AbortLogRecord log_record(txn->get_transaction_id());
        log_record.prev_lsn_ = txn->get_prev_lsn();
        lsn_t lsn = log_manager->add_log_to_buffer(&log_record);
        txn->set_prev_lsn(lsn);
        log_manager->flush_log_to_disk();
    }

    auto lock_set = txn->get_lock_set();
    std::vector<LockDataId> locks_to_release(lock_set->begin(), lock_set->end());
    for (const auto &lock_data_id : locks_to_release) {
        lock_manager_->unlock(txn, lock_data_id);
    }
    lock_set->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_index_deleted_page_set()->clear();
    txn->set_state(TransactionState::ABORTED);
}
