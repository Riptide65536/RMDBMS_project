/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

bool LockManager::is_compatible(LockMode existing_mode, LockMode requested_mode) const {
    switch (existing_mode) {
        case LockMode::INTENTION_SHARED:
            return requested_mode != LockMode::EXLUCSIVE;
        case LockMode::INTENTION_EXCLUSIVE:
            return requested_mode == LockMode::INTENTION_SHARED ||
                   requested_mode == LockMode::INTENTION_EXCLUSIVE;
        case LockMode::SHARED:
            return requested_mode == LockMode::INTENTION_SHARED || requested_mode == LockMode::SHARED;
        case LockMode::S_IX:
            return requested_mode == LockMode::INTENTION_SHARED;
        case LockMode::EXLUCSIVE:
            return false;
    }
    return false;
}

LockManager::LockMode LockManager::get_upgraded_lock_mode(LockMode existing_mode, LockMode requested_mode) const {
    if (existing_mode == requested_mode || existing_mode == LockMode::EXLUCSIVE ||
        requested_mode == LockMode::INTENTION_SHARED) {
        return existing_mode;
    }
    if (requested_mode == LockMode::EXLUCSIVE) {
        return LockMode::EXLUCSIVE;
    }
    if ((existing_mode == LockMode::SHARED && requested_mode == LockMode::INTENTION_EXCLUSIVE) ||
        (existing_mode == LockMode::INTENTION_EXCLUSIVE && requested_mode == LockMode::SHARED) ||
        existing_mode == LockMode::S_IX || requested_mode == LockMode::S_IX) {
        return LockMode::S_IX;
    }
    if (existing_mode == LockMode::INTENTION_SHARED) {
        return requested_mode;
    }
    return existing_mode;
}

LockManager::GroupLockMode LockManager::get_group_lock_mode(const LockRequestQueue &request_queue) const {
    bool has_is = false;
    bool has_ix = false;
    bool has_s = false;
    bool has_x = false;
    bool has_six = false;

    for (const auto &request : request_queue.request_queue_) {
        if (!request.granted_) {
            continue;
        }
        switch (request.lock_mode_) {
            case LockMode::INTENTION_SHARED:
                has_is = true;
                break;
            case LockMode::INTENTION_EXCLUSIVE:
                has_ix = true;
                break;
            case LockMode::SHARED:
                has_s = true;
                break;
            case LockMode::EXLUCSIVE:
                has_x = true;
                break;
            case LockMode::S_IX:
                has_six = true;
                break;
        }
    }

    if (has_x) {
        return GroupLockMode::X;
    }
    if (has_six || (has_s && has_ix)) {
        return GroupLockMode::SIX;
    }
    if (has_s) {
        return GroupLockMode::S;
    }
    if (has_ix) {
        return GroupLockMode::IX;
    }
    if (has_is) {
        return GroupLockMode::IS;
    }
    return GroupLockMode::NON_LOCK;
}

bool LockManager::lock(Transaction* txn, LockDataId lock_data_id, LockMode lock_mode) {
    if (txn == nullptr) {
        return true;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        txn->set_state(TransactionState::ABORTED);
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    std::scoped_lock lock_guard{latch_};
    auto &request_queue = lock_table_[lock_data_id];
    auto txn_id = txn->get_transaction_id();

    auto own_request = request_queue.request_queue_.end();
    for (auto iter = request_queue.request_queue_.begin(); iter != request_queue.request_queue_.end(); ++iter) {
        if (iter->txn_id_ == txn_id && iter->granted_) {
            own_request = iter;
            break;
        }
    }

    if (own_request != request_queue.request_queue_.end()) {
        LockMode upgraded_mode = get_upgraded_lock_mode(own_request->lock_mode_, lock_mode);
        if (own_request->lock_mode_ == upgraded_mode) {
            txn->get_lock_set()->insert(lock_data_id);
            return true;
        }

        for (const auto &request : request_queue.request_queue_) {
            if (!request.granted_ || request.txn_id_ == txn_id) {
                continue;
            }
            if (!is_compatible(request.lock_mode_, upgraded_mode)) {
                txn->set_state(TransactionState::ABORTED);
                throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
            }
        }
        own_request->lock_mode_ = upgraded_mode;
        request_queue.group_lock_mode_ = get_group_lock_mode(request_queue);
        txn->get_lock_set()->insert(lock_data_id);
        return true;
    }

    for (const auto &request : request_queue.request_queue_) {
        if (!request.granted_ || request.txn_id_ == txn_id) {
            continue;
        }
        if (!is_compatible(request.lock_mode_, lock_mode)) {
            txn->set_state(TransactionState::ABORTED);
            throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
        }
    }

    request_queue.request_queue_.emplace_back(txn_id, lock_mode);
    request_queue.request_queue_.back().granted_ = true;
    request_queue.group_lock_mode_ = get_group_lock_mode(request_queue);
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    lock_IS_on_table(txn, tab_fd);
    return lock(txn, LockDataId(tab_fd, rid, LockDataType::RECORD), LockMode::SHARED);
}

bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    lock_IX_on_table(txn, tab_fd);
    return lock(txn, LockDataId(tab_fd, rid, LockDataType::RECORD), LockMode::EXLUCSIVE);
}

bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::SHARED);
}

bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::EXLUCSIVE);
}

bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::INTENTION_SHARED);
}

bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::INTENTION_EXCLUSIVE);
}

bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    if (txn == nullptr) {
        return true;
    }

    std::scoped_lock lock_guard{latch_};
    auto queue_iter = lock_table_.find(lock_data_id);
    if (queue_iter == lock_table_.end()) {
        return false;
    }

    auto &request_queue = queue_iter->second;
    auto request_iter = request_queue.request_queue_.end();
    for (auto iter = request_queue.request_queue_.begin(); iter != request_queue.request_queue_.end(); ++iter) {
        if (iter->txn_id_ == txn->get_transaction_id()) {
            request_iter = iter;
            break;
        }
    }
    if (request_iter == request_queue.request_queue_.end()) {
        return false;
    }

    request_queue.request_queue_.erase(request_iter);
    request_queue.group_lock_mode_ = get_group_lock_mode(request_queue);
    if (request_queue.request_queue_.empty()) {
        lock_table_.erase(queue_iter);
    }
    txn->get_lock_set()->erase(lock_data_id);
    if (txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }
    return true;
}
