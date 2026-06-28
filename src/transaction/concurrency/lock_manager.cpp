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

namespace {

LockManager::GroupLockMode group_mode_for(LockManager::LockMode mode) {
    switch (mode) {
        case LockManager::LockMode::SHARED:
            return LockManager::GroupLockMode::S;
        case LockManager::LockMode::EXLUCSIVE:
            return LockManager::GroupLockMode::X;
        case LockManager::LockMode::INTENTION_SHARED:
            return LockManager::GroupLockMode::IS;
        case LockManager::LockMode::INTENTION_EXCLUSIVE:
            return LockManager::GroupLockMode::IX;
        case LockManager::LockMode::S_IX:
            return LockManager::GroupLockMode::SIX;
    }
    return LockManager::GroupLockMode::NON_LOCK;
}

int group_mode_rank(LockManager::GroupLockMode mode) {
    switch (mode) {
        case LockManager::GroupLockMode::NON_LOCK:
            return 0;
        case LockManager::GroupLockMode::IS:
            return 1;
        case LockManager::GroupLockMode::IX:
            return 2;
        case LockManager::GroupLockMode::S:
            return 3;
        case LockManager::GroupLockMode::SIX:
            return 4;
        case LockManager::GroupLockMode::X:
            return 5;
    }
    return 0;
}

}  // namespace

bool LockManager::is_compatible(LockMode held_mode, LockMode request_mode) const {
    if (held_mode == LockMode::EXLUCSIVE || request_mode == LockMode::EXLUCSIVE) {
        return false;
    }
    if (held_mode == LockMode::S_IX || request_mode == LockMode::S_IX) {
        return held_mode == LockMode::INTENTION_SHARED || request_mode == LockMode::INTENTION_SHARED;
    }
    if (held_mode == LockMode::SHARED) {
        return request_mode == LockMode::SHARED || request_mode == LockMode::INTENTION_SHARED;
    }
    if (request_mode == LockMode::SHARED) {
        return held_mode == LockMode::SHARED || held_mode == LockMode::INTENTION_SHARED;
    }
    return true;
}

void LockManager::refresh_group_lock_mode(LockRequestQueue &queue) {
    auto group_mode = GroupLockMode::NON_LOCK;
    for (const auto &request : queue.request_queue_) {
        if (!request.granted_) {
            continue;
        }
        auto request_group_mode = group_mode_for(request.lock_mode_);
        if (group_mode_rank(request_group_mode) > group_mode_rank(group_mode)) {
            group_mode = request_group_mode;
        }
    }
    queue.group_lock_mode_ = group_mode;
}

bool LockManager::lock(Transaction* txn, const LockDataId &lock_data_id, LockMode lock_mode) {
    if (txn == nullptr) {
        return true;
    }
    if (txn->get_state() == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
    }

    std::unique_lock<std::mutex> lock_guard(latch_);
    auto &queue = lock_table_[lock_data_id];
    auto existing_request = queue.request_queue_.end();
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            existing_request = it;
            break;
        }
    }

    if (existing_request != queue.request_queue_.end() && existing_request->granted_) {
        if (existing_request->lock_mode_ == lock_mode ||
            existing_request->lock_mode_ == LockMode::EXLUCSIVE ||
            (existing_request->lock_mode_ == LockMode::SHARED && lock_mode == LockMode::INTENTION_SHARED) ||
            (existing_request->lock_mode_ == LockMode::S_IX && lock_mode != LockMode::EXLUCSIVE)) {
            txn->get_lock_set()->insert(lock_data_id);
            return true;
        }
    }

    for (const auto &request : queue.request_queue_) {
        if (!request.granted_ || request.txn_id_ == txn->get_transaction_id()) {
            continue;
        }
        if (!is_compatible(request.lock_mode_, lock_mode)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
        }
    }

    if (existing_request != queue.request_queue_.end()) {
        existing_request->lock_mode_ = lock_mode;
        existing_request->granted_ = true;
    } else {
        queue.request_queue_.emplace_back(txn->get_transaction_id(), lock_mode);
        queue.request_queue_.back().granted_ = true;
    }
    refresh_group_lock_mode(queue);
    txn->get_lock_set()->insert(lock_data_id);
    return true;
}

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, rid, LockDataType::RECORD), LockMode::SHARED);
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, rid, LockDataType::RECORD), LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::SHARED);
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::EXLUCSIVE);
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::INTENTION_SHARED);
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    return lock(txn, LockDataId(tab_fd, LockDataType::TABLE), LockMode::INTENTION_EXCLUSIVE);
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    if (txn == nullptr) {
        return true;
    }

    std::unique_lock<std::mutex> lock_guard(latch_);
    auto queue_it = lock_table_.find(lock_data_id);
    if (queue_it == lock_table_.end()) {
        return false;
    }

    auto &queue = queue_it->second;
    for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            queue.request_queue_.erase(it);
            txn->get_lock_set()->erase(lock_data_id);
            refresh_group_lock_mode(queue);
            if (queue.request_queue_.empty()) {
                lock_table_.erase(queue_it);
            }
            if (txn->get_state() == TransactionState::GROWING) {
                txn->set_state(TransactionState::SHRINKING);
            }
            return true;
        }
    }
    return true;
}
