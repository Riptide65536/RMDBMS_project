/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "executor_utils.h"
#include "index/ix.h"
#include "system/sm.h"
#include "transaction/txn_defs.h"

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
        if (context_ != nullptr && context_->lock_mgr_ != nullptr && context_->txn_ != nullptr) {
            context_->lock_mgr_->lock_exclusive_on_table(context_->txn_, fh_->GetFd());
        }
    }
    std::unique_ptr<RmRecord> Next() override {
        for (const auto &rid : rids_) {
            auto old_record = fh_->get_record(rid, context_);
            if (context_ != nullptr && context_->txn_ != nullptr) {
                context_->txn_->append_write_record(new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *old_record));
            }

            RmRecord new_record(*old_record);
            for (const auto &set_clause : set_clauses_) {
                auto col = tab_.get_col(set_clause.lhs.col_name);
                Value rhs = cast_value_to_col_type(set_clause.rhs, *col);
                rhs.init_raw(col->len);
                memcpy(new_record.data + col->offset, rhs.raw->data, col->len);
            }

            for (const auto &index : tab_.indexes) {
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
                auto ih_it = sm_manager_->ihs_.find(index_name);
                if (ih_it == sm_manager_->ihs_.end()) {
                    continue;
                }
                auto old_key = build_index_key(index, *old_record);
                auto new_key = build_index_key(index, new_record);
                if (memcmp(old_key.data(), new_key.data(), index.col_tot_len) != 0) {
                    ensure_index_key_unique(ih_it->second.get(), new_key.data(), &rid);
                }
            }

            if (context_ != nullptr && context_->txn_ != nullptr && context_->log_mgr_ != nullptr) {
                Rid log_rid = rid;
                UpdateLogRecord log_record(context_->txn_->get_transaction_id(), *old_record, new_record, log_rid,
                                           tab_name_);
                log_record.prev_lsn_ = context_->txn_->get_prev_lsn();
                lsn_t lsn = context_->log_mgr_->add_log_to_buffer(&log_record);
                context_->txn_->set_prev_lsn(lsn);
                context_->log_mgr_->flush_log_to_disk();
            }

            for (const auto &index : tab_.indexes) {
                auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols);
                auto ih_it = sm_manager_->ihs_.find(index_name);
                if (ih_it == sm_manager_->ihs_.end()) {
                    continue;
                }
                auto old_key = build_index_key(index, *old_record);
                auto new_key = build_index_key(index, new_record);
                if (memcmp(old_key.data(), new_key.data(), index.col_tot_len) != 0) {
                    ih_it->second->delete_entry(old_key.data(), context_->txn_);
                    ih_it->second->insert_entry(new_key.data(), rid, context_->txn_);
                }
            }

            fh_->update_record(rid, new_record.data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};
