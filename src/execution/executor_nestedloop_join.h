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
#include <algorithm>
#include <memory>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "executor_utils.h"
#include "index/ix.h"
#include "system/sm.h"

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    static constexpr size_t JOIN_BUFFER_BYTES = 4 * 1024 * 1024;

    std::unique_ptr<AbstractExecutor> left_;
    std::unique_ptr<AbstractExecutor> right_;
    size_t len_;
    std::vector<ColMeta> cols_;

    std::vector<Condition> fed_conds_;
    bool isend_ = true;

    std::vector<std::unique_ptr<RmRecord>> left_block_;
    size_t left_block_capacity_ = 1;
    size_t left_block_idx_ = 0;
    std::unique_ptr<RmRecord> right_tuple_;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }
        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);

        size_t left_tuple_len = std::max<size_t>(1, left_->tupleLen());
        left_block_capacity_ = std::max<size_t>(1, JOIN_BUFFER_BYTES / left_tuple_len);
    }

    void beginTuple() override {
        isend_ = false;
        left_->beginTuple();
        if (!load_next_left_block()) {
            isend_ = true;
            return;
        }
        right_->beginTuple();
        if (right_->is_end()) {
            isend_ = true;
            left_block_.clear();
            return;
        }
        right_tuple_ = right_->Next();
        left_block_idx_ = 0;
        advance_to_next_match();
    }

    void nextTuple() override {
        if (isend_) {
            return;
        }
        ++left_block_idx_;
        advance_to_next_match();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (isend_ || right_tuple_ == nullptr || left_block_idx_ >= left_block_.size()) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(make_joined_record(*left_block_[left_block_idx_], *right_tuple_));
    }

    bool is_end() const override { return isend_; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "NestedLoopJoinExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return _abstract_rid; }

   private:
    bool load_next_left_block() {
        left_block_.clear();
        left_block_idx_ = 0;
        while (!left_->is_end() && left_block_.size() < left_block_capacity_) {
            left_block_.push_back(left_->Next());
            left_->nextTuple();
        }
        return !left_block_.empty();
    }

    void advance_to_next_match() {
        while (true) {
            while (right_tuple_ != nullptr) {
                while (left_block_idx_ < left_block_.size()) {
                    if (record_satisfies_conditions(make_joined_record(*left_block_[left_block_idx_], *right_tuple_),
                                                    cols_, fed_conds_)) {
                        return;
                    }
                    ++left_block_idx_;
                }

                left_block_idx_ = 0;
                right_->nextTuple();
                if (right_->is_end()) {
                    right_tuple_.reset();
                    break;
                }
                right_tuple_ = right_->Next();
            }

            if (!load_next_left_block()) {
                isend_ = true;
                right_tuple_.reset();
                return;
            }
            right_->beginTuple();
            if (right_->is_end()) {
                isend_ = true;
                right_tuple_.reset();
                return;
            }
            right_tuple_ = right_->Next();
            left_block_idx_ = 0;
        }
    }

    RmRecord make_joined_record(const RmRecord &left_record, const RmRecord &right_record) const {
        RmRecord record(len_);
        memcpy(record.data, left_record.data, left_->tupleLen());
        memcpy(record.data + left_->tupleLen(), right_record.data, right_->tupleLen());
        return record;
    }
};
