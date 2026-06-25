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

#include <limits>
#include <memory>
#include <vector>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "executor_utils.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    struct ColumnBound {
        const Value *eq = nullptr;
        const Value *lower = nullptr;
        bool lower_inclusive = true;
        const Value *upper = nullptr;
        bool upper_inclusive = true;
    };

    std::string tab_name_;
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<ColMeta> cols_;
    size_t len_;
    std::vector<Condition> fed_conds_;

    std::vector<std::string> index_col_names_;
    IndexMeta index_meta_;
    IxIndexHandle *ih_;

    Rid rid_;
    std::unique_ptr<IxScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names, Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        index_col_names_ = std::move(index_col_names);
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        ih_ = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_meta_.cols)).get();
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;

        if (context_ != nullptr && context_->lock_mgr_ != nullptr && context_->txn_ != nullptr) {
            context_->lock_mgr_->lock_shared_on_table(context_->txn_, fh_->GetFd());
        }

        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        auto [lower, upper] = build_scan_range();
        scan_ = std::make_unique<IxScan>(ih_, lower, upper, sm_manager_->get_bpm());
        advance_to_match();
    }

    void nextTuple() override {
        if (scan_ == nullptr || scan_->is_end()) {
            rid_ = {fh_->get_file_hdr().num_pages, -1};
            return;
        }
        scan_->next();
        advance_to_match();
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_);
    }

    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "IndexScanExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return rid_; }

   private:
    static void fill_min_value(char *dest, const ColMeta &col) {
        switch (col.type) {
            case TYPE_INT: {
                int32_t value = std::numeric_limits<int32_t>::min();
                memcpy(dest, &value, sizeof(value));
                break;
            }
            case TYPE_BIGINT:
            case TYPE_DATETIME: {
                int64_t value = std::numeric_limits<int64_t>::min();
                memcpy(dest, &value, sizeof(value));
                break;
            }
            case TYPE_FLOAT: {
                float value = std::numeric_limits<float>::lowest();
                memcpy(dest, &value, sizeof(value));
                break;
            }
            case TYPE_STRING:
                memset(dest, 0, col.len);
                break;
        }
    }

    static void fill_max_value(char *dest, const ColMeta &col) {
        switch (col.type) {
            case TYPE_INT: {
                int32_t value = std::numeric_limits<int32_t>::max();
                memcpy(dest, &value, sizeof(value));
                break;
            }
            case TYPE_BIGINT:
            case TYPE_DATETIME: {
                int64_t value = std::numeric_limits<int64_t>::max();
                memcpy(dest, &value, sizeof(value));
                break;
            }
            case TYPE_FLOAT: {
                float value = std::numeric_limits<float>::max();
                memcpy(dest, &value, sizeof(value));
                break;
            }
            case TYPE_STRING:
                memset(dest, 0xFF, col.len);
                break;
        }
    }

    static int compare_bound_value(const Value *lhs, const Value *rhs, const ColMeta &col) {
        return compare_raw_data(lhs->raw->data, col.type, col.len, rhs->raw->data, col.type, col.len);
    }

    int compare_composite_key(const char *lhs, const char *rhs) const {
        int offset = 0;
        for (const auto &col : index_meta_.cols) {
            int cmp = compare_raw_data(lhs + offset, col.type, col.len, rhs + offset, col.type, col.len);
            if (cmp != 0) {
                return cmp;
            }
            offset += col.len;
        }
        return 0;
    }

    std::vector<ColumnBound> collect_bounds() const {
        std::vector<ColumnBound> bounds(index_meta_.col_num);
        for (const auto &cond : fed_conds_) {
            if (!cond.is_rhs_val || cond.op == OP_NE) {
                continue;
            }
            for (int i = 0; i < index_meta_.col_num; ++i) {
                const auto &col = index_meta_.cols[i];
                if (cond.lhs_col.tab_name != tab_name_ || cond.lhs_col.col_name != col.name) {
                    continue;
                }
                auto &bound = bounds[i];
                if (cond.op == OP_EQ) {
                    bound.eq = &cond.rhs_val;
                    bound.lower = &cond.rhs_val;
                    bound.lower_inclusive = true;
                    bound.upper = &cond.rhs_val;
                    bound.upper_inclusive = true;
                    break;
                }
                if (cond.op == OP_GT || cond.op == OP_GE) {
                    bool inclusive = cond.op == OP_GE;
                    if (bound.lower == nullptr) {
                        bound.lower = &cond.rhs_val;
                        bound.lower_inclusive = inclusive;
                    } else {
                        int cmp = compare_bound_value(&cond.rhs_val, bound.lower, col);
                        if (cmp > 0 || (cmp == 0 && !inclusive && bound.lower_inclusive)) {
                            bound.lower = &cond.rhs_val;
                            bound.lower_inclusive = inclusive;
                        }
                    }
                } else if (cond.op == OP_LT || cond.op == OP_LE) {
                    bool inclusive = cond.op == OP_LE;
                    if (bound.upper == nullptr) {
                        bound.upper = &cond.rhs_val;
                        bound.upper_inclusive = inclusive;
                    } else {
                        int cmp = compare_bound_value(&cond.rhs_val, bound.upper, col);
                        if (cmp < 0 || (cmp == 0 && !inclusive && bound.upper_inclusive)) {
                            bound.upper = &cond.rhs_val;
                            bound.upper_inclusive = inclusive;
                        }
                    }
                }
                break;
            }
        }
        return bounds;
    }

    std::pair<Iid, Iid> build_scan_range() const {
        auto bounds = collect_bounds();
        std::vector<char> lower_key(index_meta_.col_tot_len, 0);
        std::vector<char> upper_key(index_meta_.col_tot_len, 0);
        bool use_lower = false;
        bool use_upper = false;
        bool lower_inclusive = true;
        bool upper_inclusive = true;
        bool used_prefix = false;
        bool range_consumed = false;

        int offset = 0;
        int stop_col = index_meta_.col_num;
        for (int i = 0; i < index_meta_.col_num; ++i) {
            const auto &col = index_meta_.cols[i];
            const auto &bound = bounds[i];
            char *lower_slot = lower_key.data() + offset;
            char *upper_slot = upper_key.data() + offset;

            if (!range_consumed && bound.eq != nullptr) {
                memcpy(lower_slot, bound.eq->raw->data, col.len);
                memcpy(upper_slot, bound.eq->raw->data, col.len);
                use_lower = true;
                use_upper = true;
                used_prefix = true;
                offset += col.len;
                continue;
            }

            if (!range_consumed && (bound.lower != nullptr || bound.upper != nullptr)) {
                if (bound.lower != nullptr) {
                    memcpy(lower_slot, bound.lower->raw->data, col.len);
                    lower_inclusive = bound.lower_inclusive;
                } else {
                    fill_min_value(lower_slot, col);
                    lower_inclusive = true;
                }
                if (bound.upper != nullptr) {
                    memcpy(upper_slot, bound.upper->raw->data, col.len);
                    upper_inclusive = bound.upper_inclusive;
                } else {
                    fill_max_value(upper_slot, col);
                    upper_inclusive = true;
                }
                use_lower = true;
                use_upper = true;
                used_prefix = true;
                range_consumed = true;
                stop_col = i + 1;
                offset += col.len;
                break;
            }

            stop_col = i;
            break;
        }

        if (used_prefix && !range_consumed) {
            use_lower = true;
            use_upper = true;
            lower_inclusive = true;
            upper_inclusive = true;
        }

        for (int i = stop_col; i < index_meta_.col_num; ++i) {
            const auto &col = index_meta_.cols[i];
            char *lower_slot = lower_key.data() + offset;
            char *upper_slot = upper_key.data() + offset;
            if (used_prefix) {
                fill_min_value(lower_slot, col);
                fill_max_value(upper_slot, col);
                if (range_consumed && !lower_inclusive) {
                    fill_max_value(lower_slot, col);
                }
                if (range_consumed && !upper_inclusive) {
                    fill_min_value(upper_slot, col);
                }
            }
            offset += col.len;
        }

        if (use_lower && use_upper) {
            int cmp = compare_composite_key(lower_key.data(), upper_key.data());
            if (cmp > 0 || (cmp == 0 && (!lower_inclusive || !upper_inclusive))) {
                Iid empty = ih_->leaf_begin();
                return {empty, empty};
            }
        }

        Iid lower = use_lower ? (lower_inclusive ? ih_->lower_bound(lower_key.data()) : ih_->upper_bound(lower_key.data()))
                              : ih_->leaf_begin();
        Iid upper = use_upper ? (upper_inclusive ? ih_->upper_bound(upper_key.data()) : ih_->lower_bound(upper_key.data()))
                              : ih_->leaf_end();
        return {lower, upper};
    }

    void advance_to_match() {
        while (scan_ != nullptr && !scan_->is_end()) {
            Rid candidate = scan_->rid();
            auto record = fh_->get_record(candidate, context_);
            if (record_satisfies_conditions(*record, cols_, fed_conds_)) {
                rid_ = candidate;
                return;
            }
            scan_->next();
        }
        rid_ = {fh_->get_file_hdr().num_pages, -1};
    }
};
