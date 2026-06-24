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

#include <cstring>
#include <memory>
#include <vector>

#include "execution_defs.h"
#include "executor_abstract.h"
#include "executor_utils.h"

class AggregateExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<AggregateExpr> aggregates_;
    std::vector<ColMeta> cols_;
    std::vector<ColMeta> source_cols_;
    size_t len_ = 0;
    bool is_end_ = true;
    std::unique_ptr<RmRecord> result_;

   public:
    AggregateExecutor(std::unique_ptr<AbstractExecutor> prev, std::vector<AggregateExpr> aggregates) {
        prev_ = std::move(prev);
        aggregates_ = std::move(aggregates);

        size_t offset = 0;
        source_cols_.reserve(aggregates_.size());
        for (const auto &agg : aggregates_) {
            ColMeta out_col;
            out_col.tab_name = "";
            out_col.name = agg.alias;
            out_col.offset = offset;
            out_col.index = false;

            ColMeta source_col{};
            if (!agg.is_count_star) {
                source_col = prev_->get_col_offset(agg.col);
            }
            source_cols_.push_back(source_col);

            switch (agg.type) {
                case AGG_COUNT:
                    out_col.type = TYPE_BIGINT;
                    out_col.len = sizeof(int64_t);
                    break;
                case AGG_SUM:
                    out_col.type = source_col.type == TYPE_FLOAT ? TYPE_FLOAT : TYPE_BIGINT;
                    out_col.len = out_col.type == TYPE_FLOAT ? sizeof(float) : sizeof(int64_t);
                    break;
                case AGG_MAX:
                case AGG_MIN:
                    out_col.type = source_col.type;
                    out_col.len = source_col.len;
                    break;
            }
            offset += out_col.len;
            cols_.push_back(out_col);
        }
        len_ = offset;
    }

    void beginTuple() override {
        result_ = std::make_unique<RmRecord>(len_);
        if (len_ > 0) {
            memset(result_->data, 0, len_);
        }

        std::vector<int64_t> count_values(aggregates_.size(), 0);
        std::vector<long double> numeric_sums(aggregates_.size(), 0);
        std::vector<std::vector<char>> best_values(aggregates_.size());
        std::vector<bool> initialized(aggregates_.size(), false);

        prev_->beginTuple();
        for (; !prev_->is_end(); prev_->nextTuple()) {
            auto record = prev_->Next();
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                const auto &agg = aggregates_[i];
                if (agg.type == AGG_COUNT) {
                    count_values[i]++;
                    continue;
                }

                const auto &source_col = source_cols_[i];
                const char *data = record->data + source_col.offset;
                if (agg.type == AGG_SUM) {
                    if (source_col.type == TYPE_FLOAT) {
                        numeric_sums[i] += *reinterpret_cast<const float *>(data);
                    } else if (source_col.type == TYPE_INT) {
                        numeric_sums[i] += *reinterpret_cast<const int32_t *>(data);
                    } else if (source_col.type == TYPE_BIGINT) {
                        numeric_sums[i] += *reinterpret_cast<const int64_t *>(data);
                    } else {
                        throw IncompatibleTypeError("SUM", coltype2str(source_col.type));
                    }
                    continue;
                }

                if (!initialized[i]) {
                    best_values[i].assign(data, data + source_col.len);
                    initialized[i] = true;
                    continue;
                }

                int cmp = compare_raw_data(data, source_col.type, source_col.len, best_values[i].data(),
                                           source_col.type, source_col.len);
                if ((agg.type == AGG_MAX && cmp > 0) || (agg.type == AGG_MIN && cmp < 0)) {
                    best_values[i].assign(data, data + source_col.len);
                }
            }
        }

        for (size_t i = 0; i < aggregates_.size(); ++i) {
            const auto &agg = aggregates_[i];
            const auto &out_col = cols_[i];
            char *dest = result_->data + out_col.offset;
            switch (agg.type) {
                case AGG_COUNT: {
                    int64_t value = count_values[i];
                    memcpy(dest, &value, sizeof(value));
                    break;
                }
                case AGG_SUM: {
                    if (out_col.type == TYPE_FLOAT) {
                        float value = static_cast<float>(numeric_sums[i]);
                        memcpy(dest, &value, sizeof(value));
                    } else {
                        int64_t value = static_cast<int64_t>(numeric_sums[i]);
                        memcpy(dest, &value, sizeof(value));
                    }
                    break;
                }
                case AGG_MAX:
                case AGG_MIN:
                    if (initialized[i]) {
                        memcpy(dest, best_values[i].data(), out_col.len);
                    } else {
                        memset(dest, 0, out_col.len);
                    }
                    break;
            }
        }

        is_end_ = false;
    }

    void nextTuple() override { is_end_ = true; }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end_) {
            return nullptr;
        }
        return std::make_unique<RmRecord>(*result_);
    }

    bool is_end() const override { return is_end_; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "AggregateExecutor"; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    Rid &rid() override { return _abstract_rid; }
};
