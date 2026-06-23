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
#include <cstring>
#include <vector>

#include "common/common.h"
#include "errors.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

inline bool is_numeric_type(ColType type) {
    return type == TYPE_INT || type == TYPE_FLOAT;
}

inline Value cast_value_to_col_type(Value value, const ColMeta &col) {
    if (value.type == col.type) {
        return value;
    }
    if (!is_numeric_type(value.type) || !is_numeric_type(col.type)) {
        throw IncompatibleTypeError(coltype2str(col.type), coltype2str(value.type));
    }
    if (col.type == TYPE_FLOAT) {
        value.set_float(value.type == TYPE_INT ? static_cast<float>(value.int_val) : value.float_val);
    } else {
        value.set_int(value.type == TYPE_FLOAT ? static_cast<int>(value.float_val) : value.int_val);
    }
    return value;
}

inline const ColMeta *find_col_meta(const std::vector<ColMeta> &cols, const TabCol &target) {
    auto it = std::find_if(cols.begin(), cols.end(), [&](const ColMeta &col) {
        return col.tab_name == target.tab_name && col.name == target.col_name;
    });
    if (it == cols.end()) {
        throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
    }
    return &(*it);
}

inline int compare_raw_data(const char *lhs, ColType lhs_type, int lhs_len,
                            const char *rhs, ColType rhs_type, int rhs_len) {
    if (is_numeric_type(lhs_type) && is_numeric_type(rhs_type)) {
        double lhs_value = lhs_type == TYPE_INT ? static_cast<double>(*reinterpret_cast<const int *>(lhs))
                                                : static_cast<double>(*reinterpret_cast<const float *>(lhs));
        double rhs_value = rhs_type == TYPE_INT ? static_cast<double>(*reinterpret_cast<const int *>(rhs))
                                                : static_cast<double>(*reinterpret_cast<const float *>(rhs));
        if (lhs_value < rhs_value) {
            return -1;
        }
        if (lhs_value > rhs_value) {
            return 1;
        }
        return 0;
    }

    if (lhs_type != rhs_type) {
        throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
    }

    if (lhs_type == TYPE_STRING) {
        int common_len = std::min(lhs_len, rhs_len);
        int cmp = memcmp(lhs, rhs, common_len);
        if (cmp != 0) {
            return cmp;
        }
        if (lhs_len == rhs_len) {
            return 0;
        }
        if (lhs_len > rhs_len) {
            for (int i = common_len; i < lhs_len; ++i) {
                if (lhs[i] != '\0') {
                    return 1;
                }
            }
            return 0;
        }
        for (int i = common_len; i < rhs_len; ++i) {
            if (rhs[i] != '\0') {
                return -1;
            }
        }
        return 0;
    }

    if (lhs_type == TYPE_INT) {
        int lhs_value = *reinterpret_cast<const int *>(lhs);
        int rhs_value = *reinterpret_cast<const int *>(rhs);
        return lhs_value < rhs_value ? -1 : (lhs_value > rhs_value ? 1 : 0);
    }

    float lhs_value = *reinterpret_cast<const float *>(lhs);
    float rhs_value = *reinterpret_cast<const float *>(rhs);
    return lhs_value < rhs_value ? -1 : (lhs_value > rhs_value ? 1 : 0);
}

inline bool evaluate_compare_result(int cmp, CompOp op) {
    switch (op) {
        case OP_EQ:
            return cmp == 0;
        case OP_NE:
            return cmp != 0;
        case OP_LT:
            return cmp < 0;
        case OP_GT:
            return cmp > 0;
        case OP_LE:
            return cmp <= 0;
        case OP_GE:
            return cmp >= 0;
        default:
            throw InternalError("Unexpected comparison operator");
    }
}

inline bool record_satisfies_conditions(const RmRecord &record, const std::vector<ColMeta> &cols,
                                        const std::vector<Condition> &conds) {
    for (const auto &cond : conds) {
        const ColMeta *lhs_meta = find_col_meta(cols, cond.lhs_col);
        const char *lhs_data = record.data + lhs_meta->offset;

        const char *rhs_data = nullptr;
        ColType rhs_type;
        int rhs_len;
        if (cond.is_rhs_val) {
            if (cond.rhs_val.raw == nullptr) {
                throw InternalError("Condition raw value is not initialized");
            }
            rhs_data = cond.rhs_val.raw->data;
            rhs_type = cond.rhs_val.type;
            rhs_len = cond.rhs_val.raw->size;
        } else {
            const ColMeta *rhs_meta = find_col_meta(cols, cond.rhs_col);
            rhs_data = record.data + rhs_meta->offset;
            rhs_type = rhs_meta->type;
            rhs_len = rhs_meta->len;
        }

        int cmp = compare_raw_data(lhs_data, lhs_meta->type, lhs_meta->len, rhs_data, rhs_type, rhs_len);
        if (!evaluate_compare_result(cmp, cond.op)) {
            return false;
        }
    }
    return true;
}

inline std::vector<char> build_index_key(const IndexMeta &index, const RmRecord &record) {
    std::vector<char> key(index.col_tot_len);
    int offset = 0;
    for (const auto &col : index.cols) {
        memcpy(key.data() + offset, record.data + col.offset, col.len);
        offset += col.len;
    }
    return key;
}
