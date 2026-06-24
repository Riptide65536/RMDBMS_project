/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

#include <limits>

namespace {

bool is_compatible_type(ColType lhs_type, ColType rhs_type) {
    if (lhs_type == rhs_type) {
        return true;
    }
    if ((lhs_type == TYPE_DATETIME && rhs_type == TYPE_STRING) ||
        (lhs_type == TYPE_STRING && rhs_type == TYPE_DATETIME)) {
        return true;
    }
    bool lhs_numeric = lhs_type == TYPE_INT || lhs_type == TYPE_BIGINT || lhs_type == TYPE_FLOAT;
    bool rhs_numeric = rhs_type == TYPE_INT || rhs_type == TYPE_BIGINT || rhs_type == TYPE_FLOAT;
    return lhs_numeric && rhs_numeric;
}

bool is_sum_supported_type(ColType type) {
    return type == TYPE_INT || type == TYPE_BIGINT || type == TYPE_FLOAT;
}

std::string default_aggregate_alias(AggType type, const TabCol &col, bool is_count_star) {
    switch (type) {
        case AGG_COUNT:
            return is_count_star ? "COUNT(*)" : "COUNT(" + col.col_name + ")";
        case AGG_MAX:
            return "MAX(" + col.col_name + ")";
        case AGG_MIN:
            return "MIN(" + col.col_name + ")";
        case AGG_SUM:
            return "SUM(" + col.col_name + ")";
        default:
            throw InternalError("Unexpected aggregate type");
    }
}

Value cast_value_to_type(Value value, ColType target_type) {
    if (value.type == target_type) {
        return value;
    }
    if (target_type == TYPE_DATETIME) {
        if (value.type != TYPE_STRING) {
            throw IncompatibleTypeError(coltype2str(target_type), coltype2str(value.type));
        }
        value.set_datetime(parse_datetime_string(value.str_val));
        return value;
    }
    if (!is_compatible_type(value.type, target_type)) {
        throw IncompatibleTypeError(coltype2str(target_type), coltype2str(value.type));
    }

    long double numeric = 0;
    if (value.type == TYPE_INT) {
        numeric = value.int_val;
    } else if (value.type == TYPE_BIGINT) {
        numeric = value.bigint_val;
    } else {
        numeric = value.float_val;
    }

    if (target_type == TYPE_FLOAT) {
        value.set_float(static_cast<float>(numeric));
    } else if (target_type == TYPE_INT) {
        if (numeric < std::numeric_limits<int32_t>::min() || numeric > std::numeric_limits<int32_t>::max()) {
            throw NumericOverflowError(coltype2str(target_type));
        }
        value.set_int(static_cast<int32_t>(numeric));
    } else if (target_type == TYPE_BIGINT) {
        if (numeric < std::numeric_limits<int64_t>::min() || numeric > std::numeric_limits<int64_t>::max()) {
            throw NumericOverflowError(coltype2str(target_type));
        }
        value.set_bigint(static_cast<int64_t>(numeric));
    } else {
        throw IncompatibleTypeError(coltype2str(target_type), coltype2str(value.type));
    }
    return value;
}

}  // namespace

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse))
    {
        // 处理表名
        query->tables = std::move(x->tabs);
        for (auto &tab_name : query->tables) {
            sm_manager_->db_.get_table(tab_name);
        }

        std::vector<ColMeta> all_cols;
        get_all_cols(query->tables, all_cols);

        if (x->has_aggregate) {
            query->has_aggregate = true;
            for (const auto &sv_agg : x->aggs) {
                AggregateExpr agg;
                agg.type = convert_sv_agg_type(sv_agg->agg_type);
                agg.is_count_star = sv_agg->is_count_star;
                if (!sv_agg->is_count_star) {
                    agg.col = {.tab_name = sv_agg->col->tab_name, .col_name = sv_agg->col->col_name};
                    agg.col = check_column(all_cols, agg.col);
                    auto col_meta = *sm_manager_->db_.get_table(agg.col.tab_name).get_col(agg.col.col_name);
                    if (agg.type == AGG_SUM && !is_sum_supported_type(col_meta.type)) {
                        throw IncompatibleTypeError("SUM", coltype2str(col_meta.type));
                    }
                }
                agg.alias = sv_agg->alias.empty() ? default_aggregate_alias(agg.type, agg.col, agg.is_count_star)
                                                  : sv_agg->alias;
                query->aggregates.push_back(agg);
                query->cols.push_back({.tab_name = "", .col_name = agg.alias});
            }
        } else {
            // 处理target list，再target list中添加上表名，例如 a.id
            for (auto &sv_sel_col : x->cols) {
                TabCol sel_col = {.tab_name = sv_sel_col->tab_name, .col_name = sv_sel_col->col_name};
                query->cols.push_back(sel_col);
            }
            if (query->cols.empty()) {
                for (auto &col : all_cols) {
                    TabCol sel_col = {.tab_name = col.tab_name, .col_name = col.name};
                    query->cols.push_back(sel_col);
                }
            } else {
                for (auto &sel_col : query->cols) {
                    sel_col = check_column(all_cols, sel_col);
                }
            }
        }
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        sm_manager_->db_.get_table(x->tab_name);
        query->tables = {x->tab_name};

        for (auto &sv_set_clause : x->set_clauses) {
            SetClause set_clause;
            set_clause.lhs = {.tab_name = x->tab_name, .col_name = sv_set_clause->col_name};
            std::vector<ColMeta> all_cols;
            get_all_cols(query->tables, all_cols);
            set_clause.lhs = check_column(all_cols, set_clause.lhs);
            set_clause.rhs = convert_sv_value(sv_set_clause->val);

            auto &tab = sm_manager_->db_.get_table(x->tab_name);
            auto col = tab.get_col(set_clause.lhs.col_name);
            set_clause.rhs = cast_value_to_type(set_clause.rhs, col->type);
            query->set_clauses.push_back(set_clause);
        }

        get_clause(x->conds, query->conds);
        check_clause(query->tables, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        sm_manager_->db_.get_table(x->tab_name);
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        sm_manager_->db_.get_table(x->tab_name);
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}


TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        auto pos = std::find_if(all_cols.begin(), all_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == all_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        }
        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val = cast_value_to_type(cond.rhs_val, lhs_type);
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if (!is_compatible_type(lhs_type, rhs_type)) {
            throw IncompatibleTypeError(coltype2str(lhs_type), coltype2str(rhs_type));
        }
    }
}


Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_bigint(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}

AggType Analyze::convert_sv_agg_type(ast::AggFuncType type) {
    std::map<ast::AggFuncType, AggType> m = {
        {ast::AggFunc_COUNT, AGG_COUNT},
        {ast::AggFunc_MAX, AGG_MAX},
        {ast::AggFunc_MIN, AGG_MIN},
        {ast::AggFunc_SUM, AGG_SUM},
    };
    return m.at(type);
}
