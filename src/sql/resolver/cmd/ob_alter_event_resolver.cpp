/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_alter_event_resolver.h"
#include "sql/resolver/cmd/ob_cmd_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace sql
{
ObAlterEventResolver::ObAlterEventResolver(ObResolverParams &params)
    : ObCMDResolver(params)
{
}

int ObAlterEventResolver::resolve(const ParseNode &parse_tree)
{

  int ret = OB_SUCCESS;
  CHECK_COMPATIBILITY_MODE(session_info_);
  ObAlterEventStmt *alter_event_stmt = NULL;
  if ((7 != parse_tree.num_child_) || T_EVENT_JOB_ALTER != parse_tree.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("has 7 child",
             "actual_num", parse_tree.num_child_,
             "type", parse_tree.type_,
             K(ret));
  } else if (OB_ISNULL(params_.session_info_) || OB_ISNULL(params_.schema_checker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Session info should not be NULL", K(ret), KP(params_.session_info_),
             KP(params_.schema_checker_));
  } else if (OB_ISNULL(alter_event_stmt = create_stmt<ObAlterEventStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to alter ObAlterEventStmt", K(ret));
	} else {
    stmt_ = alter_event_stmt;
    const uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
    alter_event_stmt->set_tenant_id(tenant_id);
    if (OB_FAIL(resolve_alter_event_(&parse_tree))) {
      LOG_WARN("Failed to reslove alter event", K(ret));
    }
  }
  return ret;
}

int ObAlterEventResolver::resolve_alter_event_(const ParseNode *alter_event_node)
{
  int ret = OB_SUCCESS;

  const ParseNode *definer_node             = alter_event_node->children_[0];
  const ParseNode *sp_name_node             = alter_event_node->children_[1];
  const ParseNode *schedule_and_comple_node = alter_event_node->children_[2];
  const ParseNode *event_rename_node        = alter_event_node->children_[3];
  const ParseNode *event_enable_node        = alter_event_node->children_[4];
  const ParseNode *event_comment_node       = alter_event_node->children_[5];
  const ParseNode *event_body_node          = alter_event_node->children_[6];

  ObAlterEventStmt &stmt = *(static_cast<ObAlterEventStmt *>(stmt_));
  if (OB_NOT_NULL(definer_node)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support definer", K(ret), KP(definer_node));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter definer");
  } else if (OB_ISNULL(sp_name_node) || T_SP_NAME != sp_name_node->type_ || 2 != sp_name_node->num_child_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("alter event need event name node", K(ret), KP(alter_event_node));
  } else {
    ObString db_name, event_name;
    if (OB_FAIL(ObResolverUtils::resolve_sp_name(*session_info_, *sp_name_node, db_name, event_name))) {
      LOG_WARN("get sp name failed", K(ret), KP(sp_name_node));
    } else if (0 != db_name.compare(session_info_->get_database_name())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter event in non-current database");
      LOG_WARN("db name need equal", K(ret), K(db_name), K(session_info_->get_database_name()));
    } else {
      stmt.set_event_database(db_name);
      char *event_name_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_NAME_MAX_LENGTH));
      if (OB_ISNULL(event_name_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("faild to alloc memory for event name", K(ret));
      } else {
        memset(event_name_buf, 0, OB_EVENT_NAME_MAX_LENGTH);
        snprintf(event_name_buf, OB_EVENT_NAME_MAX_LENGTH, "%lu.%s", session_info_->get_database_id(), event_name.ptr());
        stmt.set_event_name(event_name_buf);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(schedule_and_comple_node)) {
        stmt.set_start_time(OB_INVALID_TIMESTAMP);
        stmt.set_auto_drop(-1);
      } else if (2 <= schedule_and_comple_node->num_child_) {
        const ParseNode *event_schedule_node    = schedule_and_comple_node->children_[0];
        const ParseNode *event_preserve_node    = schedule_and_comple_node->children_[1];
        if (OB_ISNULL(event_schedule_node)) {
          stmt.set_start_time(OB_INVALID_TIMESTAMP);
        } else {
          //from now
          if (0 == event_schedule_node->value_) {
            stmt.set_start_time(ObTimeUtility::current_time());
            stmt.set_end_time(64060560000000000);
            stmt.set_repeat_ts(0);
          //from at time
          } else if (1 == event_schedule_node->value_) {
            int64_t start_time_us = OB_INVALID_TIMESTAMP;
            stmt.set_repeat_ts(0);
            if (OB_FAIL(get_time_us_(event_schedule_node, start_time_us))) {
              LOG_WARN("alter event get time str failed", K(ret), KP(event_schedule_node));
            } else if (start_time_us < ObTimeUtility::current_time()) {
              ret = OB_ERR_EVENT_CANNOT_ALTER_IN_THE_PAST;
              LOG_WARN("alter event get time str failed", K(ret), K(start_time_us), K(ObTimeUtility::current_time()));
            } else {
              stmt.set_start_time(start_time_us);
              stmt.set_end_time(64060560000000000);
            }
          //repeat
          } else if (2 == event_schedule_node->value_ && 3 <= event_schedule_node->num_child_) {
            const ParseNode *repeat_num_node = event_schedule_node->children_[0];
            const ParseNode *repeat_type_node = event_schedule_node->children_[1];
            const ParseNode *range_node = event_schedule_node->children_[2];
            char *repeat_interval = static_cast<char*>(allocator_->alloc(OB_EVENT_REPEAT_MAX_LENGTH));
            if (OB_ISNULL(repeat_interval)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("faild to alloc memory", K(ret));
            } else {
              memset(repeat_interval, 0, OB_EVENT_REPEAT_MAX_LENGTH);
              int64_t repeat_ts = 0;
              if (OB_FAIL(get_repeat_interval_(repeat_num_node, repeat_type_node, repeat_interval, repeat_ts))){
                LOG_WARN("alter event get repeat interval str failed", K(ret), KP(event_schedule_node));
              } else {
                stmt.set_repeat_ts(repeat_ts);
                stmt.set_repeat_interval(repeat_interval);
                if (OB_ISNULL(range_node)) {
                  stmt.set_start_time(ObTimeUtility::current_time());
                  stmt.set_end_time(64060560000000000);
                } else if (2 == range_node->num_child_){
                  const ParseNode *start_time_node = range_node->children_[0];
                  const ParseNode *end_time_node = range_node->children_[1];
                  if (OB_NOT_NULL(start_time_node)) {
                    int64_t start_time_us = OB_INVALID_TIMESTAMP;
                    if (OB_FAIL(get_time_us_(start_time_node, start_time_us))) {
                      LOG_WARN("alter event get time us failed", K(ret), KP(start_time_node));
                    } else if (start_time_us < ObTimeUtility::current_time()) {
                      ret = OB_ERR_EVENT_CANNOT_ALTER_IN_THE_PAST;
                      LOG_WARN("alter event get time str failed", K(ret), K(start_time_us), K(ObTimeUtility::current_time()));
                    } {
                      stmt.set_start_time(start_time_us);
                    }
                  } else {
                    stmt.set_start_time(ObTimeUtility::current_time());
                  }
                  if (OB_NOT_NULL(end_time_node)) {
                    int64_t end_time_us = OB_INVALID_TIMESTAMP;
                    if (OB_FAIL(get_time_us_(end_time_node, end_time_us))) {
                      LOG_WARN("alter event get time str failed", K(ret), KP(end_time_node));
                    } else {
                      stmt.set_end_time(end_time_us);
                    }
                  } else {
                    stmt.set_end_time(64060560000000000);
                  }
                }
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_INVALID_TIMESTAMP != stmt.get_end_time() && stmt.get_end_time() < stmt.get_start_time()) {
          ret = OB_ERR_EVENT_ENDS_BEFORE_STARTS;
          LOG_WARN("ends before starts", K(ret), K(stmt.get_end_time()), K(stmt.get_start_time()));
        }
        //Only when specified as "preserve" will it be false.
        if (OB_ISNULL(event_preserve_node)) {
          stmt.set_auto_drop(-1);
        } else if (T_IDENT == event_preserve_node->type_ && 1 == event_preserve_node->value_) {
          stmt.set_auto_drop(0);
        } else if (T_IDENT == event_preserve_node->type_ && 0 == event_preserve_node->value_){
          stmt.set_auto_drop(1);
        }
      }
    }


    //Only when specified as "disable" will it be false.
    if (OB_ISNULL(event_enable_node)) {
      stmt.set_is_enable(-1);
    } else if (T_IDENT == event_enable_node->type_ && 0 == event_enable_node->value_) {
      stmt.set_is_enable(0);
    } else if (T_IDENT == event_enable_node->type_ && 1 == event_enable_node->value_){
      stmt.set_is_enable(1);
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(event_rename_node)) {
      char *event_name_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_NAME_MAX_LENGTH));
      if (OB_ISNULL(event_name_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("faild to alloc memory for event name", K(ret));
      } else {
        memset(event_name_buf, 0, OB_EVENT_NAME_MAX_LENGTH);
        const ParseNode *database_node = event_rename_node->children_[0];
        const ParseNode *rename_node = event_rename_node->children_[1];
        if (OB_NOT_NULL(database_node)) {
          ObString data_base(database_node->str_len_, database_node->str_value_);
          if (0 != data_base.compare(session_info_->get_database_name())) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter event name in non-current database");
            LOG_WARN("alter event rename dabase failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          snprintf(event_name_buf, OB_EVENT_NAME_MAX_LENGTH, "%lu.%s",session_info_->get_database_id(), rename_node->str_value_);
          stmt.set_event_rename(event_name_buf);
          if (0 == stmt.get_event_name().case_compare(stmt.get_event_rename())) {
            ret = OB_ERR_EVENT_SAME_NAME;
          }
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(event_comment_node)) {
      char *event_comment_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_COMMENT_MAX_LENGTH));
      if (OB_ISNULL(event_comment_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        memset(event_comment_buf, 0, OB_EVENT_COMMENT_MAX_LENGTH);
        snprintf(event_comment_buf, OB_EVENT_COMMENT_MAX_LENGTH, "%s", event_comment_node->str_value_);
        stmt.set_event_comment(event_comment_buf);
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(event_body_node)) {
      char *event_body_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_BODY_MAX_LENGTH));
      char *sql_buf = static_cast<char*>(allocator_->alloc(OB_EVENT_SQL_MAX_LENGTH));
      if (OB_ISNULL(event_body_buf) || OB_ISNULL(sql_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        memset(event_body_buf, 0, OB_EVENT_BODY_MAX_LENGTH);
        int available_space = OB_EVENT_BODY_MAX_LENGTH;
        for (int sql_index = 0; sql_index <  event_body_node->num_child_ && OB_SUCC(ret); sql_index++) {
          if (OB_EVENT_SQL_MAX_LENGTH - 1 < event_body_node->children_[sql_index]->str_len_) {
            ret = OB_ERR_EVENT_DATA_TOO_LONG;
            ObString error_string("event body single SQL");
            LOG_USER_ERROR(OB_ERR_EVENT_DATA_TOO_LONG, error_string.length(), error_string.ptr());
            LOG_WARN("single sql too long", K(ret), K(event_body_node->children_[sql_index]->str_len_), K(sql_index));
          } else {
            memset(sql_buf, 0, OB_EVENT_SQL_MAX_LENGTH);
            snprintf(sql_buf, OB_EVENT_SQL_MAX_LENGTH, "%.*s;", (int)event_body_node->children_[sql_index]->str_len_, event_body_node->children_[sql_index]->str_value_);
            if (event_body_node->children_[sql_index]->str_len_ + 1 > available_space) {
              ret = OB_ERR_EVENT_DATA_TOO_LONG;
              ObString error_string("event body");
              LOG_USER_ERROR(OB_ERR_EVENT_DATA_TOO_LONG, error_string.length(), error_string.ptr());
              LOG_WARN("out of max length", K(ret), K(event_body_node->children_[sql_index]->str_len_), K(available_space));
            } else {
              strncat(event_body_buf, sql_buf, available_space);
              available_space = available_space - (event_body_node->children_[sql_index]->str_len_ + 1);
            }
          }
        }
        if (OB_SUCC(ret)) {
          stmt.set_event_body(event_body_buf);
        }
      }
    }
  }
  return ret;
}


int ObAlterEventResolver::get_time_us_(const ParseNode *time_node, int64_t &time_us)
{
  int ret = OB_SUCCESS;
  if (time_node->num_child_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("has 2 child",
             "actual_num", time_node->num_child_,
             "type", time_node->type_,
             K(ret));
  } else {

    ObSqlString sql;
    int64_t base_time = OB_INVALID_TIMESTAMP;
    const ParseNode *start_time_node = time_node->children_[0];
    const ParseNode *interval_node = time_node->children_[1];
    char format[] = "%Y-%m-%d %H:%M:%S";
    struct tm timeStruct;
    if (OB_ISNULL(start_time_node)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("time node is null", K(ret));
    } else if (NULL == strptime(start_time_node->str_value_, format, &timeStruct)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "date formats other than \"%Y-%m-%d %H:%M:%S\"");
      LOG_WARN("time format error", K(ret));
    } else if (OB_FAIL(sql.assign_fmt("select TIME_TO_USEC (\'%.*s\') as time", (int)start_time_node->str_len_, start_time_node->str_value_))){
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("time node is not vaild", K(ret));
    } else if (OB_FAIL(get_time_us_from_sql_(sql.ptr(), base_time))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("time str is not vaild", K(ret));
    } else if (OB_ISNULL(interval_node)) {
      time_us = base_time;
    } else {
      if (2 != interval_node->num_child_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("interval node has 2 child",
                "actual_num", interval_node->num_child_,
                "type", interval_node->type_,
                K(ret));
      } else if (OB_ISNULL(interval_node->children_[0]) || OB_ISNULL(interval_node->children_[1])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("interval node is not null",
                "actual_num", interval_node->num_child_,
                "type", interval_node->type_,
                K(ret));
      } else {
        ObDateSqlMode date_sql_mode;
        date_sql_mode.init(session_info_->get_sql_mode());
        const char *date_unit_interval = interval_node->children_[0]->str_value_;
        ObDateUnitType date_unit_type = (ObDateUnitType)interval_node->children_[1]->value_;
        if (OB_FAIL(ObTimeConverter::date_adjust(base_time, date_unit_interval, date_unit_type, time_us, true, date_sql_mode))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("time str add interval failed", K(ret), K(start_time_node), K(interval_node));
        }
      }
    }
  }
  return ret;
}

int ObAlterEventResolver::get_repeat_interval_(const ParseNode *repeat_num_node, const ParseNode *repeat_type_node, char *repeat_interval_str, int64_t &repeat_ts) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(repeat_num_node) || OB_ISNULL(repeat_type_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("interval node is null",
             K(ret), KP(repeat_num_node), KP(repeat_type_node));
  } else {
    int64_t date_unit_interval = repeat_num_node->value_;
    const char *date_unit_type = repeat_type_node->str_value_;
    if (0 == date_unit_interval || OB_EVEX_MAX_INTERVAL_VALUE < date_unit_interval) {
      ret = OB_ERR_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG;
      LOG_WARN("date_unit_interval error",
             K(ret), K(date_unit_interval));
    } else if (OB_ISNULL(date_unit_type)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("interval type is null",
             K(ret), KP(date_unit_type));
    } else {
      const char *date_unit_str = NULL;
      if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_YEAR)))) {
        repeat_ts = date_unit_interval * 12 * 30 * 24 * 60 * 60 * 1000000;
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_YEAR);
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_MONTH)))) {
        repeat_ts = date_unit_interval * 30 * 24 * 60 * 60 * 1000000;
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_MONTH);
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_DAY)))) {
        repeat_ts = date_unit_interval * 24 * 60 * 60 * 1000000;
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_DAY);
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_HOUR)))) {
        repeat_ts = date_unit_interval * 60 * 60 * 1000000;
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_HOUR);
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_MINUTE)))) {
        repeat_ts = date_unit_interval * 60 * 1000000;
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_MINUTE);
      } else if (OB_NOT_NULL(strcasestr(date_unit_type, ob_date_unit_type_str(DATE_UNIT_SECOND)))) {
        repeat_ts = date_unit_interval * 1000000;
        date_unit_str = ob_date_unit_type_str_upper(DATE_UNIT_SECOND);
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(date_unit_str)) {
        snprintf(repeat_interval_str, OB_EVENT_REPEAT_MAX_LENGTH, "FREQ=%.*sLY; INTERVAL=%ld", (int)repeat_type_node->str_len_, date_unit_str, date_unit_interval);
      }
    }
  }
  return ret;
}

int ObAlterEventResolver::get_time_us_from_sql_(const char *sql, int64_t &time_us)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("execute query failed", K(ret));
  }
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(GCTX.sql_proxy_->read(result, session_info_->get_effective_tenant_id(), sql))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("execute query failed", K(ret));
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get result", K(ret));
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      if (OB_SUCC(ret) && OB_SUCC(res.next())) {
        EXTRACT_INT_FIELD_MYSQL(res, "time", time_us, uint64_t);
      }
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
