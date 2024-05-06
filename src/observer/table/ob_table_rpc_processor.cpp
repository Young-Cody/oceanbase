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

#define USING_LOG_PREFIX SERVER
#include "ob_table_rpc_processor.h"
#include "observer/ob_service.h"
#include "storage/tx_storage/ob_access_service.h"
#include "sql/ob_end_trans_callback.h"
#include "ob_table_end_trans_cb.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "ob_htable_utils.h"
#include "sql/ob_sql.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "share/schema/ob_schema_mgr.h"
#include "ob_table_rpc_processor_util.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "share/ob_define.h"
#include "storage/tx/ob_trans_service.h"
#include "ob_table_session_pool.h"
#include "storage/tx/wrs/ob_weak_read_util.h"
#include "ob_table_move_response.h"
#include "ob_table_connection_mgr.h"
#include "share/table/ob_table_util.h"
#include "observer/mysql/obmp_base.h"
#include "lib/stat/ob_session_stat.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;

int ObTableLoginP::process()
{
  int ret = OB_SUCCESS;
  const ObTableLoginRequest &login = arg_;
  if (1 != login.auth_method_
      || 1 != login.client_version_
      || 0 != login.reserved1_
      || 0 != login.reserved2_
      || 0 != login.reserved3_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid login request", K(ret), K(login));
  } else if (1 != login.client_type_ && 2 != login.client_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid client type", K(ret), K(login));
  } else if (login.tenant_name_.empty()
             || login.user_name_.empty()
             || login.database_name_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant or user or database", K(ret), K(login));
  } else if (login.pass_scramble_.length() != 20) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scramble", K(ret), K(login));
  } else if (0 > login.ttl_us_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ttl_us", K(ret), K(login));
  } else if (SS_STOPPING == GCTX.status_
             || SS_STOPPED == GCTX.status_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("server is stopping", K(GCTX.status_), K(ret), K(login));
  } else if (SS_SERVING != GCTX.status_) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("server is not serving", K(GCTX.status_), K(ret), K(login));
  } else {
    // @todo check client_capabilities and max_packet_size
    uint64_t user_token = 0;
    if (OB_FAIL(get_ids())) {
      LOG_WARN("failed to get ids", K(ret), K(login));
    } else if (OB_FAIL(verify_password(login.tenant_name_, login.user_name_,
                                       login.pass_secret_, login.pass_scramble_,
                                       login.database_name_, user_token))) {
      LOG_DEBUG("failed to verify password", K(ret), K(login));
    } else if (OB_FAIL(generate_credential(result_.tenant_id_, result_.user_id_, result_.database_id_,
                                           login.ttl_us_, user_token, result_.credential_))) {
      LOG_WARN("failed to generate credential", K(ret), K(login));
    } else {
      MTL_SWITCH(credential_.tenant_id_) {
        if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->update_sess(credential_))) {
          LOG_WARN("failed to update session pool", K(ret), K_(credential));
        }
      }
      result_.reserved1_ = 0;
      result_.reserved2_ = 0;
      result_.server_version_ = ObString::make_string(PACKAGE_STRING);
    }
  }
  // whether the client should refresh location cache
  if (OB_SUCCESS != ret && ObTableRpcProcessorUtil::is_require_rerouting_err(ret)) {
    ObRpcProcessor::require_rerouting_ = true;
    LOG_WARN("[TABLE] login require rerouting", K(ret), "require_rerouting", ObRpcProcessor::require_rerouting_);
  }
  ObTenantStatEstGuard stat_guard(result_.tenant_id_);
#ifndef NDEBUG
    LOG_INFO("[TABLE] login", K(ret), K_(arg), K_(result), "timeout", rpc_pkt_->get_timeout());
#else
    // @todo LOG_DEBUG
    LOG_INFO("[TABLE] login", K(ret), K_(arg), K_(result), "timeout", rpc_pkt_->get_timeout(),
              "receive_ts", get_receive_timestamp());
#endif

    if (common::OB_INVALID_ARGUMENT == ret) {
      RPC_OBRPC_LOG(ERROR, "yyy retcode is 4002", "pkt", req_->get_packet());
    }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = ObTableConnectionMgr::get_instance().update_table_connection(req_, result_.tenant_id_,
                result_.database_id_, result_.user_id_))) {
    LOG_WARN("fail to update table connection", K_(req), K_(result_.tenant_id),
              K_(result_.database_id), K_(result_.user_id), K(tmp_ret), K(ret));
  }
  EVENT_INC(TABLEAPI_LOGIN_COUNT);
  return ret;
}

int ObTableLoginP::get_ids()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid schema service", K(ret), K(gctx_.schema_service_));
  } else {
    share::schema::ObSchemaGetterGuard guard;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else if (OB_FAIL(guard.get_tenant_id(arg_.tenant_name_, result_.tenant_id_))) {
      if (ret == OB_ERR_INVALID_TENANT_NAME) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_USER_ERROR(OB_TENANT_NOT_EXIST, arg_.tenant_name_.length(), arg_.tenant_name_.ptr());
      }
      LOG_WARN("get_tenant_id failed", K(ret), "tenant", arg_.tenant_name_);
    } else if (OB_INVALID_ID == result_.tenant_id_) {
      ret = OB_ERR_INVALID_TENANT_NAME;
      LOG_WARN("get_tenant_id failed", K(ret), "tenant", arg_.tenant_name_);
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(result_.tenant_id_, guard))) {
      LOG_WARN("get_schema_guard failed", K(ret), "tenant_id", result_.tenant_id_);
    } else if (OB_FAIL(guard.get_database_id(result_.tenant_id_, arg_.database_name_,
                                             result_.database_id_))) {
      LOG_WARN("failed to get database id", K(ret), "database", arg_.database_name_);
    } else if (OB_INVALID_ID == result_.database_id_) {
      ret = OB_ERR_BAD_DATABASE;
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, arg_.database_name_.length(), arg_.database_name_.ptr());
      LOG_WARN("failed to get database id", K(ret), "database", arg_.database_name_);
    } else if (OB_FAIL(guard.get_user_id(result_.tenant_id_, arg_.user_name_,
                                         ObString::make_string("%")/*assume there is no specific host*/,
                                         result_.user_id_))) {
      if (ret == OB_ERR_USER_NOT_EXIST) {
        LOG_USER_ERROR(OB_ERR_USER_NOT_EXIST);
      }
      LOG_WARN("failed to get user id", K(ret), "user", arg_.user_name_);
    } else if (OB_INVALID_ID == result_.user_id_) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_USER_NOT_EXIST);
      LOG_WARN("failed to get user id", K(ret), "user", arg_.user_name_);
    }
  }
  return ret;
}

int ObTableLoginP::verify_password(const ObString &tenant, const ObString &user,
                                   const ObString &pass_secret, const ObString &pass_scramble,
                                   const ObString &db, uint64_t &user_token)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid schema service", K(ret), K(gctx_.schema_service_));
  } else {
    share::schema::ObSchemaGetterGuard guard;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    share::schema::ObUserLoginInfo login_info;
    login_info.tenant_name_ = tenant;
    login_info.user_name_ = user;
    login_info.scramble_str_ = pass_scramble;
    login_info.db_ = db;
    login_info.passwd_ = pass_secret;
    SSL *ssl_st = NULL;//TODO::@yanhua not support ssl now for table-api
    share::schema::ObSessionPrivInfo session_priv;
    const share::schema::ObUserInfo *user_info = nullptr;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
      LOG_WARN("get_schema_guard failed", K(ret));
    } else if (OB_FAIL(guard.get_tenant_id(tenant, tenant_id))) {
      LOG_WARN("fail to get tenant_id", KR(ret), K(tenant));
    } else if (gctx_.schema_service_->get_tenant_schema_guard(tenant_id, guard)) {
      LOG_WARN("fail to get tenant guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(guard.check_user_access(login_info, session_priv, ssl_st, user_info))) {
      if (ret == OB_PASSWORD_WRONG) {
        LOG_USER_ERROR(OB_PASSWORD_WRONG, user.length(), user.ptr(), tenant.length(), tenant.ptr(), "YES"/*using password*/);
      }
      LOG_WARN("User access denied", K(login_info), K(ret));
    } else if (OB_ISNULL(user_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user info is null", K(ret));
    } else {
      user_token = user_info->get_passwd_str().hash();
    }
  }
  return ret;
}

int ObTableLoginP::generate_credential(uint64_t tenant_id,
                                       uint64_t user_id,
                                       uint64_t database,
                                       int64_t ttl_us,
                                       uint64_t user_token,
                                       ObString &credential_str)
{
  int ret = OB_SUCCESS;
  credential_.cluster_id_ = GCONF.cluster_id;
  credential_.tenant_id_ = tenant_id;
  credential_.user_id_ = user_id;
  credential_.database_id_ = database;
  if (ttl_us > 0) {
    credential_.expire_ts_ = ObTimeUtility::fast_current_time() + ttl_us;
  } else {
    credential_.expire_ts_ = 0;
  }
  credential_.hash(credential_.hash_val_, user_token);
  int64_t pos = 0;
  if (OB_FAIL(serialization::encode(credential_buf_, ObTableApiCredential::CREDENTIAL_BUF_SIZE, pos, credential_))) {
    LOG_WARN("failed to serialize credential", K(ret), K(pos));
  } else {
    credential_str.assign_ptr(credential_buf_, static_cast<int32_t>(pos));
  }
  return ret;
}

////////////////////////////////////////////////////////////////
ObTableApiProcessorBase::ObTableApiProcessorBase(const ObGlobalContext &gctx)
    :gctx_(gctx),
     table_service_(gctx_.table_service_),
     access_service_(MTL(ObAccessService *)),
     location_service_(gctx.location_service_),
     sess_guard_(),
     schema_guard_(),
     simple_table_schema_(nullptr),
     req_timeinfo_guard_(),
     schema_cache_guard_(),
     stat_event_type_(-1),
     audit_row_count_(0),
     need_audit_(false),
     request_string_(NULL),
     request_string_len_(0),
     need_retry_in_queue_(false),
     is_tablegroup_req_(false),
     retry_count_(0),
     user_client_addr_(),
     sess_stat_guard_(MTL_ID(), ObActiveSessionGuard::get_stat().session_id_)
{
  need_audit_ = GCONF.enable_sql_audit;
}

void ObTableApiProcessorBase::reset_ctx()
{
  trans_param_.reset();
  schema_guard_.reset();
  schema_cache_guard_.reset();
  simple_table_schema_ = nullptr;
}

/// Get all table schemas based on the tablegroup name
/// Since we only have one table in tablegroup, we could considered tableID as the target table now
int ObTableApiProcessorBase::init_tablegroup_schema(const ObString &arg_tablegroup_name)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = OB_INVALID_ID;
  ObSEArray<const schema::ObSimpleTableSchemaV2*, 8> table_schemas;
  if (OB_FAIL(schema_guard_.get_tablegroup_id(credential_.tenant_id_, arg_tablegroup_name, tablegroup_id))) {
    LOG_WARN("fail to get tablegroup id", K(ret), K(credential_.tenant_id_),
              K(credential_.database_id_), K(arg_tablegroup_name));
  } else if (OB_FAIL(schema_guard_.get_table_schemas_in_tablegroup(credential_.tenant_id_, tablegroup_id, table_schemas))) {
    LOG_WARN("fail to get table schema from table group", K(ret), K(credential_.tenant_id_),
              K(credential_.database_id_), K(arg_tablegroup_name), K(tablegroup_id));
  } else {
    if (table_schemas.count() != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "each Table has not one Family currently");
      LOG_WARN("number of table in table gourp must be equal to one now", K(arg_tablegroup_name), K(table_schemas.count()), K(ret));
    } else {
      simple_table_schema_ = table_schemas.at(0);
    }
  }
  return ret;
}

int ObTableApiProcessorBase::init_schema_info(const ObString &arg_table_name)
{
  int ret = OB_SUCCESS;
  if (schema_cache_guard_.is_inited()) {
    // skip and do nothing
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential_.tenant_id_));
  } else if (is_tablegroup_req_ && OB_FAIL(init_tablegroup_schema(arg_table_name))) {
    LOG_WARN("fail to get table schema from table group name", K(ret), K(credential_.tenant_id_),
              K(credential_.database_id_), K(arg_table_name));
  } else if (!is_tablegroup_req_
             && OB_FAIL(schema_guard_.get_simple_table_schema(credential_.tenant_id_,
                                                              credential_.database_id_,
                                                              arg_table_name,
                                                              false, /* is_index */
                                                              simple_table_schema_))) {
    LOG_WARN("fail to get table schema", K(ret), K(credential_.tenant_id_),
              K(credential_.database_id_), K(arg_table_name));
  } else if (OB_ISNULL(simple_table_schema_) || simple_table_schema_->get_table_id() == OB_INVALID_ID) {
    ret = OB_ERR_UNKNOWN_TABLE;
    ObString db("");
    LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, arg_table_name.length(), arg_table_name.ptr(), db.length(), db.ptr());
    LOG_WARN("table not exist", K(ret), K(credential_.tenant_id_), K(credential_.database_id_), K(arg_table_name));
  } else if (OB_FAIL(schema_cache_guard_.init(credential_.tenant_id_,
                                              simple_table_schema_->get_table_id(),
                                              simple_table_schema_->get_schema_version(),
                                              schema_guard_))) {
    LOG_WARN("fail to init schema cache guard", K(ret));
  }
  return ret;
}

int ObTableApiProcessorBase::init_schema_info(uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (schema_cache_guard_.is_inited()) {
    // skip and do nothing
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard_))) {
    LOG_WARN("fail to get schema guard", K(ret), K(credential_.tenant_id_));
  } else if (OB_FAIL(schema_guard_.get_simple_table_schema(credential_.tenant_id_, table_id, simple_table_schema_))) {
    LOG_WARN("fail to get table schema", K(ret), K(credential_.tenant_id_), K(table_id));
  } else if (OB_ISNULL(simple_table_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(credential_), K(table_id));
  } else if (OB_FAIL(schema_cache_guard_.init(credential_.tenant_id_,
                                              simple_table_schema_->get_table_id(),
                                              simple_table_schema_->get_schema_version(),
                                              schema_guard_))) {
    LOG_WARN("fail to init schema cache guard", K(ret));
  }
  return ret;
}

int ObTableApiProcessorBase::get_ls_id(const ObTabletID &tablet_id, ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  int64_t expire_renew_time = 0; // not refresh ls location cache
  if (OB_FAIL(location_service_->get(MTL_ID(), tablet_id, expire_renew_time, is_cache_hit, ls_id))) {
    LOG_WARN("failed to get ls id", K(ret), K(is_cache_hit));
  }
  return ret;
}

int ObTableApiProcessorBase::check_user_access(const ObString &credential_str)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObTableApiCredential *sess_credetial = nullptr;
  if (OB_FAIL(serialization::decode(credential_str.ptr(), credential_str.length(), pos, credential_))) {
    LOG_WARN("failed to serialize credential", K(ret), K(pos));
  } else if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->get_sess_info(credential_, sess_guard_))) {
    LOG_WARN("fail to get session info", K(ret), K_(credential));
  } else if (OB_FAIL(sess_guard_.get_credential(sess_credetial))) {
    LOG_WARN("fail to get credential", K(ret));
  } else if (sess_credetial->hash_val_ != credential_.hash_val_) {
    ret = OB_KV_CREDENTIAL_NOT_MATCH;
    char user_cred_info[128];
    char sess_cred_info[128];
    int user_len = credential_.to_string(user_cred_info, 128);
    int sess_len = sess_credetial->to_string(sess_cred_info, 128);
    LOG_USER_ERROR(OB_KV_CREDENTIAL_NOT_MATCH, user_len, user_cred_info, sess_len, sess_cred_info);
    LOG_WARN("invalid credential", K(ret), K_(credential), K(*sess_credetial));
  } else if (sess_credetial->cluster_id_ != credential_.cluster_id_) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("invalid credential cluster id", K(ret), K_(credential), K(*sess_credetial));
  } else {
    LOG_DEBUG("user can access", K_(credential));
  }
  return ret;
}

oceanbase::sql::ObSQLSessionInfo &session()
{
  static oceanbase::sql::ObSQLSessionInfo SESSION;
  return SESSION;
}

ObArenaAllocator &session_alloc()
{
  static ObArenaAllocator SESSION_ALLOC;
  return SESSION_ALLOC;
}

int ObTableApiProcessorBase::init_session()
{
  int ret = OB_SUCCESS;
  static const uint32_t sess_version = 0;
  static const uint32_t sess_id = 1;
  static const uint64_t proxy_sess_id = 1;

  // ensure allocator is constructed before session to
  // avoid coredump at observer exit
  //
  ObArenaAllocator *allocator = &session_alloc();
  oceanbase::sql::ObSQLSessionInfo &sess = session();

  if (OB_FAIL(sess.test_init(sess_version, sess_id, proxy_sess_id, allocator))) {
    LOG_WARN("init session failed", K(ret));
  } else if (OB_FAIL(sess.load_default_sys_variable(false, true))) {
    LOG_WARN("failed to load default sys var", K(ret));
  }
  return ret;
}

int ObTableApiProcessorBase::get_table_id(
    const ObString &table_name,
    const uint64_t arg_table_id,
    uint64_t &real_table_id) const
{
  int ret = OB_SUCCESS;
  real_table_id = arg_table_id;
  if (OB_INVALID_ID == real_table_id || 0 == real_table_id) {
    share::schema::ObSchemaGetterGuard schema_guard;
    const bool is_index = false;
    const int64_t tenant_id = credential_.tenant_id_;
    const int64_t database_id = credential_.database_id_;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_id(tenant_id, database_id, table_name,
        is_index, share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, real_table_id))) {
      LOG_WARN("failed to get table id", K(ret), K(tenant_id), K(database_id), K(table_name));
    } else if (OB_INVALID_ID == real_table_id) {
      ret = OB_ERR_UNKNOWN_TABLE;
      LOG_WARN("get invalid id", K(ret), K(tenant_id), K(database_id), K(table_name));
    }
  }
  return ret;
}

// transaction control
int ObTableApiProcessorBase::get_tablet_by_rowkey(uint64_t table_id, const ObIArray<ObRowkey> &rowkeys,
                                                  ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjectID, 1> part_ids;
  share::schema::ObSchemaGetterGuard schema_guard;
  SMART_VAR(sql::ObTableLocation, location_calc) {
    const uint64_t tenant_id = MTL_ID();
    const ObTableSchema *table_schema;
    if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(table_id));
    } else if (!table_schema->is_partitioned_table()) {
      tablet_ids.push_back(table_schema->get_tablet_id());
    } else if (OB_FAIL(location_calc.calculate_partition_ids_by_rowkey(
                           session(), schema_guard, table_id, rowkeys, tablet_ids, part_ids))) {
      LOG_WARN("failed to calc partition id", K(ret));
    } else {}
  }
  return ret;
}

int ObTableApiProcessorBase::start_trans(bool is_readonly,
                                         const ObTableConsistencyLevel consistency_level,
                                         const ObLSID &ls_id,
                                         int64_t timeout_ts,
                                         bool need_global_snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_param_.init(is_readonly,
                                consistency_level,
                                ls_id,
                                timeout_ts,
                                need_global_snapshot))) {
    LOG_WARN("fail to init trans param", K(ret));
  } else if (OB_FAIL(ObTableTransUtils::start_trans(trans_param_))) {
    LOG_WARN("fail to start trans", K(ret), K_(trans_param));
  }
  return ret;
}

int ObTableApiProcessorBase::end_trans(bool is_rollback,
                                       rpc::ObRequest *req,
                                       ObTableCreateCbFunctor *functor,
                                       bool use_sync /* =false */)
{
  int ret = OB_SUCCESS;
  trans_param_.is_rollback_ = is_rollback;
  trans_param_.req_ = req;
  trans_param_.use_sync_ = use_sync;
  trans_param_.create_cb_functor_ = functor;
  if (OB_FAIL(ObTableTransUtils::end_trans(trans_param_))) {
    LOG_WARN("fail to end trans", K(ret), K_(trans_param));
  }
  if (trans_param_.did_async_commit_) {
    // @note the req_ may be freed, req_processor can not be read any more.
    // The req_has_wokenup_ MUST set to be true, otherwise req_processor will invoke req_->set_process_start_end_diff, cause memory core
    // @see ObReqProcessor::run() req_->set_process_start_end_diff(ObTimeUtility::current_time());
    this->set_req_has_wokenup();
  }
  return ret;
}

bool ObTableApiProcessorBase::need_audit() const
{
  return need_audit_;
}

void ObTableApiProcessorBase::start_audit(const rpc::ObRequest *req)
{

  if (OB_LIKELY(NULL != req)) {
    audit_record_.user_client_addr_ = ObCurTraceId::get_addr();
    audit_record_.client_addr_ = RPC_REQ_OP.get_peer(req);

    audit_record_.trace_id_ = req->get_trace_id();

    save_request_string();
    generate_sql_id();
  }
}

static int set_audit_name(const char *info_name, char *&audit_name, int64_t &audit_name_length, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info_name)) {
    audit_name = NULL;
    audit_name_length = 0;
  } else {
    const int64_t name_length = strlen(info_name);
    const int64_t buf_size = name_length + 1;
    char *buf = reinterpret_cast<char *>(allocator.alloc(buf_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc memory", K(ret), K(buf_size));
    } else {
      strcpy(buf, info_name);
      audit_name = buf;
      audit_name_length = name_length;
    }
  }
  return ret;
}

void ObTableApiProcessorBase::end_audit()
{
  // credential info
  audit_record_.tenant_id_ = credential_.tenant_id_;
  audit_record_.effective_tenant_id_ = credential_.tenant_id_;
  audit_record_.user_id_ = credential_.user_id_;
  audit_record_.db_id_ = credential_.database_id_;

  // update tenant_name, user_name, database_name
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(credential_.tenant_id_, schema_guard))) {
    SERVER_LOG(WARN, "fail to get schema guard", K(ret), "tenant_id", credential_.tenant_id_);
  } else {
    { // set tenant name, ignore ret
      const share::schema::ObSimpleTenantSchema *tenant_info = NULL;
      if (OB_FAIL(schema_guard.get_tenant_info(credential_.tenant_id_, tenant_info))) {
        SERVER_LOG(WARN, "fail to get tenant info", K(ret), K(credential_.tenant_id_));
      } else if (OB_ISNULL(tenant_info)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "tenant info is null", K(ret));
      } else if (OB_FAIL(set_audit_name(tenant_info->get_tenant_name(),
          audit_record_.tenant_name_, audit_record_.tenant_name_len_, audit_allocator_))){
        SERVER_LOG(WARN, "fail to set tenant name", K(ret), "tenant_name", tenant_info->get_tenant_name());
      }
    }

    { // set user name, ignore ret
      ret = OB_SUCCESS;
      const share::schema::ObUserInfo *user_info = NULL;
      if (OB_FAIL(schema_guard.get_user_info(credential_.tenant_id_, credential_.user_id_, user_info))) {
        SERVER_LOG(WARN, "fail to get user info", K(ret), K(credential_));
      } else if (OB_ISNULL(user_info)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "user info is null", K(ret));
      } else if (OB_FAIL(set_audit_name(user_info->get_user_name(),
          audit_record_.user_name_, audit_record_.user_name_len_, audit_allocator_))) {
        SERVER_LOG(WARN, "fail to set user name", K(ret), "user_name", user_info->get_user_name());
      }
    }

    { // set database name, ignore ret
      ret = OB_SUCCESS;
      const share::schema::ObSimpleDatabaseSchema *database_info = NULL;
      if (OB_FAIL(schema_guard.get_database_schema(credential_.tenant_id_, credential_.database_id_, database_info))) {
        SERVER_LOG(WARN, "fail to get database info", K(ret), K(credential_));
      } else if (OB_ISNULL(database_info)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "database info is null", K(ret));
      } else if (OB_FAIL(set_audit_name(database_info->get_database_name(),
          audit_record_.db_name_, audit_record_.db_name_len_, audit_allocator_))) {
        SERVER_LOG(WARN, "fail to set database name", K(ret), "database_name", database_info->get_database_name());
      }
    }
  }

  // append request string to query_sql
  if (NULL != request_string_ && request_string_len_ > 0) {
    static const char request_print_prefix[] = ", \nrequest: ";
    const int64_t buf_size = audit_record_.sql_len_ + sizeof(request_print_prefix) + request_string_len_;
    char *buf = reinterpret_cast<char *>(audit_allocator_.alloc(buf_size));
    if (NULL == buf) {
      SERVER_LOG(WARN, "fail to alloc audit memory", K(buf_size), K(audit_record_.sql_), K(request_string_));
    } else {
      memset(buf, 0, buf_size);
      if (OB_NOT_NULL(audit_record_.sql_)) {
        strcat(buf, audit_record_.sql_);
      }
      strcat(buf, request_print_prefix);
      strcat(buf, request_string_);
      audit_record_.sql_ = buf;
      audit_record_.sql_len_ = buf_size;
      audit_record_.sql_cs_type_ = ObCharset::get_system_collation();
    }
  }

  // request info
  audit_record_.is_executor_rpc_ = false; // FIXME(wenqu): set false for print sql_id
  audit_record_.is_hit_plan_cache_ = false;
  audit_record_.is_inner_sql_ = false;
  audit_record_.is_multi_stmt_ = false;
  audit_record_.partition_cnt_ = 1; // always 1 for now;
  audit_record_.plan_id_ = 0;
  audit_record_.plan_type_ = OB_PHY_PLAN_UNINITIALIZED;

  // in-process info
  audit_record_.execution_id_ = 0; // not used for table api
  audit_record_.request_id_ = 0; // not used for table api
  audit_record_.seq_ = 0; // not used
  audit_record_.session_id_ = 0; // not used  for table api

  // tx info
  audit_record_.trans_id_ = tx_snapshot_.core_.tx_id_.get_id();
  audit_record_.snapshot_.version_ = tx_snapshot_.core_.version_;
  audit_record_.snapshot_.tx_id_ = tx_snapshot_.core_.tx_id_.get_id();
  audit_record_.snapshot_.scn_ = tx_snapshot_.core_.scn_.cast_to_int();
  audit_record_.snapshot_.source_ = tx_snapshot_.get_source_name();

  const int64_t elapsed_time = common::ObTimeUtility::fast_current_time() - audit_record_.exec_timestamp_.receive_ts_;
  if (elapsed_time > GCONF.trace_log_slow_query_watermark) {
    FORCE_PRINT_TRACE(THE_TRACE, "[table api][slow query]");
  }

  ret = OB_SUCCESS;
  MTL_SWITCH(credential_.tenant_id_) {
    obmysql::ObMySQLRequestManager *req_manager = MTL(obmysql::ObMySQLRequestManager*);
    if (nullptr == req_manager) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get request manager for current tenant", K(ret));
    } else if (OB_FAIL(req_manager->record_request(audit_record_, true))) {
      if (OB_SIZE_OVERFLOW == ret || OB_ALLOCATE_MEMORY_FAILED == ret) {
        LOG_DEBUG("cannot allocate mem for record", K(ret));
        ret = OB_SUCCESS;
      } else {
        if (REACH_TIME_INTERVAL(100 * 1000)) { // in case logging is too frequent
          LOG_WARN("failed to record request info in request manager", K(ret));
        }
      }
    }
  }

}

int ObTableApiProcessorBase::process_with_retry(const ObString &credential, const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  audit_record_.exec_timestamp_.process_executor_ts_ = ObTimeUtility::fast_current_time();
  ObWaitEventStat total_wait_desc;
  if (OB_ISNULL(gctx_.ob_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(gctx_.ob_service_), K(ret));
  } else if (OB_FAIL(check_arg())) {
    LOG_WARN("check arg failed", K(ret));
  } else if (OB_FAIL(check_user_access(credential))) {
    LOG_WARN("check user access failed", K(ret));
  } else {
    ObMaxWaitGuard max_wait_guard(&audit_record_.exec_record_.max_wait_event_);
    ObTotalWaitGuard total_wait_guard(&total_wait_desc);
    ObTenantStatEstGuard stat_guard(credential_.tenant_id_);
    ObProcessMallocCallback pmcb(0, audit_record_.request_memory_used_);
    ObMallocCallbackGuard malloc_guard_(pmcb);
    need_retry_in_queue_ = false;
    bool did_local_retry = false;
    do {
      ret = try_process();
      did_local_retry = false;
      // is_partition_change_error(ret) || is_master_changed_error(ret) retry in client
      // OB_SCHEMA_EAGAIN:
      if ((OB_TRY_LOCK_ROW_CONFLICT == ret || OB_TRANSACTION_SET_VIOLATION == ret || OB_SCHEMA_EAGAIN == ret)
          && retry_policy_.allow_retry()) {
        int64_t now = ObTimeUtility::fast_current_time();
        if (now > timeout_ts) {
          LOG_WARN("process timeout", K(ret), K(now), K(timeout_ts));
          did_local_retry = false;
        } else {
          if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
            // throw to queue and retry
            if (retry_policy_.allow_rpc_retry() && THIS_WORKER.can_retry()) {
              THIS_WORKER.set_need_retry();
              LOG_DEBUG("set retry flag and retry later when lock available");
              need_retry_in_queue_ = true;
            } else {
              // retry in current thread
              did_local_retry = true;
            }
          } else if (OB_TRANSACTION_SET_VIOLATION == ret) {
            EVENT_INC(TABLEAPI_TSC_VIOLATE_COUNT);
            did_local_retry = true;
            // @todo sleep for is_master_changed_error(ret) etc. ?
          } else if (OB_SCHEMA_EAGAIN == ret) {
            // retry in current thread
            did_local_retry = true;
          }
        }
      }
      if (did_local_retry) {
        if (retry_count_ < retry_policy_.max_local_retry_count_) {
          ++retry_count_;
          reset_ctx();
        } else {
          did_local_retry = false;
        }
      }
    } while (did_local_retry);
    // record events
    if (need_audit()) {
      audit_on_finish();
    }
  }
  if (lib::is_diagnose_info_enabled()) {
    audit_record_.exec_record_.wait_time_end_ = total_wait_desc.time_waited_;
    audit_record_.exec_record_.wait_count_end_ = total_wait_desc.total_waits_;
  }
  audit_record_.status_ = ret;
  return ret;
}

////////////////////////////////////////////////////////////////
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_EXECUTE> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_BATCH_EXECUTE> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_EXECUTE_QUERY> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_QUERY_AND_MUTATE> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_EXECUTE_QUERY_ASYNC> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_DIRECT_LOAD> >;
template class oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_LS_EXECUTE> >;

template<class T>
int ObTableRpcProcessor<T>::deserialize()
{
  audit_record_.exec_timestamp_.run_ts_ = ObTimeUtility::fast_current_time();
  return RpcProcessor::deserialize();
}

template<class T>
int ObTableRpcProcessor<T>::before_process()
{
  if (lib::is_diagnose_info_enabled()) {
    audit_record_.exec_record_.record_start();
  }
  audit_record_.exec_timestamp_.before_process_ts_ = ObTimeUtility::fast_current_time();
  if (OB_LIKELY(NULL != RpcProcessor::req_)) {
    audit_record_.exec_timestamp_.rpc_send_ts_ = RpcProcessor::req_->get_send_timestamp();
    audit_record_.exec_timestamp_.receive_ts_ = RpcProcessor::req_->get_receive_timestamp();
    audit_record_.exec_timestamp_.enter_queue_ts_ = RpcProcessor::req_->get_enqueue_timestamp();
  }
  user_client_addr_ = RPC_REQ_OP.get_peer(RpcProcessor::req_);
  if (need_audit()) {
    start_audit(RpcProcessor::req_);
  }
  return RpcProcessor::before_process();
}

template<class T>
int ObTableRpcProcessor<T>::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(process_with_retry(RpcProcessor::arg_.credential_, get_timeout_ts()))) {
    if (OB_NOT_NULL(request_string_)) { // request_string_ has been generated if enable sql_audit
      LOG_WARN("fail to process table_api request", K(ret), K(stat_event_type_), K(request_string_), K(audit_record_.exec_timestamp_));
    } else if (had_do_response()) { // req_ may be freed
      LOG_WARN("fail to process table_api request", K(ret), K(stat_event_type_),  K(audit_record_.exec_timestamp_));
    } else {
      LOG_WARN("fail to process table_api request", K(ret), K(stat_event_type_), "request", RpcProcessor::arg_,  K(audit_record_.exec_timestamp_));
    }
    // whether the client should refresh location cache and retry
    if (ObTableRpcProcessorUtil::is_require_rerouting_err(ret)) {
      ObRpcProcessor<T>::require_rerouting_ = true;
      LOG_WARN("table_api request require rerouting", K(ret), "require_rerouting", ObRpcProcessor<T>::require_rerouting_);
    }
  }
  return ret;
}

template<class T>
int ObTableRpcProcessor<T>::before_response(int error_code)
{
  const int64_t curr_time = ObTimeUtility::fast_current_time();
  audit_record_.exec_timestamp_.executor_end_ts_ = curr_time;
  // timestamp of start get plan, no need for table_api, set euqal to process_executor_ts_
  audit_record_.exec_timestamp_.single_process_ts_ = audit_record_.exec_timestamp_.process_executor_ts_;
  if (need_audit()) {
    const int64_t elapsed_us = curr_time - RpcProcessor::get_receive_timestamp();
    ObTableRpcProcessorUtil::record_stat(audit_record_, stat_event_type_, elapsed_us, audit_row_count_);
    // todo: distinguish hbase rows and ob rows.
  }
  return RpcProcessor::before_response(error_code);
}

template<class T>
int ObTableRpcProcessor<T>::response(int error_code)
{
  int ret = OB_SUCCESS;
  // if it is waiting for retry in queue, the response can NOT be sent.
  if (!need_retry_in_queue_ && !had_do_response()) {
    const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket&>(this->req_->get_packet());
    if (ObTableRpcProcessorUtil::need_do_move_response(error_code, *rpc_pkt)) {
      // response rerouting packet
      ObTableMoveResponseSender sender(this->req_, error_code);
      if (OB_FAIL(sender.init(ObTableApiProcessorBase::table_id_, ObTableApiProcessorBase::tablet_id_, *gctx_.schema_service_))) {
        LOG_WARN("fail to init move response sender", K(ret), K(RpcProcessor::arg_));
      } else if (OB_FAIL(sender.response())) {
        LOG_WARN("fail to do move response", K(ret));
      }
      if (OB_FAIL(ret)) {
        ret = RpcProcessor::response(error_code); // do common response when do move response failed
      }
    } else {
      ret = RpcProcessor::response(error_code);
    }
  }
  return ret;
}

template<class T>
int ObTableRpcProcessor<T>::after_process(int error_code)
{
  int ret = OB_SUCCESS;
  NG_TRACE(process_end); // print trace log if necessary
  // some statistics must be recorded for plan stat, even though sql audit disabled
  audit_record_.exec_timestamp_.exec_type_ = ExecType::RpcProcessor;
  audit_record_.exec_timestamp_.net_t_ = 0;
  audit_record_.exec_timestamp_.net_wait_t_ =
      audit_record_.exec_timestamp_.enter_queue_ts_ - audit_record_.exec_timestamp_.receive_ts_;
  audit_record_.exec_timestamp_.update_stage_time();
  if (lib::is_diagnose_info_enabled()) {
    audit_record_.exec_record_.record_end();
    audit_record_.update_event_stage_state();
  }
  if (need_audit()) {
    end_audit();
  }
  if (OB_FAIL(ObTableConnectionMgr::get_instance().update_table_connection(user_client_addr_,
                credential_.tenant_id_, credential_.database_id_, credential_.user_id_))) {
       LOG_WARN("fail to update conn active time", K(ret), K_(user_client_addr), K_(credential_.tenant_id),
                K_(credential_.database_id), K_(credential_.user_id));
  }
  return RpcProcessor::after_process(error_code);
}

template<class T>
void ObTableRpcProcessor<T>::set_req_has_wokenup()
{
  RpcProcessor::req_ = NULL;
}

template<class T>
void ObTableRpcProcessor<T>::save_request_string()
{
  int ret = OB_SUCCESS;
  const char *arg_str = to_cstring(RpcProcessor::arg_);
  if (OB_NOT_NULL(arg_str)) {
    const int64_t buf_size = strlen(arg_str) + 1;
    char *buf = reinterpret_cast<char *>(audit_allocator_.alloc(buf_size));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc audit memory", K(ret), K(buf_size), K(arg_str));
    } else {
      memset(buf, 0, buf_size);
      strcpy(buf, arg_str);
      request_string_ = buf;
      request_string_len_ = buf_size;
    }
  }
}

template<class T>
void ObTableRpcProcessor<T>::generate_sql_id()
{
  uint64_t checksum = get_request_checksum();
  checksum = ob_crc64(checksum, &credential_.tenant_id_, sizeof(credential_.tenant_id_));
  checksum = ob_crc64(checksum, &credential_.user_id_, sizeof(credential_.user_id_));
  checksum = ob_crc64(checksum, &credential_.database_id_, sizeof(credential_.database_id_));
  snprintf(audit_record_.sql_id_, (int32_t)sizeof(audit_record_.sql_id_),
     "TABLEAPI0x%04Xvv%016lX", RpcProcessor::PCODE, checksum);
}

// only use for batch_execute and htable_mutate_row, to check if need to get the global snapshot
int ObTableApiProcessorBase::check_table_has_global_index(bool &exists)
{
  int ret = OB_SUCCESS;
  exists = false;
  if (!schema_cache_guard_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is not inited", K(ret), K_(schema_cache_guard));
  } else if (OB_FAIL(schema_cache_guard_.has_global_index(exists))) {
    LOG_WARN("fail to check global index", K(ret));
  }
  return ret;
}

int ObTableApiProcessorBase::get_tablet_id(const ObTabletID &arg_tablet_id, uint64_t table_id, ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id = arg_tablet_id;
  if (!tablet_id.is_valid()) {
    share::schema::ObSchemaGetterGuard schema_guard;
    const uint64_t tenant_id = MTL_ID();
    if (OB_ISNULL(simple_table_schema_) || (!is_tablegroup_req_ && table_id != simple_table_schema_->get_table_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is NULL or table_id is not correct", K(ret), K(table_id));
    } else if (is_tablegroup_req_ && table_id != simple_table_schema_->get_table_id()) {
      // table id not equal should retry in table group route
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table id not correct in table group", K(ret));
    } else if (!simple_table_schema_->is_partitioned_table()) {
      tablet_id = simple_table_schema_->get_tablet_id();
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "partitioned table not pass tablet id");
      LOG_WARN("partitioned table must pass tablet id", K(ret), K(table_id));
    }
  }
  return ret;
}
