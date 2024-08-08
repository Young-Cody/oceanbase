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

#define USING_LOG_PREFIX BALANCE
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_struct.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "rootserver/ob_partition_balance.h"
#include "rootserver/ob_tenant_balance_service.h" // ObTenantBalanceService
#include "observer/ob_server_struct.h"//GCTX

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::share;
using namespace oceanbase::storage;

const int64_t PART_GROUP_SIZE_SEGMENT[] = {
  10 * 1024L * 1024L * 1024L, // 10G
  5 * 1024L * 1024L * 1024L,  // 5G
  2 * 1024L * 1024L * 1024L,  // 2G
  1 * 1024L * 1024L * 1024L,  // 1G
  500 * 1024L * 1024L,        // 500M
  200 * 1024L * 1024L,        // 200M
  100 * 1024L * 1024L         // 100M
};

int ObPartitionBalance::init(
    uint64_t tenant_id,
    schema::ObMultiVersionSchemaService *schema_service,
    common::ObMySQLProxy *sql_proxy,
    const int64_t primary_zone_num,
    const int64_t unit_group_num,
    TaskMode task_mode,
    const ObPartDistributionMode &part_distribution_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionBalance has inited", KR(ret), K(this));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(schema_service) || OB_ISNULL(sql_proxy)
      || (task_mode != GEN_BG_STAT && task_mode != GEN_TRANSFER_TASK)
      || part_distribution_mode <= ObPartDistributionMode::INVALID
      || part_distribution_mode >= ObPartDistributionMode::MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObPartitionBalance run", KR(ret), K(tenant_id), K(schema_service), K(sql_proxy),
            K(task_mode), K(part_distribution_mode));
  } else if (OB_FAIL(bg_builder_.init(tenant_id, "PART_BALANCE", *this, *sql_proxy, *schema_service))) {
    LOG_WARN("init all balance group builder fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(bg_map_.create(40960, lib::ObLabel("PART_BALANCE"), ObModIds::OB_HASH_NODE, tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(ls_desc_map_.create(10, lib::ObLabel("PART_BALANCE"), ObModIds::OB_HASH_NODE, tenant_id))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(bg_ls_stat_operator_.init(sql_proxy))) {
    LOG_WARN("init balance group ls stat operator fail", KR(ret));
  } else if (OB_FAIL(job_generator_.init(tenant_id, primary_zone_num, unit_group_num, sql_proxy))) {
    LOG_WARN("init job generator failed", KR(ret), K(tenant_id),
        K(primary_zone_num), K(unit_group_num), KP(sql_proxy));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;

    //reset
    allocator_.reset();
    ls_desc_array_.reset();
    task_mode_ = task_mode;
    bg_offset_ = 0;
    part_distribution_mode_ = part_distribution_mode;
    inited_ = true;
  }
  return ret;
}

void ObPartitionBalance::destroy()
{
  FOREACH(iter, ls_desc_map_) {
    if (OB_NOT_NULL(iter->second)) {
      iter->second->~ObLSDesc();
    }
  }
  FOREACH(iter, bg_map_) {
    ObArray<ObBalanceGroupInfo *> &bg_ls_arr = iter->second;
    ARRAY_FOREACH_NORET(bg_ls_arr, i) {
      ObBalanceGroupInfo* bg_info = bg_ls_arr.at(i);
      if (OB_NOT_NULL(bg_info)) {
        bg_info->~ObBalanceGroupInfo();
        bg_info = NULL;
      }
    }
    bg_ls_arr.destroy();
  }
  //reset
  job_generator_.reset();
  bg_builder_.destroy();
  allocator_.reset();
  ls_desc_array_.reset();
  ls_desc_map_.destroy();
  bg_map_.destroy();
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  dup_ls_id_.reset();
  sql_proxy_ = NULL;
  task_mode_ = GEN_BG_STAT;
  part_distribution_mode_ = ObPartDistributionMode::INVALID;
  bg_offset_ = 0;
}

int ObPartitionBalance::process(const ObBalanceJobID &job_id, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObBalanceJobType job_type(ObBalanceJobType::BALANCE_JOB_PARTITION);
  ObBalanceStrategy balance_strategy;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  } else if (OB_FAIL(prepare_balance_group_())) {
    LOG_WARN("prepare_balance_group fail", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(save_balance_group_stat_())) {
    LOG_WARN("save balance group stat fail", KR(ret), K(tenant_id_));
  } else if (GEN_BG_STAT == task_mode_) {
    // finish
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_ATTR_ALIGN;
  } else if (bg_map_.empty()) {
    LOG_INFO("PART_BALANCE balance group is empty do nothing", K(tenant_id_));
  } else if (OB_FAIL(process_balance_partition_inner_())) {
    LOG_WARN("process_balance_partition_inner fail", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_INTRA_GROUP;
  } else if (OB_FAIL(process_balance_partition_extend_())) {
    LOG_WARN("process_balance_partition_extend fail", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_INTER_GROUP;
  } else if (OB_FAIL(process_balance_partition_disk_())) {
    LOG_WARN("process_balance_partition_disk fail", KR(ret), K(tenant_id_));
  } else if (job_generator_.need_gen_job()) {
    balance_strategy = ObBalanceStrategy::PB_PART_DISK;
  }

  if (OB_FAIL(ret) || !balance_strategy.is_valid()) {
    // skip
  } else if (OB_UNLIKELY(!balance_strategy.is_partition_balance_strategy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected balance strategy", KR(ret), K(balance_strategy));
  } else {
    // compatible scenario: observer is new but data_version has not been pushed up
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id_));
    } else if (data_version < DATA_VERSION_4_2_4_0) {
      balance_strategy = ObBalanceStrategy::PB_COMPAT_OLD;
    }
    if (FAILEDx(job_generator_.gen_balance_job_and_tasks(job_type, balance_strategy, job_id, timeout))) {
      LOG_WARN("gen balance job and tasks failed", KR(ret),
          K(tenant_id_), K(job_type), K(balance_strategy), K(job_id), K(timeout));
    }
  }

  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("PART_BALANCE process", KR(ret), K(tenant_id_), K(task_mode_), K(job_id), K(timeout),
      "cost", end_time - start_time, "need balance", job_generator_.need_gen_job());
  return ret;
}

int ObPartitionBalance::prepare_ls_()
{
  int ret = OB_SUCCESS;
  ObLSStatusInfoArray ls_stat_array;
  if (!inited_ || OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(inited_), KP(sql_proxy_));
  } else if (OB_FAIL(ObTenantBalanceService::gather_ls_status_stat(tenant_id_, ls_stat_array))) {
    LOG_WARN("failed to gather ls status", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(job_generator_.prepare_ls(ls_stat_array))) {
    LOG_WARN("job_generator_ prepare ls failed", KR(ret), K(tenant_id_), K(ls_stat_array));
  } else {
    ARRAY_FOREACH(ls_stat_array, idx) {
      const ObLSStatusInfo &ls_stat = ls_stat_array.at(idx);
      ObLSDesc *ls_desc = nullptr;
      if (ls_stat.get_ls_id().is_sys_ls()) {
        // only skip sys ls
      } else if (!ls_stat.is_normal()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("ls is not in normal status", KR(ret), K(ls_stat));
      } else if (OB_ISNULL(ls_desc = reinterpret_cast<ObLSDesc*>(allocator_.alloc(sizeof(ObLSDesc))))) {
         ret = OB_ALLOCATE_MEMORY_FAILED;
         LOG_WARN("alloc mem fail", KR(ret));
      } else if (FALSE_IT(new(ls_desc) ObLSDesc(ls_stat.get_ls_id(),
                                                ls_stat.get_ls_group_id(),
                                                ls_stat.get_unit_group_id()))) {
      } else if (OB_FAIL(ls_desc_map_.set_refactored(ls_desc->get_ls_id(), ls_desc))) { // need dup ls desc to gen job
        LOG_WARN("init ls_desc to map fail", KR(ret), K(ls_desc->get_ls_id()));
      } else if (ls_stat.is_duplicate_ls()) {
        if (OB_UNLIKELY(dup_ls_id_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("there should be only one dup ls when doing partition balance",
              KR(ret), K(dup_ls_id_), K(ls_stat), K(idx));
        } else {
          dup_ls_id_ = ls_stat.get_ls_id();
        }
        // ls_desc_array_ is used to balance partition groups on all user ls.
        // Paritions on dup ls can not be balanced together.
      } else if (OB_FAIL(ls_desc_array_.push_back(ls_desc))) {
        LOG_WARN("push_back ls_desc to array fail", KR(ret), K(ls_desc->get_ls_id()));
      }
    }
    if (OB_SUCC(ret) && ls_desc_array_.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no ls can assign", KR(ret), K(tenant_id_));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[PART_BALANCE] prepare_ls", K(tenant_id_), K(dup_ls_id_),
        K(ls_desc_array_), "ls_desc_map_ size", ls_desc_map_.size(), K(ls_stat_array));
  }
  return ret;
}

int ObPartitionBalance::prepare_balance_group_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  }
  // Here must prepare balance group data first, to get schema info for this build
  else if (OB_FAIL(bg_builder_.prepare(GEN_TRANSFER_TASK == task_mode_))) {
    LOG_WARN("balance group builder prepare fail", KR(ret));
  }
  // Then prepare LS info after schema info prepared
  else if (OB_FAIL(prepare_ls_())) {
    LOG_WARN("prepare_ls fail", KR(ret), K(tenant_id_));
  }
  // At last, build balance group info
  // During this build, on_new_partition() will be called
  else if (OB_FAIL(bg_builder_.build())) {
    LOG_WARN("balance group build fail", KR(ret), K(tenant_id_));
  }
  return ret;
}

int ObPartitionBalance::on_new_partition(
    const ObBalanceGroup &bg_in,
    const ObSimpleTableSchemaV2 &table_schema,
    const ObObjectID part_object_id,
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    const int64_t tablet_size,
    const bool in_new_partition_group,
    const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  ObBalanceGroup bg = bg_in; // get a copy
  ObLSDesc *src_ls_desc = nullptr;
  ObTransferPartInfo part_info;
  const bool is_dup_ls_related_part = dup_ls_id_.is_valid() && (src_ls_id == dup_ls_id_ || dest_ls_id == dup_ls_id_);

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not inited", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!bg_in.is_valid() || !table_schema.is_valid()
            || !is_valid_id(part_object_id) || !is_valid_id(part_group_uid)
            || !src_ls_id.is_valid_with_tenant(tenant_id_)
            || !dest_ls_id.is_valid_with_tenant(tenant_id_)
            || tablet_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_in), K(table_schema), K(part_object_id),
            K(src_ls_id), K(dest_ls_id), K(tablet_size), K(part_group_uid));
  } else if (OB_FAIL(part_info.init(table_schema.get_table_id(), part_object_id))) {
    LOG_WARN("part_info init fail", KR(ret), K(table_schema.get_table_id()), K(part_object_id));
  } else if (dest_ls_id != src_ls_id) { // need transfer
    // transfer caused by table duplicate_scope change or tablegroup change
    if (OB_FAIL(job_generator_.add_need_transfer_part(src_ls_id, dest_ls_id, part_info))) {
      LOG_WARN("add need transfer part failed",
          KR(ret), K(src_ls_id), K(dest_ls_id), K(part_info), K(dup_ls_id_));
    }
  } else if (is_dup_ls_related_part) {
    // do not record dup ls related part_info
  } else if (OB_FAIL(ls_desc_map_.get_refactored(src_ls_id, src_ls_desc))) {
    LOG_WARN("get LS desc fail", KR(ret), K(src_ls_id));
  } else if (OB_ISNULL(src_ls_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found LS", KR(ret), K(src_ls_id), KPC(src_ls_desc));
  } else if (OB_FAIL(add_part_to_bg_map_(src_ls_id, bg, table_schema, part_group_uid,
                                        part_info, tablet_size))) {
    LOG_WARN("add new partition group to balance group failed", KR(ret), K(src_ls_id), K(bg),
            K(table_schema), K(part_group_uid), K(part_info), K(tablet_size));
  } else if (in_new_partition_group && FALSE_IT(src_ls_desc->add_partgroup(1, 0))) {
  } else {
    src_ls_desc->add_data_size(tablet_size);
  }
  return ret;
}

int ObPartitionBalance::add_part_to_bg_map_(
    const ObLSID &ls_id,
    ObBalanceGroup &bg,
    const ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid,
    const ObTransferPartInfo &part_info,
    const int64_t tablet_size)
{
  int ret = OB_SUCCESS;
  ObArray<ObBalanceGroupInfo *> *bg_ls_array = NULL;
  bg_ls_array = bg_map_.get(bg);
  if (OB_ISNULL(bg_ls_array)) {
    ObArray<ObBalanceGroupInfo *> bg_ls_array_obj;
    if (OB_FAIL(bg_map_.set_refactored(bg, bg_ls_array_obj))) {
      LOG_WARN("init part to balance group fail", KR(ret), K(ls_id), K(bg));
    } else {
      bg_ls_array = bg_map_.get(bg);
      if (OB_ISNULL(bg_ls_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get part from balance group fail", KR(ret), K(ls_id), K(bg));
      } else if (FALSE_IT(bg_ls_array->set_block_allocator(
          ModulePageAllocator(allocator_, "BGLSArray")))) {
      } else if (OB_FAIL(bg_ls_array->reserve(ls_desc_array_.count()))) {
        LOG_WARN("fail to reserve", KR(ret), K(bg_ls_array));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_desc_array_.count(); i++) {
          ObBalanceGroupInfo *bg_info = nullptr;
          if (OB_ISNULL(bg_info = reinterpret_cast<ObBalanceGroupInfo *>(
              allocator_.alloc(sizeof(ObBalanceGroupInfo))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret));
          } else if (OB_ISNULL(bg_info = new(bg_info) ObBalanceGroupInfo(allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret));
          } else if (OB_FAIL(bg_info->init(bg.id(), ls_desc_array_.at(i)->get_ls_id(),
                                          ls_desc_array_.count(), part_distribution_mode_))) {
            LOG_WARN("failed to init bg_info", KR(ret), K(bg_info));
          } else if (OB_FAIL(bg_ls_array->push_back(bg_info))) {
            LOG_WARN("push_back fail", KR(ret), K(ls_id), K(bg));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(bg_ls_array)) {
    bool find_ls = false;
    ARRAY_FOREACH(*bg_ls_array, idx) {
      ObBalanceGroupInfo *bg_info = bg_ls_array->at(idx);
      if (OB_ISNULL(bg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bg_info is null", KR(ret), K(bg_info), KPC(bg_ls_array), K(idx));
      } else if (bg_info->get_ls_id() == ls_id) {
        if (OB_FAIL(bg_info->append_part(table_schema, part_group_uid, part_info, tablet_size))) {
          LOG_WARN("append_part failed", KR(ret), K(bg_ls_array), K(idx), K(part_info),
                  K(tablet_size), K(part_group_uid), K(table_schema));
        }
        find_ls = true;
        break;
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!find_ls)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not found LS", KR(ret), K(ls_id), K(part_info), K(bg));
    }
  }

  return ret;
}

int ObPartitionBalance::process_balance_partition_inner_()
{
  int ret = OB_SUCCESS;
  ObIPartGroupInfo *pg_info = nullptr;
  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    ObArray<ObBalanceGroupInfo *> &bg_ls_array = iter->second;
    const int64_t ls_cnt = bg_ls_array.count();
    if (OB_UNLIKELY(0 == ls_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls cnt of the balance group is 0", KR(ret), K(bg_ls_array));
    }
    while (OB_SUCC(ret)) {
      ObBGPartGroupCountCmp cmp;
      ObBalanceGroupInfo *ls_max = nullptr;
      ObBalanceGroupInfo *ls_min = nullptr;
      ObBalanceGroupInfo *ls_more = nullptr;
      ObBalanceGroupInfo *ls_less = nullptr;
      int64_t transfer_cnt = 0;
      lib::ob_sort(bg_ls_array.begin(), bg_ls_array.end(), cmp);
      ls_min = bg_ls_array.at(0);
      ls_max = bg_ls_array.at(ls_cnt - 1);
      if (OB_FAIL(cmp.get_error_code())) {
        LOG_WARN("failed to get the balance group info with the most and least pg",
                KR(ret), K(bg_ls_array));
      } else if (OB_ISNULL(ls_min) || OB_ISNULL(ls_max)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("balance group info is null", KR(ret), KP(ls_max), KP(ls_min));
        // If difference in number of partition groups between LS does not exceed 1, this balance group is balanced
      } else if ((ls_max->get_part_group_count() - ls_min->get_part_group_count()) <= 1) {
        // balance group has done
        break;
      } else if (OB_FAIL(get_part_group_count_inbalance_ls_(bg_ls_array, ls_more, ls_less, transfer_cnt))) {
        LOG_WARN("fail to get a pair of ls whose part group count are inbalance", KR(ret));
      } else if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls_more or ls_less is null", KR(ret), KP(ls_more), KP(ls_less));
      } else {
        /* example1:          ls1  ls2  ls3  ls4  ls5
        * part_group_count:  3    4    4    5    5
        * we should find ls4 -> ls1
        *
        * example2:          ls1  ls2  ls3  ls4  ls5
        * part_group_count:  3    3    4    4    5
        * we should find ls5 -> ls2
        *
        * example3:          ls1  ls2  ls3  ls4  ls5
        * part_group_count:  3    3    3    3    5
        * we should find ls5 -> ls4
        */
        for (int64_t i = 0; OB_SUCC(ret) && i < transfer_cnt; i++) {
          if (OB_FAIL(ls_more->transfer_out(*ls_less, pg_info))) {
            LOG_WARN("failed to transfer partition from ls_more to ls_less", KR(ret),
                    KPC(ls_more), KPC(ls_less));
          } else if (OB_UNLIKELY(nullptr == pg_info
                                || !pg_info->is_valid()
                                || nullptr == pg_info->part_group())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid pg_info", KR(ret), KPC(pg_info));
          } else if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(),
                                                ls_less->get_ls_id(), pg_info->part_group()))) {
            LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()),
                    K(ls_less->get_ls_id()), KPC(pg_info));
          }
        }
      }
    }
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_inner end", K(tenant_id_), K(ls_desc_array_));
  return ret;
}

int ObPartitionBalance::add_transfer_task_(
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    const ObTransferPartGroup *part_group,
    bool modify_ls_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part group is null", KR(ret), KP(part_group));
  } else {
    ARRAY_FOREACH(part_group->get_part_list(), idx) {
      const ObTransferPartInfo &part_info = part_group->get_part_list().at(idx);
      if (OB_FAIL(job_generator_.add_need_transfer_part(src_ls_id, dest_ls_id, part_info))) {
        LOG_WARN("add need transfer part failed", KR(ret), K(src_ls_id), K(dest_ls_id), K(part_info));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (modify_ls_desc && OB_FAIL(update_ls_desc_(src_ls_id, -1, part_group->get_data_size() * -1))) {
      LOG_WARN("update_ls_desc", KR(ret), K(src_ls_id));
    } else if (modify_ls_desc && OB_FAIL(update_ls_desc_(dest_ls_id, 1, part_group->get_data_size()))) {
      LOG_WARN("update_ls_desc", KR(ret), K(dest_ls_id));
    }
  }
  return ret;
}

int ObPartitionBalance::update_ls_desc_(const ObLSID &ls_id, int64_t cnt, int64_t size)
{
  int ret = OB_SUCCESS;

  ObLSDesc *ls_desc = nullptr;
  if (OB_FAIL(ls_desc_map_.get_refactored(ls_id, ls_desc))) {
    LOG_WARN("get_refactored", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found ls", KR(ret), K(ls_id));
  } else {
    ls_desc->add_partgroup(cnt, size);
  }
  return ret;
}

int ObPartitionBalance::process_balance_partition_extend_()
{
  int ret = OB_SUCCESS;
  int64_t ls_cnt = ls_desc_array_.count();
  ObIPartGroupInfo *pg_info = nullptr;
  if (OB_UNLIKELY(0 == ls_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls cnt is 0", KR(ret), K(ls_cnt));
  }
  while (OB_SUCC(ret)) {
    ObLSPartGroupCountCmp cmp;
    ObLSDesc *ls_max = nullptr;
    ObLSDesc *ls_min = nullptr;
    ObLSDesc *ls_more_desc = nullptr;
    ObLSDesc *ls_less_desc = nullptr;
    int64_t transfer_cnt = 0;
    lib::ob_sort(ls_desc_array_.begin(), ls_desc_array_.end(), cmp);
    ls_max = ls_desc_array_.at(ls_cnt - 1);
    ls_min = ls_desc_array_.at(0);
    if (OB_FAIL(cmp.get_error_code())) {
      LOG_WARN("failed to get the ls with the most and least pg", KR(ret), K_(ls_desc_array));
    } else if (OB_ISNULL(ls_min) || OB_ISNULL(ls_max)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls desc is null", KR(ret), KP(ls_max), KP(ls_min));
    } else if ((ls_max->get_part_group_count() - ls_min->get_part_group_count()) <= 1) {
      break;
    } else if (OB_FAIL(get_part_group_count_inbalance_ls_(ls_desc_array_, ls_more_desc,
                                                          ls_less_desc, transfer_cnt))) {
      LOG_WARN("fail to get a pair of ls whose part group count are inbalance", KR(ret));
    } else if (OB_ISNULL(ls_more_desc) || OB_ISNULL(ls_less_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls desc is null", KR(ret), KP(ls_more_desc), KP(ls_less_desc));
    } else {
      /*
      * example: ls1  ls2  ls3  ls4
      * bg1      3    3    3    4
      * bg2      5    5    5    6
      *
      * we should move ls4.bg1 -> ls3 or ls4.bg2 -> ls3
      */
      for (int64_t transfer_idx = 0; OB_SUCC(ret) && transfer_idx < transfer_cnt; transfer_idx++) {
        FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
          // find ls_more/ls_less
          ObBalanceGroupInfo *ls_more = nullptr;
          ObBalanceGroupInfo *ls_less = nullptr;
          if (OB_FAIL(get_balance_group_info_(iter->second, *ls_more_desc, *ls_less_desc,
                                              ls_more, ls_less))) {
            LOG_WARN("fail to get corresponding balance group info", KR(ret), K(iter->second),
                    KPC(ls_more_desc), KPC(ls_less_desc));
          } else if (ls_more->get_part_group_count() > ls_less->get_part_group_count()) {
            if (OB_FAIL(ls_more->transfer_out(*ls_less, pg_info))) {
              LOG_WARN("failed to transfer partition from ls_more to ls_less", KR(ret),
                      KPC(ls_more), KPC(ls_less));
            } else if (OB_UNLIKELY(nullptr == pg_info
                                  || !pg_info->is_valid()
                                  || nullptr == pg_info->part_group())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid pg_info", KR(ret), KPC(pg_info));
            } else if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(), ls_less->get_ls_id(),
                                                  pg_info->part_group()))) {
              LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()),
                      K(ls_less->get_ls_id()), KPC(pg_info));
            }
            break;
          }
        }
      }
    }
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_extend end", K(ret), K(tenant_id_), K(ls_desc_array_));
  return ret;
}

int ObPartitionBalance::build_unit_group_ls_desc_(ObArray<ObUnitGroupLSDesc *> &unit_group_ls_desc_arr)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<uint64_t, ObUnitGroupLSDesc *> unit_group_ls_desc_map;
  unit_group_ls_desc_arr.reset();
  if (OB_FAIL(unit_group_ls_desc_map.create(128, "UnitGroupLSMap"))) {
    LOG_WARN("create unit_group_ls_desc_map fail", KR(ret));
  }
  ARRAY_FOREACH(ls_desc_array_, idx) {
    ObLSDesc *ls_desc = ls_desc_array_.at(idx);
    ObUnitGroupLSDesc *unit_group_ls_desc = nullptr;
    uint64_t unit_group_id = OB_INVALID_ID;
    if (OB_ISNULL(ls_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_desc is null", KR(ret), K(ls_desc));
    } else if (FALSE_IT(unit_group_id = ls_desc->get_unit_group_id())) {
    } else if (OB_FAIL(unit_group_ls_desc_map.get_refactored(unit_group_id, unit_group_ls_desc))) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
        if (OB_ISNULL(unit_group_ls_desc = reinterpret_cast<ObUnitGroupLSDesc *>(
            allocator_.alloc(sizeof(ObUnitGroupLSDesc))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", KR(ret));
        } else if (OB_ISNULL(unit_group_ls_desc = new(unit_group_ls_desc) ObUnitGroupLSDesc())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", KR(ret));
        } else if (OB_FAIL(unit_group_ls_desc_map.set_refactored(unit_group_id, unit_group_ls_desc))) {
          LOG_WARN("set_refactored failed", KR(ret), K(unit_group_id), KPC(unit_group_ls_desc));
        } else if (OB_FAIL(unit_group_ls_desc_arr.push_back(unit_group_ls_desc))) {
          LOG_WARN("push_back unit_group_ls_desc fail", KR(ret), K(unit_group_ls_desc_arr),
                  KPC(unit_group_ls_desc));
        }
      } else {
        LOG_WARN("get_refactored failed", KR(ret), K(unit_group_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(unit_group_ls_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_group_ls_desc is null", KR(ret), K(unit_group_ls_desc));
    } else if (OB_FAIL(unit_group_ls_desc->add_ls_desc(ls_desc))) {
      LOG_WARN("add_ls_desc fail", KR(ret), KPC(unit_group_ls_desc), KPC(ls_desc));
    }
  }
  return ret;
}

int ObPartitionBalance::balance_inter_unit_group_disk_(ObUnitGroupLSDesc &unit_group_max,
                                                      ObUnitGroupLSDesc &unit_group_min,
                                                      int64_t &balance_cnt)
{
  int ret = OB_SUCCESS;
  uint64_t unit_group_delta = unit_group_max.get_data_size() - unit_group_min.get_data_size();
  balance_cnt = 0;
  LOG_INFO("[PART_BALANCE] inter unit group disk_balance", K(unit_group_max), K(unit_group_min));
  for (int64_t idx = 0;
      OB_SUCC(ret) && idx < sizeof(PART_GROUP_SIZE_SEGMENT) / sizeof(PART_GROUP_SIZE_SEGMENT[0]);
      idx++) {
    const int64_t segment = PART_GROUP_SIZE_SEGMENT[idx];
    if (unit_group_delta <= segment) {
    } else if (OB_FAIL(balance_inter_unit_group_disk_min_size_(
        unit_group_max,
        unit_group_min,
        segment,
        balance_cnt))) {
      LOG_WARN("try_swap_part_group fail", KR(ret), K(unit_group_max), K(unit_group_min),
              K(segment));
    } else if (balance_cnt > 0) {
      break;
    }
  }
  return ret;
}

int ObPartitionBalance::balance_intra_unit_group_disk_(ObLSDesc &ls_max,
                                                      ObLSDesc &ls_min,
                                                      int64_t &balance_cnt)
{
  int ret = OB_SUCCESS;
  uint64_t ls_delta = ls_max.get_data_size() - ls_min.get_data_size();
  balance_cnt = 0;
  LOG_INFO("[PART_BALANCE] intra unit group disk_balance", K(ls_max), K(ls_min));
  /*
  * select swap part_group by size segment
  */
  for (int64_t idx = 0;
      OB_SUCC(ret) && idx < sizeof(PART_GROUP_SIZE_SEGMENT) / sizeof(PART_GROUP_SIZE_SEGMENT[0]);
      idx++) {
    const int64_t segment = PART_GROUP_SIZE_SEGMENT[idx];
    if (ls_delta <= segment) {
    } else if (OB_FAIL(balance_intra_unit_group_disk_min_size_(ls_max, ls_min, segment, balance_cnt))) {
      LOG_WARN("try_swap_part_group fail", KR(ret), K(ls_max), K(ls_min), K(segment));
    } else if (balance_cnt > 0) {
      break;
    }
  }
  return ret;
}

int ObPartitionBalance::try_balance_inter_unit_group_disk_(
    const ObArray<ObUnitGroupLSDesc *> &unit_group_ls_desc_arr) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(unit_group_ls_desc_arr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit_group_ls_desc_arr is empty", KR(ret), K(unit_group_ls_desc_arr));
  } else if (unit_group_ls_desc_arr.size() == 1) {
    // unit group num is 1, no need to balance inter unit group disk size
  } else {
    ObUnitGroupDataSizeCmp cmp;
    ObUnitGroupLSDesc *unit_group_max = nullptr;
    ObUnitGroupLSDesc *unit_group_min = nullptr;
    std::pair<ObArray<ObUnitGroupLSDesc *>::const_iterator,
              ObArray<ObUnitGroupLSDesc *>::const_iterator> min_max_it;
    while (OB_SUCC(ret)) {
      int64_t balance_cnt = 0;
      min_max_it = std::minmax_element(unit_group_ls_desc_arr.begin(),
                                      unit_group_ls_desc_arr.end(), cmp);
      if (OB_FAIL(cmp.get_error_code())) {
        LOG_WARN("fail to get the min/max unit group", KR(ret));
      } else if (OB_UNLIKELY(min_max_it.first == unit_group_ls_desc_arr.end()
                            || min_max_it.second == unit_group_ls_desc_arr.end())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_group_ls_desc_arr is empty", KR(ret));
      } else if (OB_ISNULL(unit_group_min = *min_max_it.first)
                || OB_ISNULL(unit_group_max = *min_max_it.second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit group ls desc is null", KR(ret), KP(unit_group_min), KP(unit_group_max));
      } else if (!check_disk_inbalance_(unit_group_max->get_data_size(),
                                        unit_group_min->get_data_size())) {
        break;
      } else if (OB_FAIL(balance_inter_unit_group_disk_(*unit_group_max, *unit_group_min,
                                                        balance_cnt))) {
        LOG_WARN("balance inter unit group disk fail", KR(ret), KPC(unit_group_max),
                KPC(unit_group_min));
      } else if (balance_cnt == 0) {
        // nothing to do
        break;
      }
    }
  }
  return ret;
}

int ObPartitionBalance::try_balance_intra_unit_group_disk_(
    const ObArray<ObUnitGroupLSDesc *> &unit_group_ls_desc_arr) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(unit_group_ls_desc_arr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit_group_ls_desc_arr is empty", KR(ret), K(unit_group_ls_desc_arr));
  } else {
    ARRAY_FOREACH(unit_group_ls_desc_arr, idx) {
      const ObUnitGroupLSDesc *unit_group_ls_desc = unit_group_ls_desc_arr.at(idx);
      if (OB_ISNULL(unit_group_ls_desc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_group_ls_desc is null", KR(ret), KPC(unit_group_ls_desc));
      }
      while (OB_SUCC(ret)) {
        ObLSDesc *ls_max = nullptr;
        ObLSDesc *ls_min = nullptr;
        int64_t balance_cnt = 0;
        if (OB_FAIL(unit_group_ls_desc->get_max_ls_desc(ls_max))) {
          LOG_WARN("fail to get max ls desc", KR(ret), KPC(unit_group_ls_desc), KPC(ls_max));
        } else if (OB_FAIL(unit_group_ls_desc->get_min_ls_desc(ls_min))) {
          LOG_WARN("fail to get min ls desc", KR(ret), KPC(unit_group_ls_desc), KPC(ls_min));
        } else if (OB_ISNULL(ls_max) || OB_ISNULL(ls_min)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls desc is null", KR(ret), KPC(unit_group_ls_desc), K(ls_max), K(ls_min));
        } else if (!check_disk_inbalance_(ls_max->get_data_size(), ls_min->get_data_size())) {
          break;
        } else if (OB_FAIL(balance_intra_unit_group_disk_(*ls_max, *ls_min, balance_cnt))) {
          LOG_WARN("balance intra unit group disk fail", KR(ret), KPC(unit_group_ls_desc));
        } else if (balance_cnt == 0) {
          // nothing to do
          break;
        }
      }
    }
  }
  return ret;
}

int ObPartitionBalance::process_balance_partition_disk_()
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitGroupLSDesc *> unit_group_ls_desc_arr;
  if (OB_FAIL(build_unit_group_ls_desc_(unit_group_ls_desc_arr))) {
    LOG_WARN("build_unit_group_ls_desc fail", KR(ret));
  } else if (OB_FAIL(try_balance_inter_unit_group_disk_(unit_group_ls_desc_arr))) {
    LOG_WARN("balance inter unit group disk failed", KR(ret), K(unit_group_ls_desc_arr));
  } else if (!job_generator_.need_gen_job()
            && OB_FAIL(try_balance_intra_unit_group_disk_(unit_group_ls_desc_arr))) {
    LOG_WARN("balance intra unit group disk failed", KR(ret), K(unit_group_ls_desc_arr));
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_disk end", KR(ret), K(tenant_id_),
          K(ls_desc_array_));
  return ret;
}

bool ObPartitionBalance::check_disk_inbalance_(uint64_t more_size, uint64_t less_size)
{
  bool need_swap = false;
  if (more_size > PART_BALANCE_THRESHOLD_SIZE) {
    if ((more_size - more_size * GCONF.balancer_tolerance_percentage / 100) > less_size) {
      need_swap = true;
    }
  }
  return need_swap;
}

int ObPartitionBalance::xfer_part_group_(
    ObBalanceGroupInfo &src_bg_ls,
    ObBalanceGroupInfo &dst_bg_ls,
    const ObIPartGroupInfo &src_pg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src_bg_ls.is_valid() || !dst_bg_ls.is_valid() || !src_pg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part group info is invalid", KR(ret), K(src_bg_ls), K(dst_bg_ls), K(src_pg));
  } else if (OB_FAIL(add_transfer_task_(src_bg_ls.get_ls_id(), dst_bg_ls.get_ls_id(),
                                        src_pg.part_group()))) {
    LOG_WARN("add_transfer_task_ fail", KR(ret), K(src_bg_ls), K(dst_bg_ls), K(src_pg));
  } else if (OB_FAIL(src_bg_ls.remove_part_group(src_pg))) {
    LOG_WARN("parts remove", KR(ret), K(src_bg_ls), K(src_pg));
  } else if (OB_FAIL(dst_bg_ls.append_part_group(src_pg))) {
    LOG_WARN("add part group in order fail", KR(ret), K(dst_bg_ls), K(src_pg));
  }
  return ret;
}

int ObPartitionBalance::swap_part_group_(
    ObBalanceGroupInfo &src_bg,
    ObBalanceGroupInfo &dst_bg,
    const ObIPartGroupInfo &src_pg,
    const ObIPartGroupInfo &dst_pg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src_bg.is_valid() || !dst_bg.is_valid()
                  || !src_pg.is_valid() || !dst_pg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part group info is invalid", KR(ret), K(src_bg), K(dst_bg), K(src_pg), K(dst_pg));
  } else if (OB_FAIL(add_transfer_task_(src_bg.get_ls_id(), dst_bg.get_ls_id(),
                                        src_pg.part_group()))) {
    LOG_WARN("add_transfer_task_ fail", KR(ret), K(src_bg.get_ls_id()), K(dst_bg.get_ls_id()));
  } else if (OB_FAIL(add_transfer_task_(dst_bg.get_ls_id(), src_bg.get_ls_id(),
                                        dst_pg.part_group()))) {
    LOG_WARN("add_transfer_task_ fail", KR(ret), K(src_bg.get_ls_id()), K(dst_bg.get_ls_id()));
  } else if (OB_FAIL(src_bg.remove_part_group(src_pg))) {
    LOG_WARN("parts remove", KR(ret), K(src_bg), K(src_pg));
  } else if (OB_FAIL(dst_bg.remove_part_group(dst_pg))) {
    LOG_WARN("parts remove", KR(ret), K(dst_bg), K(dst_pg));
  } else if (OB_FAIL(src_bg.append_part_group(dst_pg))) {
    LOG_WARN("add part group in order fail", KR(ret), K(src_bg), K(dst_pg));
  } else if (OB_FAIL(dst_bg.append_part_group(src_pg))) {
    LOG_WARN("add part group in order fail", KR(ret), K(dst_bg), K(src_pg));
  }
  return ret;
}

int ObPartitionBalance::try_xfer_part_group_(
    ObBalanceGroupInfo &src_bg,
    ObBalanceGroupInfo &dst_bg,
    int64_t part_group_min_size,
    int64_t data_size_delta,
    int64_t &balance_cnt)
{
  int ret = OB_SUCCESS;
  ObIPartGroupInfo *largest_pg = nullptr;
  int64_t xfer_size = 0;
  if (OB_FAIL(src_bg.get_largest_part_group(largest_pg))) {
    LOG_WARN("failed to get the largest part group", KR(ret), K(src_bg));
  } else if (OB_UNLIKELY(nullptr == largest_pg || !largest_pg->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group info is invalid", KR(ret), KPC(largest_pg));
  } else if (FALSE_IT(xfer_size = largest_pg->part_group()->get_data_size())) { // empty
  } else if (xfer_size >= part_group_min_size && xfer_size < data_size_delta) {
    LOG_INFO("[PART_BALANCE] swap_partition", KPC(largest_pg->part_group()), K(data_size_delta),
            K(xfer_size), K(part_group_min_size));
    if (OB_FAIL(xfer_part_group_(src_bg, dst_bg, *largest_pg))) {
      LOG_WARN("fail to swap part group", KR(ret), K(src_bg), K(dst_bg), KPC(largest_pg));
    } else {
      balance_cnt++;
      bg_offset_++;
    }
  }
  return ret;
}

int ObPartitionBalance::try_swap_part_group_(
    ObBalanceGroupInfo &src_bg,
    ObBalanceGroupInfo &dst_bg,
    int64_t part_group_min_size,
    int64_t data_size_delta,
    int64_t &balance_cnt)
{
  int ret = OB_SUCCESS;
  ObIPartGroupInfo *largest_pg = nullptr;
  ObIPartGroupInfo *smallest_pg = nullptr;
  int64_t swap_size = 0;
  if (OB_FAIL(src_bg.get_largest_part_group(largest_pg))) {
    LOG_WARN("failed to get the largest part group", KR(ret), K(src_bg));
  } else if (OB_FAIL(dst_bg.get_smallest_part_group(smallest_pg))) {
    LOG_WARN("failed to get the smallest part group", KR(ret), K(dst_bg));
  } else if (OB_UNLIKELY(nullptr == largest_pg || nullptr == smallest_pg
                        || !largest_pg->is_valid() || !smallest_pg->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("largest or smallest part group info is invalid", KR(ret), KPC(largest_pg),
            KPC(smallest_pg));
  } else if (FALSE_IT(swap_size = largest_pg->part_group()->get_data_size() -
                                  smallest_pg->part_group()->get_data_size())) { // empty
  } else if (swap_size >= part_group_min_size && swap_size < data_size_delta) {
    LOG_INFO("[PART_BALANCE] swap_partition", KPC(largest_pg->part_group()),
            KPC(smallest_pg->part_group()), K(data_size_delta),
            K(swap_size), K(part_group_min_size));
    if (OB_FAIL(swap_part_group_(src_bg, dst_bg, *largest_pg, *smallest_pg))) {
      LOG_WARN("fail to swap part group", KR(ret), K(src_bg), K(dst_bg), KPC(largest_pg),
              KPC(smallest_pg));
    } else {
      balance_cnt++;
      bg_offset_++;
    }
  }
  return ret;
}

int ObPartitionBalance::try_balance_ls_disk_(
    const ObArray<ObBalanceGroupInfo *> &bg_info_arr,
    const ObLSDesc &src_ls,
    const ObLSDesc &dest_ls,
    const int64_t part_group_min_size,
    const int64_t data_size_delta,
    int64_t &balance_cnt)
{
  int ret = OB_SUCCESS;
  // find ls_more/ls_less
  ObBalanceGroupInfo *ls_more = nullptr;
  ObBalanceGroupInfo *ls_less = nullptr;
  if (OB_FAIL(get_balance_group_info_(bg_info_arr, src_ls, dest_ls, ls_more, ls_less))) {
    LOG_WARN("fail to get corresponding balance group info", KR(ret), K(bg_info_arr),
            K(src_ls), K(dest_ls));
  } else if (ls_more->get_part_group_count() == 0) {
    // gurantee that inner/extend balance will not be violated after xfer
  } else if (ls_more->get_part_group_count() == ls_less->get_part_group_count() + 1
            && src_ls.get_part_group_count() == dest_ls.get_part_group_count() + 1
            && OB_FAIL(try_xfer_part_group_(*ls_more, *ls_less, part_group_min_size,
                                            data_size_delta, balance_cnt))) {
    LOG_WARN("try to transfer part group fail", KR(ret), KPC(ls_more), KPC(ls_less),
            K(part_group_min_size), K(data_size_delta));
  } else if (ls_less->get_part_group_count() == 0) {
  } else if (balance_cnt == 0 && OB_FAIL(try_swap_part_group_(*ls_more, *ls_less, part_group_min_size,
                                                              data_size_delta, balance_cnt))) {
    LOG_WARN("try to swap part group fail", KR(ret), KPC(ls_more), KPC(ls_less),
            K(part_group_min_size), K(data_size_delta));
  }
  return ret;
}

int ObPartitionBalance::balance_inter_unit_group_disk_min_size_(
    ObUnitGroupLSDesc &unit_group_max,
    ObUnitGroupLSDesc &unit_group_min,
    const int64_t part_group_min_size,
    int64_t &balance_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<ObArray<ObBalanceGroupInfo *> *> bgs;
  if (OB_FAIL(bgs.reserve(bg_map_.size()))) {
    LOG_WARN("fail to reserve for balance group array", KR(ret));
  }
  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    ObArray<ObBalanceGroupInfo *> &bg = iter->second;
    if (OB_FAIL(bgs.push_back(&bg))) {
      LOG_WARN("fail to push back balance groups", KR(ret), K(iter->second));
    }
  }
  for (int64_t idx = bg_offset_, end = bg_offset_ + bgs.count(); OB_SUCC(ret) && idx != end; idx++) {
    const ObArray<ObBalanceGroupInfo *> &bg = *bgs.at(idx % bgs.count());
    int64_t unit_group_delta = unit_group_max.get_data_size() - unit_group_min.get_data_size();
    ObLSDesc *ls_max = nullptr;
    ObLSDesc *ls_min = nullptr;
    if (!check_disk_inbalance_(unit_group_max.get_data_size(), unit_group_min.get_data_size())) {
      break;
    } else if (unit_group_delta <= part_group_min_size) {
      break;
    } else if (OB_FAIL(unit_group_max.get_max_ls_desc(ls_max))) {
      LOG_WARN("failed to get the max ls desc", KR(ret), K(unit_group_max));
    } else if (OB_FAIL(unit_group_min.get_min_ls_desc(ls_min))) {
      LOG_WARN("failed to get the min ls desc", KR(ret), K(unit_group_min));
    } else if (OB_ISNULL(ls_max) || OB_ISNULL(ls_min)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls_desc is nullptr");
    } else {
      unit_group_max.add_data_size(-ls_max->get_data_size());
      unit_group_min.add_data_size(-ls_min->get_data_size());
    }
    if (FAILEDx(try_balance_ls_disk_(bg, *ls_max, *ls_min, part_group_min_size,
                                    unit_group_delta, balance_cnt))) {
      LOG_WARN("try balance disk size of ls failed", KR(ret), K(bg), KPC(ls_max),
              KPC(ls_min), K(part_group_min_size), K(unit_group_delta));
    } else {
      unit_group_max.add_data_size(ls_max->get_data_size());
      unit_group_min.add_data_size(ls_min->get_data_size());
    }
  }
  return ret;
}

int ObPartitionBalance::balance_intra_unit_group_disk_min_size_(
    ObLSDesc &src_ls,
    ObLSDesc &dest_ls,
    const int64_t part_group_min_size,
    int64_t &balance_cnt)
{
  int ret = OB_SUCCESS;
  ObArray<ObArray<ObBalanceGroupInfo *> *> bgs;
  if (OB_FAIL(bgs.reserve(bg_map_.size()))) {
    LOG_WARN("fail to reserve for balance group array", KR(ret));
  }
  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    ObArray<ObBalanceGroupInfo *> &bg = iter->second;
    if (OB_FAIL(bgs.push_back(&bg))) {
      LOG_WARN("fail to push back balance groups", KR(ret), K(iter->second));
    }
  }
  for (int64_t idx = bg_offset_, end = bg_offset_ + bgs.count(); OB_SUCC(ret) && idx != end; idx++) {
    const ObArray<ObBalanceGroupInfo *> &bg = *bgs.at(idx % bgs.count());
    const int64_t ls_delta = src_ls.get_data_size() - dest_ls.get_data_size();
    if (!check_disk_inbalance_(src_ls.get_data_size(), dest_ls.get_data_size())) {
      break;
    } else if (ls_delta <= part_group_min_size) {
      break;
    } else if (OB_FAIL(try_balance_ls_disk_(bg, src_ls, dest_ls, part_group_min_size,
                                            ls_delta, balance_cnt))) {
      LOG_WARN("try balance disk size of ls failed", KR(ret), K(bg), K(src_ls),
              K(dest_ls), K(part_group_min_size), K(ls_delta));
    }
  }
  return ret;
}

int ObPartitionBalance::save_balance_group_stat_()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t affected_rows = 0;
  ObTimeoutCtx ctx;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t trans_timeout = OB_INVALID_TIMESTAMP;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (FALSE_IT(trans_timeout = GCONF.internal_sql_execute_timeout + bg_map_.size() * 100_ms)) {
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, trans_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret), K(trans_timeout));
  } else if (!is_user_tenant(tenant_id_)) {
    // do nothing
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("fail to start trans", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(bg_ls_stat_operator_.delete_balance_group_ls_stat(ctx.get_timeout(), trans, tenant_id_))) {
    LOG_WARN("fail to delete balance group ls stat", KR(ret), K(tenant_id_));
  } else {
    // iterator all balance group
    FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
      common::ObArray<ObBalanceGroupLSStat> bg_ls_stat_array;
      // iterator all ls
      for (int64_t i = 0; OB_SUCC(ret) && i < iter->second.count(); i++) {
        if (OB_NOT_NULL(iter->second.at(i))) {
          ObBalanceGroupLSStat bg_ls_stat;
          if (OB_FAIL(bg_ls_stat.build(tenant_id_,
                                       iter->first.id(),
                                       iter->second.at(i)->get_ls_id(),
                                       iter->second.at(i)->get_part_group_count(),
                                       iter->first.name()))) {
            LOG_WARN("fail to build bg ls stat", KR(ret), K(iter->first), KPC(iter->second.at(i)));
          } else if (OB_FAIL(bg_ls_stat_array.push_back(bg_ls_stat))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(bg_ls_stat_operator_.insert_update_balance_group_ls_stat(ctx.get_timeout(), trans, tenant_id_,
                iter->first.id(), bg_ls_stat_array))) {
          LOG_WARN("fail to insert update balance group ls stat", KR(ret), K(tenant_id_), K(iter->first));
        }
      }
    }
    // commit/abort
    int tmp_ret = OB_SUCCESS;
    const bool is_commit = OB_SUCC(ret);
    if (OB_SUCCESS != (tmp_ret = trans.end(is_commit))) {
      LOG_WARN("trans end failed", K(tmp_ret), K(is_commit));
      ret = (OB_SUCCESS == ret ? tmp_ret : ret);
    }
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("[PART_BALANCE] save_balance_group_stat", K(ret), "cost", end_time - start_time);
  return ret;
}

int ObPartitionBalance::get_balance_group_info_(
    const ObArray<ObBalanceGroupInfo *> &bg_ls_arr,
    const ObLSDesc &ls_more_desc,
    const ObLSDesc &ls_less_desc,
    ObBalanceGroupInfo *&ls_more,
    ObBalanceGroupInfo *&ls_less)
{
  int ret = OB_SUCCESS;
  ls_more = nullptr;
  ls_less = nullptr;
  ARRAY_FOREACH(bg_ls_arr, idx) {
    ObBalanceGroupInfo *bg_info = bg_ls_arr.at(idx);
    if (OB_ISNULL(bg_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("balance group info is null", KR(ret), K(bg_info));
    } else if (bg_info->get_ls_id() == ls_more_desc.get_ls_id()) {
      ls_more = bg_info;
    } else if (bg_info->get_ls_id() == ls_less_desc.get_ls_id()) {
      ls_less = bg_info;
    }
    if (ls_more != nullptr && ls_less != nullptr) {
      break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found ls", KR(ret), K(ls_more), K(ls_less));
  }
  return ret;
}

} // end rootserver
} // end oceanbase
