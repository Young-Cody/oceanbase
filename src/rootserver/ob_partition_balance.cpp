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
#include "rootserver/ob_ls_service_helper.h"
#include "rootserver/ob_partition_balance.h"
#include "storage/ob_common_id_utils.h"
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
    const ObPartitionScatterMode &scatter_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObPartitionBalance has inited", KR(ret), K(this));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(schema_service) || OB_ISNULL(sql_proxy)
      || (task_mode != GEN_BG_STAT && task_mode != GEN_TRANSFER_TASK)
      || (scatter_mode <= SCATTER_INVALID || scatter_mode >= SCATTER_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObPartitionBalance run", KR(ret), K(tenant_id), K(schema_service), K(sql_proxy),
            K(task_mode), K(scatter_mode));
  } else if (OB_FAIL(bg_builder_.init(tenant_id, "PART_BALANCE", *this, *sql_proxy, *schema_service))) {
    LOG_WARN("init all balance group builder fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(bg_map_.create(40960, lib::ObLabel("PART_BALANCE")))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(transfer_logical_tasks_.create(1024, lib::ObLabel("PART_BALANCE")))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(ls_desc_map_.create(10, lib::ObLabel("PART_BALANCE")))) {
    LOG_WARN("map create fail", KR(ret));
  } else if (OB_FAIL(bg_ls_stat_operator_.init(sql_proxy))) {
    LOG_WARN("init balance group ls stat operator fail", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = sql_proxy;

    //reset
    allocator_.reset();
    ls_desc_array_.reset();
    balance_job_.reset();
    balance_tasks_.reset();
    task_mode_ = task_mode;
    scatter_mode_ = scatter_mode;

    primary_zone_num_ = primary_zone_num;
    unit_group_num_ = unit_group_num;
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
    ObArray<ObBalanceGroupInfo*> &bg_ls_arr = iter->second;
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
  bg_builder_.destroy();
  allocator_.reset();
  ls_desc_array_.reset();
  ls_desc_map_.destroy();
  bg_map_.destroy();
  transfer_logical_tasks_.destroy();
  balance_job_.reset();
  balance_tasks_.reset();
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = NULL;
  task_mode_ = GEN_BG_STAT;
  scatter_mode_ = SCATTER_INVALID;
  primary_zone_num_ = -1;
  unit_group_num_ = -1;
}

int ObPartitionBalance::process()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  } else if (OB_FAIL(prepare_balance_group_())) {
    LOG_WARN("prepare_balance_group fail", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(save_balance_group_stat_())) {
    LOG_WARN("save balance group stat fail", KR(ret), K(tenant_id_));
  } else if (GEN_BG_STAT == task_mode_) {
    // finish
  } else if (bg_map_.empty()) {
    LOG_INFO("PART_BALANCE balance group is empty do nothing", K(tenant_id_));
  } else if (transfer_logical_tasks_.empty() && OB_FAIL(process_balance_partition_inner_())) {
    LOG_WARN("process_balance_partition_inner fail", KR(ret), K(tenant_id_));
  } else if (transfer_logical_tasks_.empty() && OB_FAIL(process_balance_partition_extend_())) {
    LOG_WARN("process_balance_partition_extend fail", KR(ret), K(tenant_id_));
  } else if (transfer_logical_tasks_.empty() && OB_FAIL(process_balance_partition_disk_())) {
    LOG_WARN("process_balance_partition_disk fail", KR(ret), K(tenant_id_));
  } else if (!transfer_logical_tasks_.empty() && OB_FAIL(generate_balance_job_from_logical_task_())) {
    LOG_WARN("generate_balance_job_from_logical_task_ fail", KR(ret), K(tenant_id_));
  }
  int64_t end_time = ObTimeUtility::current_time();
  LOG_INFO("PART_BALANCE process", KR(ret), K(tenant_id_), K(task_mode_),
          "cost", end_time - start_time, "task_cnt", transfer_logical_tasks_.size());
  return ret;
}

int ObPartitionBalance::prepare_ls_()
{
  int ret = OB_SUCCESS;
  ObLSAttrArray ls_attr_array;
  ObLSAttrOperator ls_operator(tenant_id_, sql_proxy_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  } else if (OB_FAIL(ls_operator.get_all_ls_by_order(ls_attr_array))) {
    LOG_WARN("get_all_ls fail", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_attr_array.count(); i++) {
      ObLSDesc *ls_desc = nullptr;
      if (0 == ls_attr_array.at(i).get_ls_group_id()) {
        // skip sys ls and duplicate ls
      } else if (!ls_attr_array.at(i).ls_is_normal()) { // TODO inclue wait offline status
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("ls is not in normal status", KR(ret), K(ls_attr_array.at(i).get_ls_id()));
      } else if (OB_ISNULL(ls_desc = reinterpret_cast<ObLSDesc*>(allocator_.alloc(sizeof(ObLSDesc))))) {
         ret = OB_ALLOCATE_MEMORY_FAILED;
         LOG_WARN("alloc mem fail", KR(ret));
      } else if (FALSE_IT(new(ls_desc) ObLSDesc(ls_attr_array.at(i).get_ls_id(), ls_attr_array.at(i).get_ls_group_id()))) {
      } else if (OB_FAIL(ls_desc_array_.push_back(ls_desc))) {
        LOG_WARN("push_back ls_desc to array fail", KR(ret), K(ls_desc->get_ls_id()));
      } else if (OB_FAIL(ls_desc_map_.set_refactored(ls_desc->get_ls_id(), ls_desc))) {
        LOG_WARN("init ls_desc to map fail", KR(ret), K(ls_desc->get_ls_id()));
      }
    }
    if (OB_SUCC(ret) && ls_desc_array_.count() == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no ls can assign", KR(ret), K(tenant_id_));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[PART_BALANCE] prepare_ls", K(tenant_id_), K(ls_desc_array_));
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
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not inited", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!bg_in.is_valid() || !table_schema.is_valid()
            || !is_valid_id(part_object_id) || !is_valid_id(part_group_uid)
            || !src_ls_id.is_valid_with_tenant(tenant_id_)
            || !dest_ls_id.is_valid_with_tenant(tenant_id_)
            || tablet_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_in), K(table_schema), K(part_object_id),
            K(src_ls_id), K(dest_ls_id), K(tablet_size), K(part_group_uid));
  } else if (OB_FAIL(ls_desc_map_.get_refactored(src_ls_id, src_ls_desc))) {
    LOG_WARN("get LS desc fail", KR(ret), K(src_ls_id));
  } else if (OB_ISNULL(src_ls_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not found LS", KR(ret), K(src_ls_id), KPC(src_ls_desc));
  } else if (OB_FAIL(part_info.init(table_schema.get_table_id(), part_object_id))) {
    LOG_WARN("part_info init fail", KR(ret), K(table_schema.get_table_id()), K(part_object_id));
  }
  // if dest_ls_id differs from src_ls_id, need generate balance task
  else if (dest_ls_id != src_ls_id) {
    ObTransferPartGroup tmp_part_group;
    if (OB_FAIL(tmp_part_group.add_part(part_info, tablet_size))) {
      LOG_WARN("part group add part fail", KR(ret), K(part_info), K(tablet_size));
    } else if (OB_FAIL(add_transfer_task_(src_ls_id, dest_ls_id, &tmp_part_group,
                                          false/*update_ls_desc_info*/))) {
      LOG_WARN("add new transfer task fail", KR(ret), K(src_ls_id), K(dest_ls_id));
    }
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
          if (OB_ISNULL(bg_info = reinterpret_cast<ObBalanceGroupInfo*>(
              allocator_.alloc(sizeof(ObBalanceGroupInfo))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret));
          } else if (OB_ISNULL(bg_info = new(bg_info) ObBalanceGroupInfo(allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc mem fail", KR(ret));
          } else if (OB_FAIL(bg_info->init(bg.id(), ls_desc_array_.at(i)->get_ls_id(),
                                          ls_desc_array_.count(), scatter_mode_))) {
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
    int64_t part_group_sum = 0;
    ObArray<ObBalanceGroupInfo*> &bg_ls_array = iter->second;
    int64_t ls_cnt = bg_ls_array.count();
    for (int64_t ls_idx = 0; ls_idx < ls_cnt; ls_idx++) {
      part_group_sum += bg_ls_array.at(ls_idx)->get_part_group_count();
    }
    while (OB_SUCC(ret)) {
      lib::ob_sort(bg_ls_array.begin(), bg_ls_array.end(),
                  [] (ObBalanceGroupInfo* left, ObBalanceGroupInfo* right) {
        if (left->get_part_group_count() < right->get_part_group_count()) {
          return true;
        } else if (left->get_part_group_count() == right->get_part_group_count()) {
          if (left->get_ls_id() > right->get_ls_id()) {
            return true;
          }
        }
        return false;
      });
      ObBalanceGroupInfo *ls_max = bg_ls_array.at(ls_cnt - 1);
      ObBalanceGroupInfo *ls_min = bg_ls_array.at(0);
      //If difference in number of partition groups between LS does not exceed 1, this balance group is balanced
      if (ls_max->get_part_group_count() - ls_min->get_part_group_count() <= 1) {
        // balance group has done
        break;
      }
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

      ObBalanceGroupInfo *ls_more = nullptr;
      int64_t ls_more_dest = 0;
      ObBalanceGroupInfo *ls_less = nullptr;
      int64_t ls_less_dest = 0;
      for (int64_t ls_idx = bg_ls_array.count() - 1; ls_idx >= 0; ls_idx--) {
        int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
            ? part_group_sum / ls_cnt
            : part_group_sum / ls_cnt + 1;;
        if (bg_ls_array.at(ls_idx)->get_part_group_count() > part_dest) {
          ls_more = bg_ls_array.at(ls_idx);
          ls_more_dest = part_dest;
          break;
        }
      }
      for (int64_t ls_idx = 0; ls_idx < bg_ls_array.count(); ls_idx++) {
        int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
            ? part_group_sum / ls_cnt
            : part_group_sum / ls_cnt + 1;;
        if (bg_ls_array.at(ls_idx)->get_part_group_count() < part_dest) {
          ls_less = bg_ls_array.at(ls_idx);
          ls_less_dest = part_dest;
          break;
        }
      }
      if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not found dest ls", KR(ret));
        break;
      }
      int64_t transfer_cnt = MIN(ls_more->get_part_group_count() - ls_more_dest,
                                ls_less_dest - ls_less->get_part_group_count());
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
  LOG_INFO("[PART_BALANCE] process_balance_partition_inner end", K(tenant_id_), K(ls_desc_array_));
  return ret;
}

int ObPartitionBalance::add_transfer_task_(
    const ObLSID &src_ls_id,
    const ObLSID &dest_ls_id,
    ObTransferPartGroup *part_group,
    bool modify_ls_desc)
{
  int ret = OB_SUCCESS;
  ObTransferTaskKey task_key(src_ls_id, dest_ls_id);
  ObTransferPartList *tansfer_part_info = transfer_logical_tasks_.get(task_key);
  if (OB_ISNULL(tansfer_part_info)) {
    ObTransferPartList part_arr;
    if (OB_FAIL(transfer_logical_tasks_.set_refactored(task_key, part_arr))) {
      LOG_WARN("fail to init transfer task into map", KR(ret), K(task_key), K(part_arr));
    } else {
      tansfer_part_info = transfer_logical_tasks_.get(task_key);
      if (OB_ISNULL(tansfer_part_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail get transfer task from map", KR(ret), K(task_key));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_group->get_part_list().count(); i++) {
      if (OB_FAIL(tansfer_part_info->push_back(part_group->get_part_list().at(i)))) {
        LOG_WARN("fail to push part info into transfer part array", KR(ret),
            K(part_group->get_part_list().at(i)));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (modify_ls_desc && OB_FAIL(update_ls_desc_(src_ls_id, -1, part_group->get_data_size() * -1))) {
   LOG_WARN("update_ls_desc", KR(ret), K(src_ls_id));
  } else if (modify_ls_desc && OB_FAIL(update_ls_desc_(dest_ls_id, 1, part_group->get_data_size()))) {
   LOG_WARN("update_ls_desc", KR(ret), K(dest_ls_id));
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
  uint64_t part_group_sum = 0;
  ObIPartGroupInfo *pg_info = nullptr;
  for (int64_t i = 0; i < ls_cnt; i++) {
    part_group_sum += ls_desc_array_.at(i)->get_partgroup_cnt();
  }
  while (OB_SUCC(ret)) {
    lib::ob_sort(ls_desc_array_.begin(), ls_desc_array_.end(), [] (ObLSDesc *l, ObLSDesc *r) {
      if (l->get_partgroup_cnt() < r->get_partgroup_cnt()) {
        return true;
      } else if (l->get_partgroup_cnt() == r->get_partgroup_cnt()) {
        if (l->get_data_size() < r->get_data_size()) {
          return true;
        } else if (l->get_data_size() == r->get_data_size()) {
          if (l->get_ls_id() > r->get_ls_id()) {
            return true;
          }
        }
      }
      return false;
    });

    ObLSDesc *ls_max = ls_desc_array_.at(ls_cnt - 1);
    ObLSDesc *ls_min = ls_desc_array_.at(0);
    if (ls_max->get_partgroup_cnt() - ls_min->get_partgroup_cnt() <= 1) {
      break;
    }
    /*
     * example: ls1  ls2  ls3  ls4
     * bg1      3    3    3    4
     * bg2      5    5    5    6
     *
     * we should move ls4.bg1 -> ls3 or ls4.bg2 -> ls3
     */
    ObLSDesc *ls_more_desc = nullptr;
    int64_t ls_more_dest = 0;
    ObLSDesc *ls_less_desc = nullptr;
    int64_t ls_less_dest = 0;
    for (int64_t ls_idx = ls_desc_array_.count() - 1; ls_idx >= 0; ls_idx--) {
      int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
          ? part_group_sum / ls_cnt
          : part_group_sum / ls_cnt + 1;;
      if (ls_desc_array_.at(ls_idx)->get_partgroup_cnt() > part_dest) {
        ls_more_desc = ls_desc_array_.at(ls_idx);
        ls_more_dest = part_dest;
        break;
      }
    }
    for (int64_t ls_idx = 0; ls_idx < ls_desc_array_.count(); ls_idx++) {
      int64_t part_dest = ls_idx < (ls_cnt - (part_group_sum % ls_cnt))
          ? part_group_sum / ls_cnt
          : part_group_sum / ls_cnt + 1;;
      if (ls_desc_array_.at(ls_idx)->get_partgroup_cnt() < part_dest) {
        ls_less_desc = ls_desc_array_.at(ls_idx);
        ls_less_dest = part_dest;
        break;
      }
    }
    if (OB_ISNULL(ls_more_desc) || OB_ISNULL(ls_less_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not found dest ls", KR(ret), K(ls_more_desc), K(ls_less_desc));
      break;
    }
    int64_t transfer_cnt = MIN(ls_more_desc->get_partgroup_cnt() - ls_more_dest,
                              ls_less_dest - ls_less_desc->get_partgroup_cnt());
    for (int64_t transfer_idx = 0; OB_SUCC(ret) && transfer_idx < transfer_cnt; transfer_idx++) {
      FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
        // find ls_more/ls_less
        ObBalanceGroupInfo *ls_more = nullptr;
        ObBalanceGroupInfo *ls_less = nullptr;
        for (int64_t idx = 0; OB_SUCC(ret) && idx < iter->second.count(); idx++) {
          if (iter->second.at(idx)->get_ls_id() == ls_more_desc->get_ls_id()) {
            ls_more = iter->second.at(idx);
          } else if (iter->second.at(idx)->get_ls_id() == ls_less_desc->get_ls_id()) {
            ls_less = iter->second.at(idx);
          }
          if (ls_more != nullptr && ls_less != nullptr) {
            break;
          }
        }
        if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not found ls", KR(ret));
          break;
        }
        if (ls_more->get_part_group_count() > ls_less->get_part_group_count()) {
          ObTransferPartGroup *part_group = NULL;
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
          break;
        }
      }
    }
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_extend end", K(ret), K(tenant_id_), K(ls_desc_array_));
  return ret;
}

int ObPartitionBalance::process_balance_partition_disk_()
{
  int ret = OB_SUCCESS;
  int64_t ls_cnt = ls_desc_array_.count();
  while (OB_SUCC(ret)) {
    lib::ob_sort(ls_desc_array_.begin(), ls_desc_array_.end(), [] (ObLSDesc *l, ObLSDesc *r) {
      if (l->get_data_size() < r->get_data_size()) {
        return true;
      } else if (l->get_data_size() == r->get_data_size()) {
        if (l->get_ls_id() > r->get_ls_id()) {
          return true;
        }
      }
      return false;
    });

    ObLSDesc &ls_max = *ls_desc_array_.at(ls_cnt - 1);
    ObLSDesc &ls_min = *ls_desc_array_.at(0);
    if (!check_ls_need_swap_(ls_max.get_data_size(), ls_min.get_data_size())) {
      // disk has balance
      break;
    }
    LOG_INFO("[PART_BALANCE] disk_balance", K(ls_max), K(ls_min));

    /*
     * select swap part_group by size segment
     */
    int64_t swap_cnt = 0;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < sizeof(PART_GROUP_SIZE_SEGMENT) / sizeof(PART_GROUP_SIZE_SEGMENT[0]); idx++) {
      if (OB_FAIL(try_swap_part_group_(ls_max, ls_min, PART_GROUP_SIZE_SEGMENT[idx], swap_cnt))) {
        LOG_WARN("try_swap_part_group fail", KR(ret), K(ls_max), K(ls_min), K(PART_GROUP_SIZE_SEGMENT[idx]));
      } else if (swap_cnt > 0) {
        break;
      }
    }
    if (swap_cnt == 0) {
      // nothing to do
      break;
    }
  }
  LOG_INFO("[PART_BALANCE] process_balance_partition_disk end", KR(ret), K(tenant_id_), K(ls_desc_array_));
  return ret;
}

bool ObPartitionBalance::check_ls_need_swap_(uint64_t ls_more_size, uint64_t ls_less_size)
{
  bool need_swap = false;
  if (ls_more_size > PART_BALANCE_THRESHOLD_SIZE) {
    if ((ls_more_size - ls_more_size * GCONF.balancer_tolerance_percentage / 100) > ls_less_size) {
      need_swap = true;
    }
  }
  return need_swap;
}

int ObPartitionBalance::try_swap_part_group_(
    ObLSDesc &src_ls,
    ObLSDesc &dest_ls,
    int64_t part_group_min_size,
    int64_t &swap_cnt)
{
  int ret = OB_SUCCESS;
  ObIPartGroupInfo *largest_pg = nullptr;
  ObIPartGroupInfo *smallest_pg = nullptr;
  FOREACH_X(iter, bg_map_, OB_SUCC(ret)) {
    if (!check_ls_need_swap_(src_ls.get_data_size(), dest_ls.get_data_size())) {
      break;
    }
    // find ls_more/ls_less
    ObBalanceGroupInfo *ls_more = nullptr;
    ObBalanceGroupInfo *ls_less = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < iter->second.count(); idx++) {
      if (iter->second.at(idx)->get_ls_id() == src_ls.get_ls_id()) {
        ls_more = iter->second.at(idx);
      } else if (iter->second.at(idx)->get_ls_id() == dest_ls.get_ls_id()) {
        ls_less = iter->second.at(idx);
      }
      if (ls_more != nullptr && ls_less != nullptr) {
        break;
      }
    }
    if (OB_ISNULL(ls_more) || OB_ISNULL(ls_less)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not found ls", KR(ret), K(ls_more), K(ls_less));
      break;
    }
    if (ls_more->get_part_group_count() == 0 || ls_less->get_part_group_count() == 0) {
      continue;
    }
    int64_t swap_size = 0;
    if (OB_FAIL(ls_more->get_largest_part_group(largest_pg))) {
      LOG_WARN("failed to get the largest part group", KR(ret), KPC(ls_more));
    } else if (OB_FAIL(ls_less->get_smallest_part_group(smallest_pg))) {
      LOG_WARN("failed to get the smallest part group", KR(ret), KPC(ls_less));
    } else if (OB_UNLIKELY(nullptr == largest_pg || nullptr == smallest_pg
                          || !largest_pg->is_valid() || !smallest_pg->is_valid()
                          || nullptr == largest_pg->part_group()
                          || nullptr == smallest_pg->part_group())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("largest or smallest part group info is invalid", KR(ret), KPC(largest_pg),
              KPC(smallest_pg));
    } else if (FALSE_IT(swap_size = largest_pg->part_group()->get_data_size() -
                                    smallest_pg->part_group()->get_data_size())) { // empty
    } else if (swap_size >= part_group_min_size
              && src_ls.get_data_size() - dest_ls.get_data_size() > swap_size) {
      LOG_INFO("[PART_BALANCE] swap_partition", KPC(largest_pg->part_group()),
              KPC(smallest_pg->part_group()), K(src_ls), K(dest_ls),
              K(swap_size), K(part_group_min_size));
      if (OB_FAIL(add_transfer_task_(ls_more->get_ls_id(), ls_less->get_ls_id(),
                                    largest_pg->part_group()))) {
        LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()), K(ls_less->get_ls_id()));
      } else if (OB_FAIL(ls_less->append_part_group(*largest_pg))) {
        LOG_WARN("push_back fail", KR(ret), K(ls_less), K(largest_pg));
      } else if (OB_FAIL(ls_more->remove_part_group(*largest_pg))) {
        LOG_WARN("parts remove", KR(ret), K(ls_more), K(largest_pg));
      } else if (OB_FAIL(add_transfer_task_(ls_less->get_ls_id(), ls_more->get_ls_id(),
                                            smallest_pg->part_group()))) {
        LOG_WARN("add_transfer_task_ fail", KR(ret), K(ls_more->get_ls_id()), K(ls_less->get_ls_id()));
      } else if (OB_FAIL(ls_more->append_part_group(*smallest_pg))) {
        LOG_WARN("push_back fail", KR(ret), K(ls_more));
      } else if (OB_FAIL(ls_less->remove_part_group(*smallest_pg))) {
        LOG_WARN("parts remove", KR(ret), K(ls_less));
      } else {
        swap_cnt++;
      }
    }
  }
  return ret;
}

int ObPartitionBalance::generate_balance_job_from_logical_task_()
{
  int ret = OB_SUCCESS;

  ObBalanceJobID job_id;
  ObBalanceJobStatus job_status(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
  ObBalanceJobType job_type(ObBalanceJobType::BALANCE_JOB_PARTITION);
  const char* balance_stradegy = "partition balance"; // TODO
  ObString comment;

  if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id_, job_id))) {
    LOG_WARN("gen_unique_id", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(balance_job_.init(tenant_id_, job_id, job_type, job_status, primary_zone_num_, unit_group_num_,
          comment, ObString(balance_stradegy)))) {
    LOG_WARN("job init fail", KR(ret), K(tenant_id_), K(job_id));
  } else {
    FOREACH_X(iter, transfer_logical_tasks_, OB_SUCC(ret)) {
      ObLSDesc *src_ls = nullptr;
      ObLSDesc *dest_ls = nullptr;
      if (OB_FAIL(ls_desc_map_.get_refactored(iter->first.get_src_ls_id(), src_ls))) {
        LOG_WARN("get_refactored", KR(ret), K(iter->first.get_src_ls_id()));
      } else if (OB_FAIL(ls_desc_map_.get_refactored(iter->first.get_dest_ls_id(), dest_ls))) {
        LOG_WARN("get_refactored", KR(ret), K(iter->first.get_dest_ls_id()));
      } else if (OB_ISNULL(src_ls) || OB_ISNULL(dest_ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not find ls", KR(ret), K(src_ls), K(dest_ls));
      } else if (OB_FAIL(transfer_logical_task_to_balance_task(tenant_id_,
          balance_job_.get_job_id(), src_ls->get_ls_id(), dest_ls->get_ls_id(),
          src_ls->get_ls_group_id(), dest_ls->get_ls_group_id(), iter->second,
          balance_tasks_))) {
        LOG_WARN("failed to transfer logical task", KR(ret), K(tenant_id_), K(balance_job_),
                  KPC(src_ls), KPC(dest_ls), K(iter->second));
      }
    }
  }
  return ret;
}

#define GENERATE_TASK(balance_type, src_ls, dest_ls, ls_group_id, part_list)             \
  do {                                                                        \
    if (OB_SUCC(ret)) {                                                       \
      ObBalanceTask task;                                                     \
      ObBalanceTaskID task_id;                                                \
      if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id, task_id))) {      \
        LOG_WARN("gen_unique_id", KR(ret), K(tenant_id));                     \
      } else if (OB_FAIL(task.simple_init(tenant_id, balance_job_id, task_id, \
                                          balance_type, ls_group_id, src_ls,   \
                                          dest_ls, part_list))) {             \
        LOG_WARN("init task fail", KR(ret), K(tenant_id), K(balance_job_id),  \
                 K(task_id), K(ls_group_id), K(src_ls), K(dest_ls),           \
                 K(part_list));                                               \
      } else if (OB_FAIL(task_array.push_back(task))) {                       \
        LOG_WARN("push_back fail", KR(ret), K(task));                         \
      }                                                                       \
    }                                                                         \
  } while (0)

int ObPartitionBalance::transfer_logical_task_to_balance_task(
    const uint64_t tenant_id,
    const ObBalanceJobID &balance_job_id,
    const ObLSID &src_ls, const ObLSID &dest_ls,
    const uint64_t src_ls_group, const uint64_t dest_ls_group,
    const ObTransferPartList &part_list,
    ObArray<ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  //can not reset task array
  if (OB_UNLIKELY(!balance_job_id.is_valid() || !src_ls.is_valid()
      || !dest_ls.is_valid() || OB_INVALID_ID == src_ls_group
      || OB_INVALID_ID == dest_ls_group || 0 >= part_list.count()
      || src_ls == dest_ls || !is_valid_tenant_id(tenant_id))
      || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(balance_job_id), K(src_ls),
            K(dest_ls), K(src_ls_group), K(dest_ls_group), K(part_list),
            K(tenant_id), KP(GCTX.sql_proxy_));
  } else if (src_ls_group == dest_ls_group) {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_TRANSFER);
    GENERATE_TASK(task_type, src_ls, dest_ls, src_ls_group, part_list);
  } else {
    ObLSID tmp_ls_id;
    const ObTransferPartList empty_part_list;//for alter and merge
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_SPLIT);
    if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(GCTX.sql_proxy_, tenant_id, tmp_ls_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id));
    } else {
      GENERATE_TASK(task_type, src_ls, tmp_ls_id, src_ls_group, part_list);
    }
     // alter
    if (OB_SUCC(ret)) {
      ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_ALTER);
      GENERATE_TASK(task_type, tmp_ls_id, tmp_ls_id, dest_ls_group, empty_part_list);
    }
    // merge
    if (OB_SUCC(ret)) {
      ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_MERGE);
      GENERATE_TASK(task_type, tmp_ls_id, dest_ls, dest_ls_group, empty_part_list);
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
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObPartitionBalance not init", KR(ret), K(this));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
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

//////////////////////////////////////////////////////////

int ObPartitionHelper::check_partition_option(
    const schema::ObSimpleTableSchemaV2 &t1,
    const schema::ObSimpleTableSchemaV2 &t2,
    bool is_subpart,
    bool &is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = false;
  if (OB_FAIL(share::schema::ObSimpleTableSchemaV2::compare_partition_option(t1, t2, is_subpart, is_matched))) {
    LOG_WARN("fail to compare partition optition", KR(ret));
  }
  return ret;
}

int ObPartitionHelper::get_part_info(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, ObPartInfo &part_info)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  ObObjectID part_id;
  ObObjectID first_level_part_id;
  if (OB_FAIL(table_schema.get_tablet_and_object_id_by_index(part_idx, -1, tablet_id, part_id, first_level_part_id))) {
    LOG_WARN("fail to get_tablet_and_object_id_by_index", KR(ret), K(table_schema), K(part_idx));
  } else if (OB_FAIL(part_info.init(tablet_id, part_id))) {
    LOG_WARN("fail init part_info", KR(ret), K(tablet_id), K(part_id));
  }
  return ret;
}

int ObPartitionHelper::get_sub_part_num(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, int64_t &sub_part_num)
{
  int ret = OB_SUCCESS;
  const schema::ObPartition *partition = NULL;
  if (OB_FAIL(table_schema.get_partition_by_partition_index(part_idx, schema::CHECK_PARTITION_MODE_NORMAL, partition))) {
    LOG_WARN("fail to get partition by part_idx", KR(ret), K(part_idx));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition not exist", KR(ret), K(table_schema), K(part_idx));
  } else {
    sub_part_num = partition->get_sub_part_num();
  }
  return ret;
}

int ObPartitionHelper::get_sub_part_info(const schema::ObSimpleTableSchemaV2 &table_schema, int64_t part_idx, int64_t sub_part_idx, ObPartInfo &part_info)
{
  int ret = OB_SUCCESS;

  ObTabletID tablet_id;
  ObObjectID part_id;
  ObObjectID first_level_part_id;
  if (OB_FAIL(table_schema.get_tablet_and_object_id_by_index(part_idx, sub_part_idx, tablet_id, part_id, first_level_part_id))) {
    LOG_WARN("fail to get_tablet_and_object_id_by_index", KR(ret), K(table_schema), K(part_idx), K(sub_part_idx));
  } else if (OB_FAIL(part_info.init(tablet_id, part_id))) {
    LOG_WARN("fail init part_info", KR(ret), K(tablet_id), K(part_id));
  }

  return ret;
}

} // end rootserver
} // end oceanbase
