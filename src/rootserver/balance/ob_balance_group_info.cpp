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

#include "ob_balance_group_info.h"

namespace oceanbase
{
using namespace share;
namespace rootserver
{
int ObTransferPartGroup::add_part(const ObTransferPartInfo &part, int64_t data_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!part.is_valid() || data_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part), K(data_size));
  } else if (OB_FAIL(part_list_.push_back(part))) {
    LOG_WARN("push back part into part info fail", KR(ret), K(part), K(part_list_));
  } else {
    data_size_ += data_size;
  }
  return ret;
}

///////////////////////////////////////////////

ObBalanceGroupInfo::~ObBalanceGroupInfo()
{
  // for each partition group in array, release its memory
  for (int64_t i = 0; i < part_group_buckets_.count(); i++) {
    common::ObArray<ObTransferPartGroup*> &part_groups = part_group_buckets_.at(i);
    for (int64_t j = 0; j < part_groups.count(); j++) { 
      ObTransferPartGroup *part_group = part_groups.at(j);
      if (OB_NOT_NULL(part_group)) {
        part_group->~ObTransferPartGroup();
        alloc_.free(part_group);
        part_group = NULL;
      }
    }
    part_groups.destroy();
  }
  part_group_buckets_.destroy();
  part_group_cnt_ = 0;
  target_ls_num_ = 0;
  last_part_group_uid_ = OB_INVALID_ID;
  last_part_group_ = NULL;
}


int ObBalanceGroupInfo::init(const ObBalanceGroupID &bg_id, const int64_t target_ls_num) {
  int ret = OB_SUCCESS;
  
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!bg_id.is_valid() || target_ls_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bg_id or target_ls_num", KR(ret), K(bg_id), K(target_ls_num));
  } else {
    part_group_buckets_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < target_ls_num; i++) {
      common::ObArray<ObTransferPartGroup *> part_groups(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc_, "PartGroupArray"));
      if (OB_FAIL(part_group_buckets_.push_back(part_groups))) {
        LOG_WARN("failed to push part groups", KR(ret), K(part_groups), K(part_group_buckets_));
      }
    }
    if (OB_SUCC(ret)) {
      id_ = bg_id;
      target_ls_num_ = target_ls_num;
      inited_ = true;
    }
  }
  return ret;
}

int ObBalanceGroupInfo::append_part(ObTransferPartInfo &part,
    const int64_t data_size,
    const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  ObTransferPartGroup *part_group = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!part.is_valid() || data_size < 0 || !is_valid_id(part_group_uid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part), K(data_size));
  } else if (OB_FAIL(create_new_part_group_if_needed_(part_group_uid))) {
    LOG_WARN("create new part group if needed failed", KR(ret), K(part_group_uid), K_(last_part_group_uid));
  } else if (OB_ISNULL(part_group = last_part_group_)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no partition groups in this balance group", KPC(this), KR(ret), K(part));
  } else if (OB_FAIL(part_group->add_part(part, data_size))) {
    LOG_WARN("add part into partition group fail", KR(ret),
        KPC(part_group), K(part), K(data_size), K(part_group_uid), KPC(this));
  } else {
    LOG_TRACE("[ObBalanceGroupInfo] append part", K(part), K(data_size), K(part_group_uid),
        "part_group_count", part_group_cnt_, KPC(part_group));
  }
  return ret;
}

int ObBalanceGroupInfo::create_new_part_group_if_needed_(const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_id(part_group_uid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_group_uid", KR(ret), K(part_group_uid));
  } else if (part_group_uid != last_part_group_uid_) {
    // only create new part group when part_group_uid is different from last_part_group_uid_
    // (Scenarios with invalid last_part_group_uid_ have been included)
    ObTransferPartGroup *part_group = NULL;
    const int64_t part_group_size = sizeof(ObTransferPartGroup);
    void *buf = alloc_.alloc(part_group_size);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for partition group fail", KR(ret), K(buf), K(part_group_size));
    } else if (OB_ISNULL(part_group = new(buf) ObTransferPartGroup(alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("construct ObTransferPartGroup fail", KR(ret), K(buf), K(part_group_size));
    } else {
      int64_t bucket_idx = part_group_uid % target_ls_num_;
      if (bucket_idx < 0 || bucket_idx >= part_group_buckets_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bucket_idx is invalid", KR(ret), K(bucket_idx), K_(part_group_buckets));
      } else if (OB_FAIL(part_group_buckets_.at(bucket_idx).push_back(part_group))) {
        LOG_WARN("failed to push back part group");
      } else {
        last_part_group_uid_ = part_group_uid;
        last_part_group_ = part_group;
        part_group_cnt_ += 1;
      }
    }
  }
  return ret;
}

int ObBalanceGroupInfo::transfer_out(const int64_t part_group_count,
    int64_t bucket_idx,
    ObBalanceGroupInfo &dst_bg_info,
    share::ObTransferPartList &part_list,
    int64_t &removed_part_count)
{
  int ret = OB_SUCCESS;

  removed_part_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dst_bg_info.is_valid()
                        || dst_bg_info.part_group_buckets_.count() != part_group_buckets_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dst_bg_info", KR(ret));
  } else if (OB_UNLIKELY(part_group_count > part_group_cnt_ || part_group_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_group_count", KR(ret), K(part_group_count), K_(part_group_cnt));
  } else if (OB_UNLIKELY(bucket_idx < 0 || bucket_idx >= part_group_buckets_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket_idx", KR(ret));
  } else if (OB_INVALID_ID != last_part_group_uid_) {
    last_part_group_uid_ = OB_INVALID_ID;
    last_part_group_ = NULL;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_group_count; i++) {
    ObTransferPartGroup *pg = NULL;
    // remove part group in bucket `bucket_idx` first
    if (!part_group_buckets_.at(bucket_idx).empty() && OB_FAIL(part_group_buckets_.at(bucket_idx).pop_back(pg))) {
        LOG_WARN("failed to pop back part group", KR(ret), K(part_group_buckets_.at(bucket_idx)));
    }
    // bucket at `bucket_idx` is empty, remove part group from other buckets
    for (int64_t j = bucket_idx + 2; OB_SUCC(ret) && NULL == pg && j < bucket_idx + 2 + part_group_buckets_.size(); j++) {
      ObArray<ObTransferPartGroup*> &part_groups = part_group_buckets_.at(j % part_group_buckets_.size());
      if (!part_groups.empty()) {
        if (OB_FAIL(part_groups.pop_back(pg))) {
          LOG_WARN("failed to pop back part group", KR(ret), K(part_groups), K(pg));
        } else {
          bucket_idx = j % part_group_buckets_.count();
        }
        break;
      }
    }
    if (OB_FAIL(ret)) { // empty
    } else if (OB_ISNULL(pg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid part group, is NULL, unexpected", K(pg), K(i), K(part_group_buckets_));
    } else if (FALSE_IT(removed_part_count += pg->count())) {
    } else if (OB_FAIL(append(part_list, pg->get_part_list()))) {
      LOG_WARN("append array to part list fail", KR(ret), K(part_list), KPC(pg));
    } else if (OB_FAIL(dst_bg_info.append_part_group(bucket_idx, pg))) {
      LOG_WARN("failed to append part group", KR(ret), K(bucket_idx), KPC(pg));
    } else {
      part_group_cnt_--;
    }
  }
  return ret;
}

int ObBalanceGroupInfo::get_bucket_closest_to_empty(int64_t &bucket_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else {
    int64_t closest_to_empty_bucket_size = INT64_MAX;
    int64_t closest_to_empty_bucket_idx = -1;
    for (int64_t i = 0; i < part_group_buckets_.size(); i++) {
      const ObArray<ObTransferPartGroup*> &part_groups = part_group_buckets_.at(i);
      if (!part_groups.empty() && part_groups.size() < closest_to_empty_bucket_size) {
        closest_to_empty_bucket_idx = i;
        closest_to_empty_bucket_size = part_groups.size();
      }
    }
    if (-1 == closest_to_empty_bucket_idx) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get a bucket closest to empty", KR(ret), K_(part_group_buckets));
    } else {
      bucket_idx = closest_to_empty_bucket_idx;
    }
  }
  return ret;
}

int ObBalanceGroupInfo::get_fullest_bucket(int64_t &bucket_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else {
    int64_t fullest_bucket_size = -1;
    int64_t fullest_bucket_idx = -1;
    for (int64_t i = 0; i < part_group_buckets_.size(); i++) {
      const ObArray<ObTransferPartGroup*> &part_groups = part_group_buckets_.at(i);
      if (part_groups.size() > fullest_bucket_size) {
        fullest_bucket_idx = i;
        fullest_bucket_size = part_groups.size();
      }
    }
    if (-1 == fullest_bucket_idx) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get the fullest bucket", KR(ret), K_(part_group_buckets));
    } else {
      bucket_idx = fullest_bucket_idx;
    }
  }
  return ret;
}

int ObBalanceGroupInfo::append_part_group(const int64_t bucket_idx, ObTransferPartGroup *part_group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(bucket_idx < 0 || bucket_idx >= part_group_buckets_.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket idx", KR(ret), K(bucket_idx));
  } else if (OB_ISNULL(part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part_group is null", KR(ret), K(part_group));
  } else if (OB_FAIL(part_group_buckets_.at(bucket_idx).push_back(part_group))) {
    LOG_WARN("failed to push back part group");
  } else {
    part_group_cnt_ += 1;
  }
  return ret;
}

}
}
