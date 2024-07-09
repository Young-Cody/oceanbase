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

#ifndef OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H
#define OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H

#include "lib/container/ob_array.h"           //ObArray
#include "lib/ob_define.h"                    // OB_MALLOC_NORMAL_BLOCK_SIZE
#include "lib/allocator/ob_allocator.h"       // ObIAllocator
#include "share/transfer/ob_transfer_info.h"  // ObTransferPartInfo, ObTransferPartList
#include "ob_balance_group_define.h"          //ObBalanceGroupID
#include "lib/allocator/page_arena.h"         // ModulePageAllocator

namespace oceanbase
{
namespace rootserver
{

// A group of partitions that should be distributed on the same LS and transfered together
class ObTransferPartGroup
{
public:
  ObTransferPartGroup() :
      data_size_(0),
      part_list_("PartGroup") {}

  ObTransferPartGroup(common::ObIAllocator &alloc) :
      data_size_(0),
      part_list_(alloc, "PartGroup") {}

  ~ObTransferPartGroup() {
    data_size_ = 0;
    part_list_.reset();
  }

  int64_t get_data_size() const { return data_size_; }
  const share::ObTransferPartList &get_part_list() const { return part_list_; }
  int64_t count() const { return part_list_.count(); }

  // add new partition into partition group
  int add_part(const share::ObTransferPartInfo &part, int64_t data_size);
  
  TO_STRING_KV(K_(data_size), K_(part_list));
private:
  int64_t data_size_;
  share::ObTransferPartList part_list_;
};

// Balance Group Partition Info
//
// A group of Partition Groups (ObTransferPartGroup) that should be evenly distributed on all LS.
class ObBalanceGroupInfo final
{
public:
  explicit ObBalanceGroupInfo(common::ObIAllocator &alloc) :
      inited_(false),
      id_(),
      last_part_group_uid_(OB_INVALID_ID),
      last_part_group_(NULL),
      alloc_(alloc),
      part_group_buckets_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(alloc, "PartGroupArray")),
      part_group_cnt_(0),
      target_ls_num_(0)
  {
  }

  ~ObBalanceGroupInfo();

  int init(const ObBalanceGroupID &bg_id, const int64_t target_ls_num);

  bool is_valid() { return inited_ && id_.is_valid(); }
  const ObBalanceGroupID &id() const { return id_; }
  int64_t get_part_group_count() const { return part_group_cnt_; }

  // append partition at the newest partition group. create new partition group if needed
  //
  // @param [in] part                         target partition info which will be added
  // @param [in] data_size                    partition data size
  // @param [in] part_group_uid               partition group unique id
  //
  // @return OB_SUCCESS         success
  // @return OB_ENTRY_EXIST     no partition group found
  // @return other              fail
  int append_part(share::ObTransferPartInfo &part,
      const int64_t data_size,
      const uint64_t part_group_uid);

  // transfer out partition groups in bucket `bucket_idx`
  //
  // @param [in] part_group_count           partition group count that need be removed
  // @param [in] bucket_idx                 transfer out partition groups in bucket `bucket_idx`
  // @param [in/out] dst_bg_info            push transfered out part group into dst_bg_info
  // @param [in/out] part_list              push transfered out part into the part list
  // @param [out] removed_part_count        removed partition count
  int transfer_out(const int64_t part_group_count,
      int64_t bucket_idx,
      ObBalanceGroupInfo &dst_bg_info,
      share::ObTransferPartList &part_list,
      int64_t &removed_part_count);

  int get_bucket_closest_to_empty(int64_t &bucket_idx) const;
  int get_fullest_bucket(int64_t &bucket_idx) const;

  TO_STRING_KV(K_(id), "part_group_count", part_group_cnt_);

private:
  int create_new_part_group_if_needed_(const uint64_t part_group_uid);
  int append_part_group(const int64_t bucket_idx, ObTransferPartGroup *part_group);

private:
  bool inited_;
  ObBalanceGroupID id_;
  int64_t last_part_group_uid_; // unique id of the last part group in part_groups_
  ObTransferPartGroup *last_part_group_;
  ObIAllocator &alloc_; // allocator for ObTransferPartGroup
  // Partition Group Array
  common::ObArray<common::ObArray<ObTransferPartGroup *>> part_group_buckets_;
  int64_t part_group_cnt_;
  int64_t target_ls_num_;
};

}
}
#endif /* !OCEANBASE_ROOTSERVER_OB_BALANCE_GROUP_INFO_H */
