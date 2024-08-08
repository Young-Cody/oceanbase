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

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>

#define  private public
#define  protected public

#include "rootserver/ob_partition_balance.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver{

#define GB (1024L * 1024L * 1024L)

class TestPartitionBalance : public testing::Test
{
public:
  TestPartitionBalance() {}
  virtual ~TestPartitionBalance(){}
  virtual void SetUp() {};
  virtual void TearDown() {}
  virtual void TestBody() {}
protected:
  ObMultiVersionSchemaService multi_schema_service_;
  ObMySQLProxy sql_proxy_;
};

TEST_F(TestPartitionBalance, disk_balance_inter1)
{
  int ret = OB_SUCCESS;
  ObPartitionBalance part_balance;
  uint64_t tenant_id;
  ret = part_balance.init(OB_SYS_TENANT_ID, &multi_schema_service_, &sql_proxy_, 2, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  part_balance.tenant_id_ = 1002;
  int64_t ls_id = 1001;
  uint64_t ls_group_ids[2] = {1001, 1002};
  for (int64_t i = 0; i < 2; i++) {
    for (int64_t j = 0; j < 2; j++) {
      ObLSDesc *ls_desc = nullptr;
      ls_desc = reinterpret_cast<ObLSDesc*>(
          part_balance.allocator_.alloc(sizeof(ObLSDesc)));
      ASSERT_NE(ls_desc, nullptr);
      new(ls_desc) ObLSDesc(ObLSID(ls_id), ls_group_ids[i], ls_group_ids[i]);
      ret = part_balance.ls_desc_array_.push_back(ls_desc);
      ASSERT_EQ(ret, OB_SUCCESS);
      ret = part_balance.ls_desc_map_.set_refactored(ObLSID(ls_id), ls_desc);
      ASSERT_EQ(ret, OB_SUCCESS);
      ls_id++;
    }
  }
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1001), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1002), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1003), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1004), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  // ls 1001: 1000     ls 1001: 800
  // ls 1002: 1000     ls 1002: 1000

  // ls 1003: 800   => ls 1003: 1000
  // ls 1004: 800      ls 1004: 800
  ObBalanceGroup bg;
  ObSimpleTableSchemaV2 table_schema;
  bg.id_ = ObBalanceGroupID(1, 0);
  bg.name_ = "bg";
  table_schema.tenant_id_ = 1001;
  table_schema.table_id_ = 500001;
  table_schema.table_name_ = "part_table";
  table_schema.database_id_ = 0;
  ret = part_balance.on_new_partition(bg, table_schema, 500002, ObLSID(1001), ObLSID(1001), 1000 * GB, true, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500003, ObLSID(1002), ObLSID(1002), 1000 * GB, true, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500004, ObLSID(1003), ObLSID(1003), 800 * GB, true, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500005, ObLSID(1004), ObLSID(1004), 800 * GB, true, 3);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->partgroup_cnt_, 1);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 1000 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 1000 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 800 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->data_size_, 800 * GB);

  ObArray<ObUnitGroupLSDesc *> unit_group_ls_desc_arr;
  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 2);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 2000 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(1)->get_data_size(), 1600 * GB);

  // inter unit group disk balance
  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_TRUE(part_balance.job_generator_.need_gen_job());

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->partgroup_cnt_, 1);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 800 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 1000 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 1000 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->data_size_, 800 * GB);

  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 2);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 1800 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(1)->get_data_size(), 1800 * GB);


  part_balance.job_generator_.normal_to_normal_part_map_.clear();
  ASSERT_FALSE(part_balance.job_generator_.need_gen_job());

  // intra unit group disk balance
  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(part_balance.job_generator_.need_gen_job());
}

TEST_F(TestPartitionBalance, disk_balance_inter2)
{
  int ret = OB_SUCCESS;
  ObPartitionBalance part_balance;
  uint64_t tenant_id;
  ret = part_balance.init(OB_SYS_TENANT_ID, &multi_schema_service_, &sql_proxy_, 2, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  part_balance.tenant_id_ = 1002;
  int64_t ls_id = 1001;
  uint64_t ls_group_ids[2] = {1001, 1002};
  for (int64_t i = 0; i < 2; i++) {
    for (int64_t j = 0; j < 2; j++) {
      ObLSDesc *ls_desc = nullptr;
      ls_desc = reinterpret_cast<ObLSDesc*>(
          part_balance.allocator_.alloc(sizeof(ObLSDesc)));
      ASSERT_NE(ls_desc, nullptr);
      new(ls_desc) ObLSDesc(ObLSID(ls_id), ls_group_ids[i], ls_group_ids[i]);
      ret = part_balance.ls_desc_array_.push_back(ls_desc);
      ASSERT_EQ(ret, OB_SUCCESS);
      ret = part_balance.ls_desc_map_.set_refactored(ObLSID(ls_id), ls_desc);
      ASSERT_EQ(ret, OB_SUCCESS);
      ls_id++;
    }
  }
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1001), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1002), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1003), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1004), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  // ls 1001: 500 500     ls 1001: 500
  // ls 1002: 500 500     ls 1002: 500 500

  // ls 1003: 500     =>  ls 1003: 500 500
  // ls 1004: 500         ls 1004: 500
  ObBalanceGroup bg;
  ObSimpleTableSchemaV2 table_schema;
  bg.id_ = ObBalanceGroupID(1, 0);
  bg.name_ = "bg";
  table_schema.tenant_id_ = 1001;
  table_schema.table_id_ = 500001;
  table_schema.table_name_ = "part_table";
  table_schema.database_id_ = 0;
  ret = part_balance.on_new_partition(bg, table_schema, 500002, ObLSID(1001), ObLSID(1001), 500 * GB, true, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500003, ObLSID(1001), ObLSID(1001), 500 * GB, true, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500004, ObLSID(1002), ObLSID(1002), 500 * GB, true, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500005, ObLSID(1002), ObLSID(1002), 500 * GB, true, 3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500006, ObLSID(1003), ObLSID(1003), 500 * GB, true, 4);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500007, ObLSID(1004), ObLSID(1004), 500 * GB, true, 5);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->partgroup_cnt_, 1);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 1000 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 1000 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 500 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->data_size_, 500 * GB);

  ObArray<ObUnitGroupLSDesc *> unit_group_ls_desc_arr;
  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 2);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 2000 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(1)->get_data_size(), 1000 * GB);

  // inter unit group disk balance
  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_TRUE(part_balance.job_generator_.need_gen_job());

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->partgroup_cnt_, 1);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 500 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 1000 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 1000 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->data_size_, 500 * GB);

  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 2);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 1500 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(1)->get_data_size(), 1500 * GB);


  part_balance.job_generator_.normal_to_normal_part_map_.clear();
  ASSERT_FALSE(part_balance.job_generator_.need_gen_job());

  // intra unit group disk balance
  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(part_balance.job_generator_.need_gen_job());
}

TEST_F(TestPartitionBalance, disk_balance_inter3)
{
  int ret = OB_SUCCESS;
  ObPartitionBalance part_balance;
  uint64_t tenant_id;
  ret = part_balance.init(OB_SYS_TENANT_ID, &multi_schema_service_, &sql_proxy_, 3, 3);
  ASSERT_EQ(ret, OB_SUCCESS);
  part_balance.tenant_id_ = 1002;
  int64_t ls_id = 1001;
  uint64_t ls_group_ids[3] = {1001, 1002, 1003};
  for (int64_t i = 0; i < 3; i++) {
    for (int64_t j = 0; j < 3; j++) {
      ObLSDesc *ls_desc = nullptr;
      ls_desc = reinterpret_cast<ObLSDesc*>(
          part_balance.allocator_.alloc(sizeof(ObLSDesc)));
      ASSERT_NE(ls_desc, nullptr);
      new(ls_desc) ObLSDesc(ObLSID(ls_id), ls_group_ids[i], ls_group_ids[i]);
      ret = part_balance.ls_desc_array_.push_back(ls_desc);
      ASSERT_EQ(ret, OB_SUCCESS);
      ret = part_balance.ls_desc_map_.set_refactored(ObLSID(ls_id), ls_desc);
      ASSERT_EQ(ret, OB_SUCCESS);
      ls_id++;
    }
  }
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1001), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1002), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1003), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1004), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1005), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1006), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1007), 1003);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1008), 1003);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1009), 1003);
  ASSERT_EQ(ret, OB_SUCCESS);
  // ls 1001: 500       ls 1001:
  // ls 1002: 500   =>  ls 1002:
  // ls 1003: 500       ls 1003: 500

  // ls 1004:           ls 1004: 500
  // ls 1005:       =>  ls 1005:
  // ls 1006:           ls 1006:

  // ls 1007:           ls 1007: 500
  // ls 1008:       =>  ls 1008:
  // ls 1009:           ls 1009:
  ObBalanceGroup bg;
  ObSimpleTableSchemaV2 table_schema;
  bg.id_ = ObBalanceGroupID(1, 0);
  bg.name_ = "bg";
  table_schema.tenant_id_ = 1001;
  table_schema.table_id_ = 500001;
  table_schema.table_name_ = "part_table";
  table_schema.database_id_ = 0;
  ret = part_balance.on_new_partition(bg, table_schema, 500002, ObLSID(1001), ObLSID(1001), 500 * GB, true, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500003, ObLSID(1002), ObLSID(1002), 500 * GB, true, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500004, ObLSID(1003), ObLSID(1003), 500 * GB, true, 2);
  ASSERT_EQ(ret, OB_SUCCESS);

  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(part_balance.ls_desc_array_.at(i)->data_size_, 500 * GB);
    ASSERT_EQ(part_balance.ls_desc_array_.at(i)->partgroup_cnt_, 1);
  }
  for (int i = 3; i < 9; i++) {
    ASSERT_EQ(part_balance.ls_desc_array_.at(i)->data_size_, 0);
    ASSERT_EQ(part_balance.ls_desc_array_.at(i)->partgroup_cnt_, 0);
  }
  ObArray<ObUnitGroupLSDesc *> unit_group_ls_desc_arr;
  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 3);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 1500 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(1)->get_data_size(), 0);
  ASSERT_EQ(unit_group_ls_desc_arr.at(2)->get_data_size(), 0);

  // inter unit group disk balance
  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_TRUE(part_balance.job_generator_.need_gen_job());

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 0);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 0);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(4)->partgroup_cnt_, 0);
  ASSERT_EQ(part_balance.ls_desc_array_.at(5)->partgroup_cnt_, 0);
  ASSERT_EQ(part_balance.ls_desc_array_.at(6)->partgroup_cnt_, 1);
  ASSERT_EQ(part_balance.ls_desc_array_.at(7)->partgroup_cnt_, 0);
  ASSERT_EQ(part_balance.ls_desc_array_.at(8)->partgroup_cnt_, 0);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 0 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 0 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 500 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->data_size_, 500 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(4)->data_size_, 0 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(5)->data_size_, 0 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(6)->data_size_, 500 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(7)->data_size_, 0 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(8)->data_size_, 0 * GB);

  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 3);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 500 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(1)->get_data_size(), 500 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(2)->get_data_size(), 500 * GB);


  part_balance.job_generator_.normal_to_normal_part_map_.clear();
  ASSERT_FALSE(part_balance.job_generator_.need_gen_job());

  // intra unit group disk balance
  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(part_balance.job_generator_.need_gen_job());
}

TEST_F(TestPartitionBalance, disk_balance_intra1)
{
  int ret = OB_SUCCESS;
  ObPartitionBalance part_balance;
  uint64_t tenant_id;
  ret = part_balance.init(OB_SYS_TENANT_ID, &multi_schema_service_, &sql_proxy_, 3, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  part_balance.tenant_id_ = 1002;
  for (int64_t ls_id = 1001; ls_id <= 1003; ls_id++) {
    ObLSDesc *ls_desc = nullptr;
    ls_desc = reinterpret_cast<ObLSDesc*>(part_balance.allocator_.alloc(sizeof(ObLSDesc)));
    ASSERT_NE(ls_desc, nullptr);
    new(ls_desc) ObLSDesc(ObLSID(ls_id), 1001, 1001);
    ret = part_balance.ls_desc_array_.push_back(ls_desc);
    ASSERT_EQ(ret, OB_SUCCESS);
    ret = part_balance.ls_desc_map_.set_refactored(ObLSID(ls_id), ls_desc);
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1001), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1002), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1003), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  // ls 1001 190: [bg1: 60 60] [bg2: 50]     [bg3: 10 10]
  // ls 1002 230: [bg1: 60 60] [bg2: 50 50]  [bg3: 10]
  // ls 1003 130: [bg1: 60]    [bg2: 50]     [bg3: 10 10]
  // =>
  // ls 1001 190: [bg1: 60 60] [bg2: 50]     [bg3: 10 10]
  // ls 1002 180: [bg1: 60 60] [bg2: 50]     [bg3: 10]
  // ls 1003 180: [bg1: 60]    [bg2: 50 50]  [bg3: 10 10]
  ObBalanceGroup bg;
  ObSimpleTableSchemaV2 table_schema;
  bg.id_ = ObBalanceGroupID(1, 0);
  bg.name_ = "bg1";
  table_schema.tenant_id_ = 1001;
  table_schema.table_id_ = 500001;
  table_schema.table_name_ = "part_table";
  table_schema.database_id_ = 0;
  ret = part_balance.on_new_partition(bg, table_schema, 500002, ObLSID(1001), ObLSID(1001), 60 * GB, true, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500003, ObLSID(1001), ObLSID(1001), 60 * GB, true, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500004, ObLSID(1002), ObLSID(1002), 60 * GB, true, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500005, ObLSID(1002), ObLSID(1002), 60 * GB, true, 3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500006, ObLSID(1003), ObLSID(1003), 60 * GB, true, 4);
  ASSERT_EQ(ret, OB_SUCCESS);

  bg.id_ = ObBalanceGroupID(2, 0);
  bg.name_ = "bg2";
  table_schema.table_id_ = 500007;
  ret = part_balance.on_new_partition(bg, table_schema, 500008, ObLSID(1001), ObLSID(1001), 50 * GB, true, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500009, ObLSID(1002), ObLSID(1002), 50 * GB, true, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500010, ObLSID(1002), ObLSID(1002), 50 * GB, true, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500011, ObLSID(1003), ObLSID(1003), 50 * GB, true, 3);
  ASSERT_EQ(ret, OB_SUCCESS);

  bg.id_ = ObBalanceGroupID(3, 0);
  bg.name_ = "bg3";
  table_schema.table_id_ = 500012;
  ret = part_balance.on_new_partition(bg, table_schema, 500013, ObLSID(1001), ObLSID(1001), 10 * GB, true, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500014, ObLSID(1001), ObLSID(1001), 10 * GB, true, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500015, ObLSID(1002), ObLSID(1002), 10 * GB, true, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500016, ObLSID(1003), ObLSID(1003), 10 * GB, true, 3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500017, ObLSID(1003), ObLSID(1003), 10 * GB, true, 4);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 5);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 5);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 4);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 190 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 230 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 130 * GB);

  ObArray<ObUnitGroupLSDesc *> unit_group_ls_desc_arr;
  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 1);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 550 * GB);

  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_TRUE(part_balance.job_generator_.need_gen_job());

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 5);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 4);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 5);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 190 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 180 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 180 * GB);
}

TEST_F(TestPartitionBalance, disk_balance_inter_intra)
{
  int ret = OB_SUCCESS;
  ObPartitionBalance part_balance;
  uint64_t tenant_id;
  ret = part_balance.init(OB_SYS_TENANT_ID, &multi_schema_service_, &sql_proxy_, 2, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  part_balance.tenant_id_ = 1002;
  int64_t ls_id = 1001;
  uint64_t ls_group_ids[2] = {1001, 1002};
  for (int64_t i = 0; i < 2; i++) {
    for (int64_t j = 0; j < 2; j++) {
      ObLSDesc *ls_desc = nullptr;
      ls_desc = reinterpret_cast<ObLSDesc*>(
          part_balance.allocator_.alloc(sizeof(ObLSDesc)));
      ASSERT_NE(ls_desc, nullptr);
      new(ls_desc) ObLSDesc(ObLSID(ls_id), ls_group_ids[i], ls_group_ids[i]);
      ret = part_balance.ls_desc_array_.push_back(ls_desc);
      ASSERT_EQ(ret, OB_SUCCESS);
      ret = part_balance.ls_desc_map_.set_refactored(ObLSID(ls_id), ls_desc);
      ASSERT_EQ(ret, OB_SUCCESS);
      ls_id++;
    }
  }
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1001), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1002), 1001);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1003), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.job_generator_.ls_group_id_map_.set_refactored(ObLSID(1004), 1002);
  ASSERT_EQ(ret, OB_SUCCESS);
  // ls 1001: 120 80     ls 1001: 20 80
  // ls 1002: 120 80     ls 1002: 120 80

  // ls 1003: 80 20   => ls 1003: 80 120
  // ls 1004: 80 20      ls 1004: 80 20
  ObBalanceGroup bg;
  ObSimpleTableSchemaV2 table_schema;
  bg.id_ = ObBalanceGroupID(1, 0);
  table_schema.tenant_id_ = 1001;
  table_schema.table_id_ = 500001;
  table_schema.table_name_ = "part_table";
  table_schema.database_id_ = 0;
  bg.name_ = "bg";
  ret = part_balance.on_new_partition(bg, table_schema, 500002, ObLSID(1001), ObLSID(1001), 120 * GB, true, 0);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500003, ObLSID(1001), ObLSID(1001), 80 * GB, true, 1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500004, ObLSID(1002), ObLSID(1002), 120 * GB, true, 2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500005, ObLSID(1002), ObLSID(1002), 80 * GB, true, 3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500006, ObLSID(1003), ObLSID(1003), 80 * GB, true, 4);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500007, ObLSID(1003), ObLSID(1003), 20 * GB, true, 5);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500008, ObLSID(1004), ObLSID(1004), 80 * GB, true, 6);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = part_balance.on_new_partition(bg, table_schema, 500009, ObLSID(1004), ObLSID(1004), 20 * GB, true, 7);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->partgroup_cnt_, 2);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 200 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 200 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 100 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->data_size_, 100 * GB);

  ObArray<ObUnitGroupLSDesc *> unit_group_ls_desc_arr;
  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 2);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 400 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(1)->get_data_size(), 200 * GB);

  // inter unit group disk balance
  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_TRUE(part_balance.job_generator_.need_gen_job());

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->partgroup_cnt_, 2);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->partgroup_cnt_, 2);

  ASSERT_EQ(part_balance.ls_desc_array_.at(0)->data_size_, 100 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(1)->data_size_, 200 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(2)->data_size_, 200 * GB);
  ASSERT_EQ(part_balance.ls_desc_array_.at(3)->data_size_, 100 * GB);

  part_balance.build_unit_group_ls_desc_(unit_group_ls_desc_arr);
  ASSERT_EQ(unit_group_ls_desc_arr.count(), 2);
  ASSERT_EQ(unit_group_ls_desc_arr.at(0)->get_data_size(), 300 * GB);
  ASSERT_EQ(unit_group_ls_desc_arr.at(1)->get_data_size(), 300 * GB);


  part_balance.job_generator_.normal_to_normal_part_map_.clear();
  ASSERT_FALSE(part_balance.job_generator_.need_gen_job());

  // intra unit group disk balance
  ret = part_balance.process_balance_partition_disk_();
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_FALSE(part_balance.job_generator_.need_gen_job());
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#undef private
#undef protected
