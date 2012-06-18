/*
 * kv_store_test.h
 *
 *  Created on: Jun 11, 2012
 *      Author: eleanor
 */

#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree.h"
#include "include/rados/librados.hpp"

#include <string>
#include <climits>
#include <iostream>

using namespace std;
using ceph::bufferlist;

//typedef int (KvStoreTest::*kvs_test_t)();


class KvStoreTest {
protected:
  librados::IoCtx io_ctx;
  librados::Rados rados;
  int k;
  string pool_name;
  string rados_id;
  KvFlatBtree kvs;
  //kvs_test_t test;

  int entries;
  int ops;

public:
  KvStoreTest()
  : k(10),
    pool_name("data"),
    rados_id("admin"),
    entries(100),
    ops(100)//,
 //   test(stress_tests)
  {}

  int setup(int argc, const char** argv);

  string random_string(int len);

  /**
   * Test of correctness for the set, get, and remove methods. Stores two
   * key/values, displays them, and then overwrites one of them,
   * displaying the result.
   *
   * @return error code.
   */
  int test_set_get_rm_one_kv();

  int test_split_merge();

  int test_non_random_insert_gets();

  int test_random_insertions();

  int test_random_ops();

  /**
   * Test correctness of all methods in KeyValueStructure
   */
  int functionality_tests();

  int stress_tests();
};
