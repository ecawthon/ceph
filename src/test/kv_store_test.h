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

class KvStoreTest {
protected:
  librados::IoCtx io_ctx;
  librados::Rados rados;
  int k;
  string pool_name;
  string rados_id;
  KvFlatBtree kvs;

  int entries;
  int ops;

public:
  KvStoreTest()
  : k(5),
    pool_name("data"),
    rados_id("admin"),
    entries(100),
    ops(100)
  {}

  int setup(int argc, const char** argv);

  int generate_small_non_random_omap(
      std::map<std::string,bufferlist> * out_omap);

  string random_string(int len);

  int generate_random_vector(vector<pair<string, bufferlist> > *ret);
  /**
   * Test of correctness for the set and get methods that take a single
   * key/value pair as arguments in KeyValueStructure. Stores two key/values,
   * displays them, and then overwrites one of them, displaying the result.
   *
   * @return error code.
   */
  int test_set_get_rm_one_kv();

  int test_set_get_kv_map();

  int test_many_single_ops();

  int test_random_insertions();

  int test_random_ops();

  /**
   * Test correctness of all methods in KeyValueStructure
   */
  int test_functionality();
};
