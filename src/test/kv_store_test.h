/*
 * kv_store_test.h
 *
 *  Created on: Jun 11, 2012
 *      Author: eleanor
 */

#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree.h"
#include "key_value_store/kv_flat_btree_async.h"
#include "include/rados/librados.hpp"

#include <string>
#include <climits>
#include <cfloat>
#include <iostream>

using namespace std;
using ceph::bufferlist;

//typedef int (KvStoreTest::*kvs_test_t)();

struct set_args {
  KvFlatBtreeAsync * kvba;
  string key;
  bufferlist val;
};

struct rm_args {
  KvFlatBtreeAsync * kvba;
  string key;
};

struct kv_bench_data {
  double avg_latency;
  double min_latency;
  double max_latency;
  double total_latency;
  int started_ops;
  int completed_ops;
  std::map<int,int> freq_map;
  pair<int,int> mode;
  kv_bench_data()
  : avg_latency(0.0), min_latency(DBL_MAX), max_latency(0.0),
    total_latency(0.0),
    started_ops(0), completed_ops(0)
  {}
};

class StopWatch {
protected:
  utime_t begin_time;
  utime_t end_time;
public:
  void start_time();
  void stop_time();
  double get_time();
  void clear();
};

class KvStoreTest {
protected:
  int k;
  string rados_id;
  KeyValueStructure * kvs;
  //kvs_test_t test;

  int entries;
  int ops;
  const static int clients = 2;
  double increment;
  kv_bench_data data;
  static StopWatch sw[clients];

public:
  KvStoreTest()
  : k(2),
    entries(30),
    ops(10),
    increment(0.01)
 //   test(stress_tests)
  {}

  //~KvStoreTest();

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

  static void *pset(void *ptr);

  static void *prm(void *ptr);

  int test_concurrent_sets(int argc, const char** argv);

  int test_concurrent_set_rms(int argc, const char** argv);

  int test_verify_random_set_rms(int argc, const char** argv);


  int test_stress_random_set_rms(int argc, const char** argv);

  /**
   * Test correctness of all methods in KeyValueStructure
   */
  int functionality_tests();

  int stress_tests();

  int verification_tests(int argc, const char** argv);

  void print_time_data();
};
