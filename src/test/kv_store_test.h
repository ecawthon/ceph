/*
 * kv_store_test.h
 *
 *  Created on: Jun 11, 2012
 *      Author: eleanor
 */

#define EINCONSIST 135

#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree_async.h"
#include "common/Clock.h"
#include "global/global_context.h"
#include "common/Mutex.h"
#include "common/Cond.h"

#include <string>
#include <climits>
#include <cfloat>
#include <iostream>

using namespace std;
using ceph::bufferlist;

struct StopWatch {
  utime_t begin_time;
  utime_t end_time;

  void start_time() {
    begin_time = ceph_clock_now(g_ceph_context);
  }
  void stop_time() {
    end_time = ceph_clock_now(g_ceph_context);
  }
  double get_time() {
    return (end_time - begin_time) * 1000;
  }
  void clear() {
    begin_time = end_time = utime_t();
  }
};

struct set_args {
  KeyValueStructure * kvs;
  string key;
  bufferlist val;
  StopWatch sw;
};

struct rm_args {
  KeyValueStructure * kvs;
  string key;
  StopWatch sw;
};

struct kv_bench_data {
  //latency
  double avg_latency;
  double min_latency;
  double max_latency;
  double total_latency;
  int started_ops;
  int completed_ops;
  std::map<uint64_t,uint64_t> freq_map;
  pair<uint64_t,uint64_t> mode_latency;
  vector<pair<char, double> > latency_datums;

  //throughput
  double avg_throughput;
  double min_throughput;
  double max_throughput;
  std::map<uint64_t, uint64_t> interval_to_ops_map;
  std::map<uint64_t, uint64_t>::iterator it;
  pair<uint64_t,uint64_t> mode_throughput;
  kv_bench_data()
  : avg_latency(0.0), min_latency(DBL_MAX), max_latency(0.0),
    total_latency(0.0),
    started_ops(0), completed_ops(0),
    avg_throughput(0.0), min_throughput(DBL_MAX), max_throughput(0.0),
    it(interval_to_ops_map.begin())
  {}
};

class KvStoreTest;

struct timed_args {
  StopWatch sw;
  KvStoreTest * kvst;
bufferlist val;
  int err;
  char op;

  timed_args ()
  : kvst(NULL),
    err(0),
    op(' ')
  {};

  timed_args (KvStoreTest * k)
  : kvst(k),
    err(0),
    op(' ')
  {}
};

typedef pair<string, bufferlist> (KvStoreTest::*next_gen_t)(bool new_elem);

class KvStoreTest {
protected:
  int k;

  int wait_time;
  int entries;
  int ops;
  int clients;
  double uincrement;
  int increment;
  int key_size;
  int val_size;
  map<int, char> probs;
  set<string> key_map;
  kv_bench_data data;
  char inject;
  KeyValueStructure * kvs;
  string client_name;
  Cond op_avail;
  Mutex ops_in_flight_lock;
  Mutex data_lock;
  int ops_in_flight;
  int max_ops_in_flight;
  Mutex ops_count_lock;
  int ops_count;

  /*
   * rados_id	op_char	latency
   */
  void print_time_datum(ostream * s, pair<char, double> d);

  /**
   * prints the latency statistics generated by test_stress_random_set_rms
   */
  void print_time_data();

  /**
   * Called by concurrent tests to run and time a set operation
   */
  static void *pset(void *ptr);

  /**
   * called by concurrent tests to run and time a remove operation
   */
  static void *prm(void *ptr);

  static void *throughput_counter(void * arg);


public:
  KvStoreTest();

  ~KvStoreTest();

  int setup(int argc, const char** argv);

  string random_string(int len);

  pair<string, bufferlist> rand_distr(bool new_elem);

  static void aio_set_callback_not_timed(int * err, void *arg);

  static void aio_callback_timed(int * err, void *arg);

  /**
   * Test of correctness for the set, get, and remove methods. Stores two
   * key/values, displays them, and then overwrites one of them,
   * displaying the result.
   *
   * @return error code.
   */
  int test_set_get_rm_one_kv();

  /**
   * Test of correctness for split and merge, including both rebalance
   * and merge.
   */
  int test_split_merge();

  /**
   * Tests entries sets of sequentially increasing keys, in forward and
   * reverse order
   */
  int test_non_random_insert_gets();

  /**
   * Tests entries insertions of random keys and values
   */
  int test_random_insertions();

  /**
   * Tests ops insertions and removals of random keys and values.
   * Sets more often than it removes.
   */
  int test_random_ops();

  /**
   * uses varying waitpoints to test two concurrent sets of non-random keys.
   * The number of loops is the number necessary to test all combinations
   * of waitpoints.
   */
  int test_verify_concurrent_sets(int argc, const char** argv);

  /**
   * uses varying waitpoints to test a set and a remove of non-random keys.
   * The number of loops is the number necessary to test all combinations
   * of waitpoints.
   */
  int test_verify_concurrent_set_rms(int argc, const char** argv);

  /**
   * uses varying waitpoints to test concurrent sets and removes of random keys.
   * The number of loops is the number necessary to test all combinations
   * of waitpoints.
   */
  int test_verify_random_set_rms(int argc, const char** argv);

  /**
   * uses varying waitpoints to test concurrent sets and removes of random keys.
   * uses clients threads at once and repeats for ops loops. starts with entries
   * key/value pairs before timing. Prints latency statistics.
   */
  int test_stress_random_set_rms(int argc, const char** argv);

  int test_teuthology_aio(next_gen_t distr, const map<int, char> &probs);

  int test_teuthology_sync(next_gen_t distr, const map<int, char> &probs);

  /**
   * Test correctness of all methods in KeyValueStructure
   */
  int functionality_tests();

  /**
   * Run all sequential stress tests
   * (currently excludes test_stres_random_set_rms)
   */
  int stress_tests();

  /**
   * Run all teuthology tests
   */
  int teuthology_tests();

  /**
   * Run all concurrency tests
   */
  int verification_tests(int argc, const char** argv);
};
