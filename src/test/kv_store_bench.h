/*
 * KvStoreBench.h
 *
 *  Created on: Aug 23, 2012
 *      Author: eleanor
 */

#ifndef KVSTOREBENCH_H_
#define KVSTOREBENCH_H_

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

/**
 * stores pairings from op type to time taken for that op (for latency), and to
 * time that op completed to the nearest second (for throughput).
 */
struct kv_bench_data {
  JSONFormatter throughput_jf;

  JSONFormatter latency_jf;
};

class KvStoreBench;

/**
 * keeps track of the number of milliseconds between two events - used to
 * measure latency
 */
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

/**
 * arguments passed to the callback method when the op is being timed
 */
struct timed_args {
  StopWatch sw;
  //kv_bench_data data;
  KvStoreBench * kvsb;
  bufferlist val;
  int err;
  char op;

  timed_args ()
  : kvsb(NULL),
    err(0),
    op(' ')
  {};

  timed_args (KvStoreBench * k)
  : kvsb(k),
    err(0),
    op(' ')
  {}
};

typedef pair<string, bufferlist> (KvStoreBench::*next_gen_t)(bool new_elem);

class KvStoreBench {

protected:
  int k;

  int entries;
  int ops;
  int clients;
  int cache_size;
  double cache_refresh;
  int increment;
  int key_size;
  int val_size;
  map<int, char> probs;
  set<string> key_map;
  kv_bench_data data;
  KeyValueStructure * kvs;
  string client_name;
  Cond op_avail;
  Mutex ops_in_flight_lock;
  Mutex data_lock;
  int ops_in_flight;
  int max_ops_in_flight;

  /*
   * rados_id	op_char	latency
   */
  void print_time_datum(ostream * s, pair<char, double> d);

  /**
   * prints the latency statistics generated by test_stress_random_set_rms
   */
  void print_time_data();

public:

  KvStoreBench();

  ~KvStoreBench();

  int setup(int argc, const char** argv);

  string random_string(int len);

  /**
   * Tests entries insertions of random keys and values
   */
  int test_random_insertions();

  int test_teuthology_aio(next_gen_t distr, const map<int, char> &probs);

  int test_teuthology_sync(next_gen_t distr, const map<int, char> &probs);

  pair<string, bufferlist> rand_distr(bool new_elem);

  static void aio_set_callback_not_timed(int * err, void *arg);

  static void aio_callback_timed(int * err, void *arg);

  int teuthology_tests();

};

#endif /* KVSTOREBENCH_H_ */
