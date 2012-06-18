/*
 * kv_flat_btree.hpp
 *
 *  Created on: Jun 8, 2012
 *      Author: eleanor
 */

#ifndef KV_FLAT_BTREE_HPP_
#define KV_FLAT_BTREE_HPP_

#include "key_value_store/key_value_structure.h"
#include <sstream>

using namespace std;
using ceph::bufferlist;

class KvFlatBtree : KeyValueStructure{
protected:
  int k;
  static int increment;
  librados::IoCtx io_ctx;
  string map_obj_name;
  stringstream current_op;

  /**
   * returns the name of the next bucket that contains key
   */
  int next(const string &key, string *ret);
  int prev(const string &key, string *ret);

  //things that need to be atomic
  int oid(const string &key, string * oid);
  bool is_full(const string &oid);
  bool is_half_empty(const string &oid);

  //things that modify buckets
  int split(const string &obj);
  int rebalance(const string &oid);

public:
  KvFlatBtree()
  : k(5)
  {}

  KvFlatBtree(int k_val, const librados::IoCtx &ioctx)
  : k(k_val),
    io_ctx(ioctx),
    map_obj_name("index_object"),
    current_op("constructor")
  {}

  KvFlatBtree& operator=(const KvFlatBtree &kvb);

  virtual int set(const string &key, const bufferlist &val,
        bool update_on_existing);

  virtual int remove(const string &key);

  virtual int remove_all();

  virtual int get(const string &key, bufferlist *val);

  virtual int get_all_keys(std::set<string> *keys);

  virtual int get_all_keys_and_values(map<string,bufferlist> *kv_map);

  virtual int get_keys_in_range(const string &min_key, const string &max_key,
        std::set<string> *key_set, int max_keys);

  virtual int get_key_vals_in_range(string min_key,
        string max_key, map<string,bufferlist> *kv_map, int max_keys);

  virtual bool is_consistent();

  virtual string str();

};


#endif /* KV_FLAT_BTREE_HPP_ */
