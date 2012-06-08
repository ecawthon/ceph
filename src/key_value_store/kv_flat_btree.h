/*
 * kv_flat_btree.hpp
 *
 *  Created on: Jun 8, 2012
 *      Author: eleanor
 */

#ifndef KV_FLAT_BTREE_HPP_
#define KV_FLAT_BTREE_HPP_

#include "key_value_store/key_value_structure.h"

using namespace std;
using ceph::bufferlist;

class KvFlatBtree : KeyValueStructure{
protected:
  int k;
  static int increment;
  librados::IoCtx io_ctx;
  string map_obj_name;

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
  : k(1)
  {}

  KvFlatBtree(int k_val, const librados::IoCtx &ioctx);

  int set(const string &key, const bufferlist &val,
        bool update_on_existing);

  int set(const map<string,bufferlist> &kv_map,
        bool update_on_existing);

  int remove(const string &key);

  int remove(const std::set<string> &keys);

  int remove_all();

  int get(const string &key, bufferlist *val);

  int get_all_keys(std::set<string> *keys);

  int get_all_keys_and_values(map<string,bufferlist> *kv_map);

  int get_keys_in_range(const string &min_key, const string &max_key,
        std::set<string> *key_set, int max_keys);

  int get_key_vals_in_range(string min_key,
        string max_key, map<string,bufferlist> *kv_map, int max_keys);

  string str();

};


#endif /* KV_FLAT_BTREE_HPP_ */
