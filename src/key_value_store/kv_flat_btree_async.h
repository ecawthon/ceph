/*
 * KvFlatBtreeAsync.h
 *
 *  Created on: Jun 18, 2012
 *      Author: eleanor
 */

#ifndef KVFLATBTREEASYNC_H_
#define KVFLATBTREEASYNC_H_

#include "key_value_store/key_value_structure.h"
#include <sstream>
#include <stdarg.h>

using namespace std;
using ceph::bufferlist;

class KvFlatBtreeAsync : public KvFlatBtree {
protected:
  int k;
  string index_name;
  string rados_id;
  string prefix;
  int prefix_num;
  librados::Rados rados;
  string pool_name;

  string to_string(string s, int i);
  bufferlist to_bl(string s);
  bufferlist to_bl(string s, int i);
  int bl_to_int(bufferlist *bl);

  //Things that NEVER modify objects
  int next(const string &key, string *ret);
  int prev(const string &oid, string *ret);
  int oid(const string &key, bufferlist * raw_val);
  int oid(const string &key, bufferlist * raw_val, string * max_key);

  //Things that modify objects
  int split(const string &obj);
  int rebalance(const string &oid);
public:
  KvFlatBtreeAsync(int k_val, string rid)
  : k(k_val),
    index_name("index_object"),
    rados_id(rid),
    prefix(string(rados_id).append(".")),
    prefix_num(0),
    pool_name("data")
  {}

//  ~KvFlatBtreeAsync();
  int setup(int argc, const char** argv);

  int set(const string &key, const bufferlist &val,
        bool update_on_existing);

  int remove(const string &key);

  int remove_all();

  //readers
  int get(const string &key, bufferlist *val);

  int get_all_keys(std::set<string> *keys);

  int get_all_keys_and_values(map<string,bufferlist> *kv_map);

  int get_keys_in_range(const string &min_key, const string &max_key,
        std::set<string> *key_set, int max_keys);

  int get_key_vals_in_range(string min_key,
        string max_key, map<string,bufferlist> *kv_map, int max_keys);

  bool is_consistent();

  string str();
};

#endif /* KVFLATBTREEASYNC_H_ */
