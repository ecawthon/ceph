/*
 * KvFlatBtreeAsync.h
 *
 *  Created on: Jun 18, 2012
 *      Author: eleanor
 */

#ifndef KVFLATBTREEASYNC_H_
#define KVFLATBTREEASYNC_H_

#include "key_value_store/key_value_structure.h"
#include "include/utime.h"
#include "include/rados.h"
#include <sstream>
#include <stdarg.h>

using namespace std;
using ceph::bufferlist;

struct prefix_data {
  utime_t ts;
  string prefix;
  vector<pair<string, string> > to_create;
  vector<pair<string, string> > to_delete;
  bufferlist val;
};

class KvFlatBtreeAsync : public KvFlatBtree {
protected:
  int k;
  string index_name;
  string rados_id;
  int client_index;
  char pair_init;
  char sub_separator;
  char pair_end;
//  char separator;
  char sub_terminator;
  char terminator;
  librados::Rados rados;
  string pool_name;
  vector<__useconds_t> waits;
  int wait_index;
  utime_t TIMEOUT;

  int parse_prefix(bufferlist * bl, prefix_data * ret);
  int cleanup(const prefix_data &p, const int &errno);

  int remove_obj(string obj);
  int aio_remove_obj(string obj, librados::AioCompletion * a);


  //Things that NEVER modify objects
  int next(const string &obj_high_key, const string &obj,
      string * ret_high_key, string *ret);
  int prev(const string &obj_high_key, const string &obj,
      string * ret_high_key, string *ret);
  int oid(const string &key, bufferlist * raw_val);
  int oid(const string &key, bufferlist * raw_val, string * max_key);

  //Things that modify objects
  int split(const string &obj, const string &high_key, int * ver,
      map<string,bufferlist> * omap);
  int rebalance(const string &o1, const string &hk1, int *ver, bool reverse);
public:
  string client_name;


  KvFlatBtreeAsync(int k_val, string name)
  : k(k_val),
    index_name("index_object"),
    rados_id("admin"),
    client_name(string(name).append(".")),
    client_index(0),
    pair_init('('),
    sub_separator('|'),
    pair_end(')'),
//    separator(','),
    sub_terminator(';'),
    terminator(':'),
    pool_name("data"),
    TIMEOUT(2000000,0)
  {}

  KvFlatBtreeAsync(int k_val, string name, vector<__useconds_t> wait)
  : k(k_val),
    index_name("index_object"),
    rados_id("admin"),
    client_name(string(name).append("dot")),
    client_index(0),
    pair_init('('),
    sub_separator('|'),
    pair_end(')'),
//    separator(','),
    sub_terminator(';'),
    terminator(':'),
    pool_name("data"),
    waits(wait),
    wait_index(0),
    TIMEOUT(2000000,0)
  {}

  static string to_string(string s, int i);
  static bufferlist to_bl(string s);
  static bufferlist to_bl(string s, int i);

  //~KvFlatBtreeAsync();

  /*
   * should be in the format:
   * timestamp
   * sub_terminator
   * objects to be created, organized into pair_init high key sub_separator
   * obj name pair_end, separated by separators
   * sub_terminator
   * objects to be removed, organized into pair_init high key sub_separator
   * obj name pair_end, separated by separators
   * terminator
   * value
   */
  string to_string_f(string s);
  static int bl_to_int(bufferlist *bl);

//  ~KvFlatBtreeAsync();
  int setup(int argc, const char** argv);

  void set_waits(const vector<__useconds_t> &wait);

  int set(const string &key, const bufferlist &val,
        bool update_on_existing);

  int remove(const string &key);

  int remove_all();

  //readers
  int get(const string &key, bufferlist *val);

  int get_all_keys(std::set<string> *keys);

  int get_all_keys_and_values(map<string,bufferlist> *kv_map);

  bool is_consistent();

  string str();
};

#endif /* KVFLATBTREEASYNC_H_ */
