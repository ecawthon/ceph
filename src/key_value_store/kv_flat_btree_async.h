/*
 * KvFlatBtreeAsync.h
 *
 *  Created on: Jun 18, 2012
 *      Author: eleanor
 */

#ifndef KVFLATBTREEASYNC_H_
#define KVFLATBTREEASYNC_H_

#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree.h"
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
  librados::IoCtx io_ctx;
  string rados_id;
  string client_name;
  int client_index;
  char pair_init;
  char sub_separator;
  char pair_end;
  char sub_terminator;
  char terminator;
  librados::Rados rados;
  string pool_name;
  vector<__useconds_t> waits;
  int wait_index;
  utime_t TIMEOUT;


  /*
   * creates a prefix_data with the information contained in the prefix of
   * the bufferlist.
   *
   * prefixes should be in the format:
   * 1 (to indicate that there is a prefix)
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
  int parse_prefix(bufferlist * bl, prefix_data * ret);

  //These read, but do not write, librados objects

  /**
   * finds the object in the index with the lowest key value that is greater
   * than obj_high_key. If obj_high_key is the max key, ret_high_key and ret
   * are set to obj_high_key and obj, respectively.
   *
   * @param obj_high_key: the key stored in the index for obj
   * @param obj: the name of the object to search for
   * @param ret_high_key: string to store the key of the next high object
   * @param ret: string to store the name of the next high object
   */
  int next(const string &obj_high_key, const string &obj,
      string * ret_high_key, string *ret);

  /**
   * finds the object in the index with the lowest key value that is greater
   * than obj_high_key. If obj_high_key is the max key, ret_high_key and ret
   * are set to obj_high_key and obj, respectively.
   *
   * @param obj_high_key: the key stored in the index for obj
   * @param obj: the name of the object to search for
   * @param ret_high_key: string to store the key of the next high object
   * @param ret: string to store the name of the next high object
   */
  int prev(const string &obj_high_key, const string &obj,
      string * ret_high_key, string *ret);

  /**
   * finds the object where a key belongs.
   *
   * @param key: the key to search for - should either have a 0 befor it or be
   * 1 (the max key)
   * @param raw_val: the bufferlist in the first index value such that its key
   * is greater than key. This may include a prefix.
   * @param max_key: string where the key of the object should be stored
   */
  int oid(const string &key, bufferlist * raw_val, string * max_key);

  //These sometimes modify objects and the index

  /**
   * Reads obj and generates information about it. If necessary, splits it.
   *
   * @param obj: the object to read/split
   * @param high_key: the key of obj in the index
   * @param ver: int to store the version number of the read
   * @param omap: map where the omap of the obj will be stored
   * @return -1 if obj does not need to be split,
   */
  int split(const string &obj, const string &high_key, int * ver,
      map<string,bufferlist> * omap);

  /**
   * reads o1 and the next object after o1 and, if necessary, rebalances them.
   * if hk1 is the highest key in the index, calls rebalance on the next highest
   * key.
   *
   * @param o1: the name of the object to check
   * @param hk1: the key corresponding to o1
   * @param ver: the version number of o1 (or, if reverse, of the next object)
   * @param reverse: if true, ver will be the version of the next object after
   * o1. This is used when rebalance calls itself on the previous object.
   * @return -1 if no change needed, -ENOENT if o1 does not exist,
   * -ECANCELED if the rebalance fails due to another thread (meaning rebalance
   * should be repeated)
   */
  int rebalance(const string &o1, const string &hk1, int *ver, bool reverse);

  /**
   * Called when a client discovers that another client has died during  a
   * split or a merge. cleans up after that client.
   *
   * @param p: the prefix data parsed from the index entry left by the dead
   * client.
   * @param errno: the error that caused the client to realize the other client
   * died (should be -ENOENT or -ETIMEDOUT)
   */
  int cleanup(const prefix_data &p, const int &errno);

public:
  KvFlatBtreeAsync(int k_val, string name)
  : k(k_val),
    index_name("index_object"),
    rados_id("admin"),
    client_name(string(name).append(".")),
    client_index(0),
    pair_init('('),
    sub_separator('|'),
    pair_end(')'),
    sub_terminator(';'),
    terminator(':'),
    pool_name("data"),
    waits(),
    wait_index(1),
    TIMEOUT(50000000000000,0)
  {}

  KvFlatBtreeAsync(int k_val, string name, vector<__useconds_t> wait)
  : k(k_val),
    index_name("index_object"),
    rados_id("admin"),
    client_name(string(name).append(".")),
    client_index(0),
    pair_init('('),
    sub_separator('|'),
    pair_end(')'),
    sub_terminator(';'),
    terminator(':'),
    pool_name("data"),
    waits(wait),
    wait_index(0),
    TIMEOUT(50000000000000,0)
  {}

  /**
   * creates a string with an int at the end.
   *
   * @param s: the string on the left
   * @param i: the int to be appended to the string
   * @return the string
   */
  static string to_string(string s, int i);

  /**
   * returns a bufferlist containing s
   */
  static bufferlist to_bl(string s);

  /**
   * puts escape characters before any special characters in a string that goes
   * in a prefix
   */
  string to_string_f(string s);

  /**
   * returns the rados_id of this KvFlatBtreeAsync
   */
  string get_name();

  /**
   * sets up the rados and io_ctx of this KvFlatBtreeAsync. If the don't already
   * exist, creates the index and max object.
   */
  int setup(int argc, const char** argv);

  /**
   * sets up the waitpoints according to wait and resets wait_index
   *
   * @param wait: the array of wait times to use
   */
  void set_waits(const vector<__useconds_t> &wait);

  //writers inherited from
  int set(const string &key, const bufferlist &val,
        bool update_on_existing);

  int remove(const string &key);

  int remove_all();

  //readers
  /**
   * returns true if all of the following are true:
   *
   * all objects are accounted for in the index (i.e., no floating objects)
   * no index entries have prefixes
   * all objects have k <= size <= 2k
   * all keys in an object are within the specified predicted by the index
   *
   * if any of those fails, states that the problem(s) are, and prints str().
   *
   * @pre: no operations are in progress
   */
  bool is_consistent();

  /**
   * returns an ASCII representation of the index and sub objects, showing
   * stats about each object and all omaps.
   */
  string str();

  int get(const string &key, bufferlist *val);

  //none of these have been updated or tested with multiple clients
  int get_all_keys(std::set<string> *keys);

  int get_all_keys_and_values(map<string,bufferlist> *kv_map);

};

#endif /* KVFLATBTREEASYNC_H_ */
