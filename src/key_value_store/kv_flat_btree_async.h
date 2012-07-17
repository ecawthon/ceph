/*
 * KvFlatBtreeAsync.h
 *
 *  Created on: Jun 18, 2012
 *      Author: eleanor
 */

#ifndef KVFLATBTREEASYNC_H_
#define KVFLATBTREEASYNC_H_

#define ESUICIDE 134
#define EPREFIX 136

#include "key_value_store/key_value_structure.h"
#include "include/utime.h"
#include "include/rados.h"
#include <sstream>
#include <stdarg.h>

using namespace std;
using ceph::bufferlist;

enum {
  ADD_PREFIX = 1,
  MAKE_OBJECT = 2,
  UNWRITE_OBJECT = 3,
  RESTORE_OBJECT = 4,
  REMOVE_OBJECT = 5,
  REMOVE_PREFIX = 6
};



struct prefix_data {
  utime_t ts;
  string prefix;
  vector<vector<string> > to_create;
  vector<vector<string> > to_delete;
  bufferlist val;
  void clear() {
    ts = utime_t();
    prefix = "";
    to_create.clear();
    to_delete.clear();
    val.clear();
  }
};

struct object_info {
  string key;
  string name;
  map<std::string, bufferlist> omap;
  bufferlist unwritable;
  int version;
  int size;
};

class KvFlatBtreeAsync : public KeyValueStructure {
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

  /**
   * Waits for a period determined by the waits vector (does not wait if waits
   * vector is empty). called before most ObjectOperations.
   */
  int interrupt();

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
   * obj name sub_separator version number pair_end, separated by separators
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
  int split(const string &obj, const string &high_key,
      object_info *info);

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
  int rebalance(const string &o1, const string &hk1, object_info * info1,
      bool reverse);

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

  int read_object(const string &obj, object_info * info);

  void set_up_prefix_index(
      const map<string, string> &to_create,
      const vector<object_info*> &to_delete,
      librados::ObjectWriteOperation * owo,
      prefix_data * p,
      int * err);

  void set_up_ops(
      const vector<map<std::string, bufferlist> > &create_maps,
      const vector<object_info*> &delete_infos,
      vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops,
      const prefix_data &p,
      int * err);

  void set_up_make_object(
      const map<std::string, bufferlist> &to_set,
      librados::ObjectWriteOperation *owo);

  void set_up_unwrite_object(
      const int &ver, librados::ObjectWriteOperation *owo);

  void set_up_restore_object(
      librados::ObjectWriteOperation *owo);

  void set_up_delete_object(
      librados::ObjectWriteOperation *owo);

  void set_up_remove_prefix(
      const prefix_data &p,
      librados::ObjectWriteOperation * owo,
      int * err);

  int perform_ops( const string &debug_prefix,
      const prefix_data &p,
      vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops);

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
    TIMEOUT(10000,0)
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
    TIMEOUT(1000,0)
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
   * sets up the waitpoints according to wait and resets wait_index
   *
   * @param wait: the array of wait times to use
   */
  void set_waits(const vector<__useconds_t> &wait);

  /**
   * sets up the rados and io_ctx of this KvFlatBtreeAsync. If the don't already
   * exist, creates the index and max object.
   */
  int setup(int argc, const char** argv);

  int set(const string &key, const bufferlist &val,
        bool update_on_existing);

  int remove(const string &key);

  /**
   * returns true if all of the following are true:
   *
   * all objects are accounted for in the index or a prefix
   * 	(i.e., no floating objects)
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

  int remove_all();

  int get_all_keys(std::set<string> *keys);

  int get_all_keys_and_values(map<string,bufferlist> *kv_map);

};

#endif /* KVFLATBTREEASYNC_H_ */
