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
#include "include/encoding.h"
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


/**
 * The index object is a key value map that stores
 * the highest key stored in an object as keys, and an index_data
 * as the corresponding value. The index_data contains the name
 * of the librados object where keys containing that range of keys
 * are located, and information about split and merge operations that
 * may need to be cleaned up if a client dies.
 */
struct index_data {
  //"1" if there is a prefix (because a split or merge is
  //in progress), otherwise ""
  string prefix;
  utime_t ts; //time that a split/merge started
  //objects to be created. the elements have
  //two elements, first the (encoded) key, then the object name.
  vector<vector<string> > to_create;
  //objects to be deleted. The elements have three elements, first,
  //the (encoded) key, then the object name, and finally, the version number of
  //the object to be deleted at the last read.
  vector<vector<string> > to_delete;

  //the encoded key corresponding to the object
  string key;
  //the name of the object where the key range is located.
  string obj;

  index_data()
  {}

  index_data(string raw_key)
  {
    if (raw_key == ""){
      key = "1";
    } else {
      key = "0" + raw_key;
    }
  }

  //true if there is a prefix and now - ts > timeout.
  bool is_timed_out(utime_t now);

  void encode(bufferlist &bl) const {
    ENCODE_START(1,1,bl);
    ::encode(prefix, bl);
    ::encode(ts, bl);
    ::encode(prefix, bl);
    ::encode(to_create, bl);
    ::encode(to_delete, bl);
    ::encode(obj, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator &p) {
    DECODE_START(1, p);
    ::decode(prefix, p);
    ::decode(ts, p);
    ::decode(prefix, p);
    ::decode(to_create, p);
    ::decode(to_delete, p);
    ::decode(obj, p);
    DECODE_FINISH(p);
  }

  /*
   * Prints a string representation of the information, in the following format:
   * (key,
   * prefix
   * ts
   * elements of to_create, organized into (high key| obj name)
   * ;
   * elements of to_delete, organized into (high key| obj name | version number)
   * :
   * val)
   */
  string str() const {
    stringstream strm;
    strm << '(' << key << ',' << prefix;
    if (prefix == "1") {
      strm << ts.sec() << '.' << ts.usec();
      for(vector<vector<string> >::const_iterator it = to_create.begin();
	  it != to_create.end(); ++it) {
	  strm << '(' << (*it)[0] << '|'
	      << (*it)[1] << ')';
      }
      strm << ';';
      for(vector<vector<string> >::const_iterator it = to_delete.begin();
	  it != to_delete.end(); ++it) {
	  strm << '(' << (*it)[0] << '|'
	      << (*it)[1] << '|'
	      << (*it)[2] << ')';
      }
      strm << ':';
    }
    strm << obj << ')';
    return strm.str();
  }
};

/**
 * Stores information read from a librados object.
 */
struct object_data {
  string raw_key; //the max key
  string name; //the object's name
  map<std::string, bufferlist> omap; // the omap of the object
  bool unwritable; // an xattr that, if false, means an op is in
		  // progress and other clients should not write to it.
  int version; //the version at time of read
  int size; //the number of elements in the omap

  object_data()
  {}

  object_data(string the_key, string the_name)
  : raw_key(the_key),
    name(the_name)
  {}

  object_data(string the_key, string the_name,
      map<std::string, bufferlist> the_omap)
  : raw_key(the_key),
    name(the_name),
    omap(the_omap)
  {}
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
  const static int timeout_seconds = 1;
  utime_t TIMEOUT;
  friend struct index_data;

  /**
   * Waits for a period determined by the waits vector (does not wait if waits
   * vector is empty). called before most ObjectOperations.
   */
  int interrupt();

  //These read, but do not write, librados objects

  /**
   * finds the object in the index with the lowest key value that is greater
   * than obj_high_key. If obj_high_key is the max key, returns -EOVERFLOW
   *
   * @param obj_high_key: the key stored in the index for obj
   * @param obj: the name of the object to search for
   * @param ret_high_key: string to store the key of the next high object
   * @param ret: string to store the name of the next high object
   */
  int next(const index_data &idata, index_data * out_data);

  /**
   * finds the object in the index with the lowest key value that is greater
   * than obj_high_key. If obj_high_key is the lowest key, returns -ERANGE
   *
   * @param obj_high_key: the key stored in the index for obj
   * @param obj: the name of the object to search for
   * @param ret_high_key: string to store the key of the next high object
   * @param ret: string to store the name of the next high object
   */
  int prev(const index_data &idata, index_data * out_data);

  /**
   * finds the index_data and high key where a key belongs.
   *
   * @param key: the key to search for
   * @param raw_val: the bufferlist in the first index value such that its key
   * is greater than key. This may include a prefix.
   * @param max_key: string where the (encoded) key of the object should be
   * stored
   */
  int read_index(const string &key, index_data * idata);

  //These sometimes modify objects and the index

  /**
   * Reads obj and generates information about it. If necessary, splits it.
   *
   * @param obj: the object to read/split
   * @param high_key: the (encoded) key of obj in the index
   * @param info: all information read from the object
   * @return -1 if obj does not need to be split,
   */
  int split(const index_data &idata, object_data *odata);

  /**
   * reads o1 and the next object after o1 and, if necessary, rebalances them.
   * if hk1 is the highest key in the index, calls rebalance on the next highest
   * key.
   *
   * @param o1: the name of the object to check
   * @param hk1: the key corresponding to o1
   * @param info1: all the information read from the object
   * @param reverse: if true, ver will be the version of the next object after
   * o1. This is used when rebalance calls itself on the previous object.
   * @return -1 if no change needed, -ENOENT if o1 does not exist,
   * -ECANCELED if the rebalance fails due to another thread (meaning rebalance
   * should be repeated)
   */
  int rebalance(const index_data &idata1, object_data * odata1,
      bool reverse);

  /**
   * Called when a client discovers that another client has died during  a
   * split or a merge. cleans up after that client.
   *
   * @param idata: the index data parsed from the index entry left by the dead
   * client.
   * @param errno: the error that caused the client to realize the other client
   * died (should be -ENOENT or -ETIMEDOUT)
   * @post: rolls forward if -ENOENT, otherwise rolls back.
   */
  int cleanup(const index_data &idata, const int &errno);

  /**
   * performs an ObjectReadOperation to populate info
   */
  int read_object(const string &obj, object_data * odata);

  /**
   * sets up owo to change the index in preparation for a split/merge.
   *
   * @param to_create: the objects to be created, as a map of encoded keys to values
   */
  void set_up_prefix_index(
      const vector<object_data> &to_create,
      const vector<object_data> &to_delete,
      librados::ObjectWriteOperation * owo,
      index_data * idata,
      int * err);

  void set_up_ops(
      const vector<object_data> &create_data,
      const vector<object_data> &delete_data,
      vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops,
      const index_data &idata,
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
      const index_data &idata,
      librados::ObjectWriteOperation * owo,
      int * err);

  int perform_ops( const string &debug_prefix,
      const index_data &idata,
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
    TIMEOUT(2,0)
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

  static bufferlist to_bl(const index_data &idata);

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
