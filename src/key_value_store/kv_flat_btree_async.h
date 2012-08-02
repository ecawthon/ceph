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
 * stores information about a key in the index.
 *
 * prefix is "0" unless key is "", in which case it is "1". This ensures that
 * the object with key "" will always be the highest key in the index.
 */
struct key_data {
  string raw_key;
  string prefix;

  key_data()
  {}

  /**
   * @pre: key is a raw key (does not contain a prefix)
   */
  key_data(string key)
  : raw_key(key)
  {
    raw_key == "" ? prefix = "1" : prefix = "0";
  }

  /**
   * parses the prefix from encoded and stores the data in this.
   *
   * @pre: encoded has a prefix
   */
  void parse(string encoded) {
    prefix = encoded[0];
    raw_key = encoded.substr(1,encoded.length());
  }

  /**
   * returns a string containing the encoded (prefixed) key
   */
  string encoded() const {
    return prefix + raw_key;
  }
};

/**
 * The index object is a key value map that stores
 * the highest key stored in an object as keys, and an index_data
 * as the corresponding value. The index_data contains the encoded
 * high key, the name of the librados object where keys containing
 * that range of keys are located, and information about split and
 * merge operations that may need to be cleaned up if a client dies.
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
  key_data kdata;
  //the name of the object where the key range is located.
  string obj;

  index_data()
  {}

  index_data(string raw_key)
  : kdata(raw_key)
  {}

  //true if there is a prefix and now - ts > timeout.
  bool is_timed_out(utime_t now);

  //note that this does not include the key
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
    strm << '(' << kdata.encoded() << ',' << prefix;
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
  key_data kdata; //the max key, from the index
  string name; //the object's name
  map<std::string, bufferlist> omap; // the omap of the object
  bool unwritable; // an xattr that, if false, means an op is in
		  // progress and other clients should not write to it.
  int version; //the version at time of read
  int size; //the number of elements in the omap

  object_data()
  {}

  object_data(string the_name)
  : name(the_name)
  {}

  object_data(key_data kdat, string the_name)
  : kdata(kdat),
    name(the_name)
  {}

  object_data(key_data kdat, string the_name,
      map<std::string, bufferlist> the_omap)
  : kdata(kdat),
    name(the_name),
    omap(the_omap)
  {}

  object_data(key_data kdat, string the_name, int the_version)
  : kdata(kdat),
    name(the_name),
    version(the_version)
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
   * than idata.key. If idata.key is the max key, returns -EOVERFLOW. If
   * idata has a prefix and has timed out, cleans up.
   *
   * @param idata: idata for the object to search for.
   * @param out_data: the idata for the next object.
   *
   * @pre: idata must contain a key.
   * @post: out_data contains complete information
   */
  int next(const index_data &idata, index_data * out_data);

  /**
   * finds the object in the index with the lowest key value that is greater
   * than idata.key. If idata.key is the lowest key, returns -ERANGE If
   * idata has a prefix and has timed out, cleans up.
   *
   * @param idata: idata for the object to search for.
   * @param out_data: the idata for the next object.
   *
   * @pre: idata must contain a key.
   * @ost: out_data contains complete information
   */
  int prev(const index_data &idata, index_data * out_data);

  /**
   * finds the index_data where a key belongs.
   *
   * @param key: the key to search for
   * @param idata: the index_data for the first index value such that idata.key
   * is greater than key.
   * @pre: key is not encoded
   * @post: idata contains complete information
   * stored
   */
  int read_index(const string &key, index_data * idata);

  //These sometimes modify objects and the index

  /**
   * Reads obj and generates information about it. If necessary, splits it.
   *
   * @param idata: index data for the object being split
   * @param odata: object data read from the object to be split
   * @pre: idata contains a key and an obj
   * @post: odata has complete information
   * @return -1 if obj does not need to be split,
   */
  int split(const index_data &idata, object_data *odata);

  /**
   * reads o1 and the next object after o1 and, if necessary, rebalances them.
   * if hk1 is the highest key in the index, calls rebalance on the next highest
   * key.
   *
   * @param idata: index data for the object being rebalanced
   * @param odata: object data read from idata.obj (if !reverse) or the next
   * idata in the index (if reverse).
   * @pre: idata contains a key and an obj
   * @post: odata has complete information about whichever object it read
   * @param reverse: if true, ver will be the version of the next object after
   * o1. This is used when rebalance calls itself on the previous object.
   * @return -1 if no change needed, -ENOENT if o1 does not exist,
   * -ECANCELED if the rebalance fails due to another thread (meaning rebalance
   * should be repeated)
   */
  int rebalance(const index_data &idata1, object_data * odata1,
      bool reverse);

  /**
   * performs an ObjectReadOperation to populate odata
   *
   * @post: odata has all information about obj except for key (which is "")
   */
  int read_object(const string &obj, object_data * odata);

  /**
   * sets up owo to change the index in preparation for a split/merge.
   *
   * @param to_create: vector of object_data to be created.
   * @param to_delete: vector of object_data to be deleted.
   * @param owo: the ObjectWriteOperation to set up
   * @param idata: will be populated by index data for this op.
   * @param err: error code reference to pass to omap_cmp
   * @pre: entries in to_create and to_delete must have keys and names.
   */
  void set_up_prefix_index(
      const vector<object_data> &to_create,
      const vector<object_data> &to_delete,
      librados::ObjectWriteOperation * owo,
      index_data * idata,
      int * err);

  /**
   * sets up all make, mark, restore, and delete ops, as well as the remove
   * prefix op, based on idata.
   *
   * @param create_data: vector of data about the objects to be created.
   * @pre: entries in create_data must have names and omaps and be in idata
   * order
   * @param to_delete: vector of data about the objects to be deleted
   * @pre: entries in to_delete must have versions and be in idata order
   * @param ops: the owos to set up. the pair is a pair of op identifiers
   * and names of objects - set_up_ops fills these in.
   * @pre: ops must be the correct size and the ObjectWriteOperation pointers
   * must be valid.
   * @param idata: the idata with information about how to set up the ops
   * @pre: idata has valid to_create and to_delete
   * @param err: the int to get the error value for omap_cmp
   */
  void set_up_ops(
      const vector<object_data> &create_data,
      const vector<object_data> &delete_data,
      vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops,
      const index_data &idata,
      int * err);

  /**
   * sets up owo to exclusive create, set omap to to_set, and set
   * unwritable to "0"
   */
  void set_up_make_object(
      const map<std::string, bufferlist> &to_set,
      librados::ObjectWriteOperation *owo);

  /**
   * sets up owo to assert object version and that object version is
   * writable,
   * then mark it unwritable.
   *
   * @param ver: if this is 0, no version is asserted.
   */
  void set_up_unwrite_object(
      const int &ver, librados::ObjectWriteOperation *owo);

  /**
   * sets up owo to assert that an object is unwritable and then mark it
   * writable
   */
  void set_up_restore_object(
      librados::ObjectWriteOperation *owo);

  /**
   * sets up owo to assert that the object is unwritable and then remove it
   */
  void set_up_delete_object(
      librados::ObjectWriteOperation *owo);

  /**
   * perform the operations in ops and handles errors.
   *
   * @param debug_prefix: what to print at the beginning of debug output
   * @param idata: the idata for the object being operated on, to be
   * passed to cleanup if necessary
   * @param ops: this contains an int identifying the type of op,
   * a string that is the name of the object to operate on, and a pointer
   * to the ObjectWriteOperation to use. All of this must be complete.
   * @post: all operations are performed and most errors are handled
   * (e.g., cleans up if an assertion fails). If an unknown error is found,
   * returns it.
   */
  int perform_ops( const string &debug_prefix,
      const index_data &idata,
      vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops);

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
