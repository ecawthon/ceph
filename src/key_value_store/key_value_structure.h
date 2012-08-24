/*
 * key_value_structure.hpp
 *
 *  Created on: Jun 8, 2012
 *      Author: eleanor
 */

#ifndef KEY_VALUE_STRUCTURE_HPP_
#define KEY_VALUE_STRUCTURE_HPP_

#include "include/rados/librados.hpp"
#include "include/utime.h"
#include <vector>

using std::string;
using std::map;
using std::set;
using ceph::bufferlist;

class KeyValueStructure;

typedef int (KeyValueStructure::*injection_t)();
typedef void (*callback)(int * err, void *arg);

class KeyValueStructure{
public:
  map<char, int> opmap;
  /**
   * returns 0
   */
  virtual int nothing() = 0;


  /**
   * 10% chance of waiting wait_ms seconds
   */
  virtual int wait() = 0;

  /**
   * 10% chance of killing the client.
   */
  virtual int suicide() = 0;

  ////////////////DESTRUCTOR/////////////////
  virtual ~KeyValueStructure() {};

  ////////////////UPDATERS///////////////////

  /**
   * set up the KeyValueStructure (i.e., initialize rados/io_ctx, etc.)
   */
  virtual int setup(int argc, const char** argv) = 0;


  virtual void set_inject(injection_t inject, int wait_time) = 0;

  /**
   * if update_on_existing is false, returns an error if
   * key already exists in the structure
   */
  virtual int set(const string &key, const bufferlist &val,
      bool update_on_existing) = 0;

  /**
   * removes the key-value for key. returns an error if key does not exist
   */
  virtual int remove(const string &key) = 0;

  /**
   * removes all keys and values
   */
  virtual int remove_all() = 0;

  ////////////////READERS////////////////////
  /**
   * gets the val associated with key.
   *
   * @param key the key to get
   * @param val the value is stored in this
   * @return error code
   */
  virtual int get(const string &key, bufferlist *val) = 0;

  virtual int set_many(const map<string, bufferlist> &in_map) = 0;

  /**
   * stores all keys in keys. set should put them in order by key.
   */
  virtual int get_all_keys(std::set<string> *keys) = 0;

  /**
   * stores all keys and values in kv_map. map should put them in order by key.
   */
  virtual int get_all_keys_and_values(map<string,bufferlist> *kv_map) = 0;

  virtual void aio_get(const string &key, bufferlist *val, callback cb,
      void *cb_args, int * err) = 0;

  virtual void aio_set(const string &key, const bufferlist &val, bool exclusive,
      callback cb, void * cb_args, int * err) = 0;

  virtual void aio_remove(const string &key, callback cb, void *cb_args,
      int * err) = 0;

  /**
   * True if the structure meets its own requirements for consistency.
   */
  virtual bool is_consistent() = 0;

  /**
   * prints a string representation of the structure
   */
  virtual string str() = 0;
};


#endif /* KEY_VALUE_STRUCTURE_HPP_ */
