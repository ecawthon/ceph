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

class KeyValueStructure{
public:
  ////////////////DESTRUCTOR/////////////////
  virtual ~KeyValueStructure() {};

  ////////////////UPDATERS///////////////////

  /**
   * set up the KeyValueStructure (i.e., initialize rados/io_ctx, etc.)
   */
  virtual int setup(int argc, const char** argv) = 0;

  /**
   * set up waitpoints for verification testing
   */
  virtual void set_waits(const vector<__useconds_t> &wait) = 0;

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

  /**
   * stores all keys in keys. set should put them in order by key.
   */
  virtual int get_all_keys(std::set<string> *keys) = 0;

  /**
   * stores all keys and values in kv_map. map should put them in order by key.
   */
  virtual int get_all_keys_and_values(map<string,bufferlist> *kv_map) = 0;

  /**
   * gets keys starting at min_key and ending after max_key or after max_keys
   * keys in key_set
   *
   * @param min_key the key to start at
   * @param max_key the last key to return, unless max_keys is reached first.
   * pass NULL to rely on max_keys instead.
   * @param key_set the keys are stored in this
   * @param max_keys the number of keys to return, unless max_key is hit first.
   * pass -1 to max_keys to get all keys in range
   */
  //virtual int get_keys_in_range(const string &min_key, const string &max_key,
  //    std::set<string> *key_set, int max_keys) = 0;

  /**
   * stores keys and values starting at min_key and ending at max_key
   * or after max_keys keys in kv_map
   *
   * @param min_key the key to start at
   * @param max_key the key after the last key to return, unless max_keys
   * is reached first. pass NULL to rely on max_keys instead.
   * @param kv_map the results are stored here
   * @param max_keys the number of keys to return, unless max_key is hit first.
   * pass -1 to max_keys to get all keys in range
   */
  //virtual int get_key_vals_in_range(string min_key,
  //    string max_key, map<string,bufferlist> *kv_map, int max_keys) = 0;

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
