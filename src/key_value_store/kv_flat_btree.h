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

class KvFlatBtree : public KeyValueStructure{
protected:
  int k;
  static int increment;
  librados::IoCtx io_ctx;
  string map_obj_name;
  stringstream current_op;



  //Things that NEVER modify objects
  /**
   * returns the name of the next bucket higher than the one to which key maps.
   * If key is higher than the highest key in map_obj, ret = key.
   *
   * Does not modify any objects
   *
   * @param key the key to check
   * @param ret next key will be stored here.
   */
  virtual int next(const string &key, string *ret);

  /**
   * returns the name of the highest bucket whose name is < oid. If oid is not
   * in the index, ret = oid.
   *
   * Does not modify any objects
   */
  virtual int prev(const string &oid, string *ret);

  /**
   * Returns true if object oid has size >= 2k.
   *
   * @pre: oid exists (otherwise returns ENOENT)
   */
  virtual bool is_full(const string &oid);

  /**
   * Returns true if object oid has size < k.
   *
   * @pre: oid exits (otherwise returns ENOENT)
   */
  virtual bool is_half_empty(const string &oid);


  //Things that OFTEN modify objects
  /**
   * sets oid to the name of the object in which key belongs.
   *
   * If key is higher than the max entry in the index, creates a new object
   * called key and sets oid to that, and inserts that into the index.
   *
   */
  virtual int oid(const string &key, string * oid);

  //Things that ALWAYS modify objects
  /**
   * Splits obj into two objects, one with name obj and one with the name of
   * the kth entry in obj.
   *
   * @post: the smaller object will have exactly k entries. The bigger object
   * will have its original size - k entries.
   */
  virtual int split(const string &obj);

  /**
   * Rearranges entries s.t. oid has at least k entries.
   *
   *
   */
  virtual int rebalance(const string &oid);

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

  //modifiers

  /**
   * creates the index if it doesn't exist.
   */
  virtual int set(const string &key, const bufferlist &val,
        bool update_on_existing);

  virtual int remove(const string &key);

  virtual int remove_all();

  //readers
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
