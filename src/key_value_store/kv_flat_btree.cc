/*
 * kv_flat_btree.cc
 *
 *  Created on: Jun 8, 2012
 *      Author: eleanor
 */

#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree.h"
#include "include/rados/librados.hpp"

#include <string>
#include <climits>
#include <iostream>
#include <cassert>
#include <cmath>
#include <sstream>
#include <stdlib.h>
#include <iterator>

using namespace std;
using ceph::bufferlist;

int KvFlatBtree::next(const string &key, string *ret) {
  current_op << "getting next of " << key << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::set<string> keys;
  oro.omap_get_keys(key,LONG_MAX,&keys,&err);
  io_ctx.operate(map_obj_name, &oro, NULL);
  if (err < 0){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }

  if (keys.size() == 0 || (keys.count(key) == 1 && keys.size() == 1)) {
    *ret = key;
    return err;
  }
  std::set<string>::iterator it = keys.begin();
  if (keys.count(key) == 0) {
    *ret = *it;
    return err;
  }
  else if (keys.size() > 1){
    it++;
    *ret = *it;
    return err;
  }

  return err;

  //(*ret)[ret->length() - 1] = static_cast<char>(key[key.length() - 1] + 2*k);
}

int KvFlatBtree::prev(const string &oid, string *ret) {
  current_op << "getting prev of " << oid << std::endl;
  int err = 0;
  *ret = oid;
  librados::ObjectReadOperation oro;
  std::set<string> keys;
  oro.omap_get_keys("",LONG_MAX,&keys,&err);
  io_ctx.operate(map_obj_name, &oro, NULL);
  if (err < 0){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }

  if (keys.size() == 0) {
    return err;
  }
  std::set<string>::iterator it = keys.lower_bound(oid);
  if (keys.count(oid) == 0) {
    *ret = *it;
    return err;
  }
  else if (it != keys.begin()){
    it--;
    *ret = *it;
    return err;
  }

  return err;
}

int KvFlatBtree::oid(const string &key, string * oid) {
  current_op << "getting oid for " << key << std::endl;
  librados::ObjectReadOperation oro;
  int err = 0;
  std::set<string> keys;
  oro.omap_get_keys("",LONG_MAX,&keys,&err);
  io_ctx.operate(map_obj_name, &oro, NULL);
  if (err < 0){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }

  if (keys.size() > 0) {
    for (std::set<string>::iterator it = keys.begin(); it != keys.end(); ++it){
      if(*it >= key){
	*oid = *it;
	return 0;
      }
    }
  }
  //if we haven't returned yet, it's because the key is higher than the highest
  //existing bucket.
  next(key, oid);
  bufferlist zero;
  zero.append("0");
  librados::ObjectWriteOperation create;
  create.create(false);
  create.setxattr("size", zero);
  io_ctx.operate(*oid, &create);

  librados::ObjectWriteOperation make_new_max;
  map<string,bufferlist> new_max_map;
  new_max_map.insert(pair<string,bufferlist>(*oid, bufferlist()));
  make_new_max.omap_set(new_max_map);
  io_ctx.operate(map_obj_name, &make_new_max);
  return 0;
}

bool KvFlatBtree::is_full(const string &oid){
  current_op << "checking if " << oid << " is full" << std::endl;
  int err;
  librados::ObjectReadOperation oro;
  bufferlist size;
  oro.getxattr("size", &size, &err);
  io_ctx.operate(oid, &oro, NULL);
  if (err < 0) {
      cout << "isfull's read failed with " << err << std::endl;
      return false;
  }
  if (atoi(string(size.c_str(), size.length()).c_str()) >= 2*k) return true;
  return false;
}

bool KvFlatBtree::is_half_empty(const string &oid) {
  current_op << "checking if " << oid << " is half empty" << std::endl;
  int err;
  librados::ObjectReadOperation oro;
  bufferlist size;
  oro.getxattr("size", &size, &err);
  io_ctx.operate(oid, &oro, NULL);
  if (err < 0) {
      cout << "getting size failed with code " << err;
      cout << std::endl;
      return false;
  }
  if (atoi(string(size.c_str(), size.length()).c_str()) < k) return true;
  return false;
}

int KvFlatBtree::split(const string &obj) {
  current_op << "splitting " << obj << std::endl;
  int err = 0;
  librados::ObjectReadOperation get_obj;
  map<string, bufferlist> contents;
  std::set<string> keys;
  bufferlist obj_size;
  get_obj.getxattr("size", &obj_size, &err);
  get_obj.omap_get_keys("", k, &keys, &err);
  get_obj.omap_get_vals("", k, &contents, &err);
  err = io_ctx.operate(obj, &get_obj, NULL);
  if (err < 0){
    cout << "read operation failed with code " << err;
    cout << std::endl;
    return err;
  }
  if(contents.size() == 0) {
    cout << "error: splitting empty object" << std::endl;
    return -61;
  }

  map<string, bufferlist>::reverse_iterator rit = contents.rbegin();
  string name = rit->first;

  librados::ObjectWriteOperation create_new_obj;
  stringstream size1;//this is the size of the new object
  size1 << contents.size();
  bufferlist size1_bfr;
  size1_bfr.append(size1.str());
  create_new_obj.create(false);
  create_new_obj.omap_set(contents);
  create_new_obj.setxattr("size", size1_bfr);
  err = io_ctx.operate(name, &create_new_obj);
  if (err < 0) {
    cout << "splitting failed" << std::endl;
    return err;
  }

  bufferlist new_size;//this is the new size of the old (higher) object)
  stringstream new_size_strm;
  new_size_strm <<
      atoi(string(obj_size.c_str(), obj_size.length()).c_str())
      - atoi(string(size1_bfr.c_str(), size1_bfr.length()).c_str());
  new_size.append(new_size_strm.str());

  librados::ObjectWriteOperation remove_old_keys;
  remove_old_keys.omap_rm_keys(keys);
  remove_old_keys.setxattr("size", new_size);
  err = io_ctx.operate(obj, &remove_old_keys);
  if (err < 0) {
    cout << "splitting failed" << std::endl;
    return err;
  }

  ///REMOVE THIS PART WHEN NOT DEBUGGING - IT IS O(n)!
/*  cout << "\tmoved the following keys from " << obj << " to " << name
 * << std::endl;
  for (std::set<string>::iterator it = keys.begin(); it != keys.end(); it++) {
    cout << "\t" << *it << std::endl;
  }*/

  librados::ObjectWriteOperation update_map_object;
  map<string,bufferlist> map_obj_map;
  map_obj_map.insert(pair<string,bufferlist>(name, bufferlist()));
  update_map_object.omap_set(map_obj_map);
  err = io_ctx.operate(map_obj_name, &update_map_object);
  if (err < 0) {
    cout << "splitting failed" << std::endl;
    return err;
  }
  return err;
}

int KvFlatBtree::rebalance(const string &oid) {
  current_op << "rebalancing " << oid << std::endl;
  int err = 0;
  string next_obj;
  next(oid, &next_obj);
  if (oid == next_obj) {
    prev(oid, &next_obj);
    if (oid == next_obj) {
      //cout << "only one node - cannot rebalance" << std::endl;
      return 0;
    }
    rebalance(next_obj);
    return 0;
  }
  //cout << "rebalance: oid is " << oid << " , next_obj is " << next_obj << std::endl;
  map<string, bufferlist> kvs_to_add;
  std::map<string, bufferlist> next_obj_map;
  std::set<string> keys_to_rm;
  map<string, bufferlist> oid_kvs;
  std::set<string> rm_from_index;
  rm_from_index.insert(oid);

  librados::ObjectReadOperation read_oid;
  bufferlist size;
  read_oid.getxattr("size", &size, &err);
  read_oid.omap_get_vals("",LONG_MAX, &oid_kvs, &err);
  io_ctx.operate(oid, &read_oid, NULL);
  int oid_size = atoi(string(size.c_str(), size.length()).c_str());

  librados::ObjectReadOperation oro;
  bufferlist next_size;
  oro.getxattr("size", &next_size, &err);
  oro.omap_get_vals("", k / 2 + 1, &next_obj_map, &err);
  io_ctx.operate(next_obj, &oro, NULL);
  if (err < 0) {
    cout << "getting size failed with code " << err;
    cout << std::endl;
    return true;
  }
  int next_obj_size =
      atoi(string(next_size.c_str(), next_size.length()).c_str());

  string small = oid;
  string big = next_obj;
  map<string,bufferlist> *small_map = &oid_kvs;
  map<string,bufferlist> *big_map = &next_obj_map;
  int small_size = oid_size;
  int big_size = next_obj_size;
  map<string, bufferlist>::iterator it = big_map->begin();

  if (next_obj_size < oid_size && oid_size > k) {
    current_op << "rebalancing: object to the right is smaller" << std::endl;
    small = next_obj;
    big = oid;
    small_map = &next_obj_map;
    big_map = &oid_kvs;
    small_size = next_obj_size;
    big_size = next_obj_size;
    map<string,bufferlist>::reverse_iterator it = big_map->rbegin();
  }

  if (big_size > k) {
    //big has extra keys, so steal some
    //cout << small << " is stealing from " << big << std::endl;
    bufferlist new_size;
    stringstream new_size_strm;
    bufferlist new_big_size;
    stringstream new_big_size_strm;

    if (big_size - k == 1) {
      kvs_to_add.insert(*it);
      keys_to_rm.insert(it->first);
      new_size_strm << small_size + 1;
      new_big_size_strm << big_size - 1;
    } else {
      new_size_strm << small_size + (big_size - k) / 2;
      new_big_size_strm << big_size - (big_size - k) / 2;
      for (int i = 0; i < (next_obj_size - k) / 2; i++) {
	kvs_to_add.insert(*it);
	keys_to_rm.insert(it->first);
	++it;

	//if (it == big_map->end() || it == big_map->rend()) break;
      }
    }

    string new_name = kvs_to_add.rbegin()->first;
    kvs_to_add.insert(small_map->begin(), small_map->end());

    //create new object with all the keys
    librados::ObjectWriteOperation add_keys;
    new_size.append(new_size_strm.str().c_str());
    add_keys.omap_set(kvs_to_add);
    add_keys.setxattr("size", new_size);
    io_ctx.operate(new_name, &add_keys);

    //update map_obj - this means gets/sets for keys in the range being moved
    //will immediately start getting pointed to the new object, so it doesn't
    //matter that we haven't removed them from next_obj yet.
    //however, next_obj still has a high size - this could cause it to be split
    //inappropriately. I'll fix it later.
    librados::ObjectWriteOperation update_index;
    map<string, bufferlist> add_to_index;
    add_to_index.insert(pair<string,bufferlist>(new_name, bufferlist()));
    update_index.omap_rm_keys(rm_from_index);
    update_index.omap_set(add_to_index);
    io_ctx.operate(map_obj_name, &update_index);

    //remove keys from next_obj
    librados::ObjectWriteOperation rm_keys;
    new_big_size.append(new_big_size_strm.str().c_str());
    rm_keys.omap_rm_keys(keys_to_rm);
    rm_keys.setxattr("size", new_big_size);
    io_ctx.operate(oid, &rm_keys);

    //delete the old object
    librados::ObjectWriteOperation rm_old_obj;
    rm_old_obj.remove();
    io_ctx.operate(oid, &rm_old_obj);

    ///REMOVE THIS PART WHEN NOT DEBUGGING - IT IS O(n)!
    /*cout << "\tmoved the following keys from " << big << " to " << new_name
     * << std::endl;
    for (std::set<string>::iterator it = keys_to_rm.begin();
	it != keys_to_rm.end(); it++) {
      cout << "\t" << *it << std::endl;
    }*/
  } else {
    //big has k or fewer entries, so we can safely merge the two.
    //cout << "merging " << small << " and " << big << std::endl;
    librados::ObjectWriteOperation add_keys;
    stringstream new_next_size_strm;
    bufferlist new_next_size;
    new_next_size_strm << oid_size + next_obj_size;
    new_next_size.append(new_next_size_strm.str().c_str());
    add_keys.omap_set(oid_kvs);
    add_keys.setxattr("size", new_next_size);
    io_ctx.operate(next_obj, &add_keys);

    librados::ObjectWriteOperation update_index;
    update_index.omap_rm_keys(rm_from_index);
    io_ctx.operate(map_obj_name, &update_index);

    librados::ObjectWriteOperation rm_old_obj;
    rm_old_obj.remove();
    io_ctx.operate(oid, &rm_old_obj);
  }

  return err;
}

KvFlatBtree& KvFlatBtree::operator=(const KvFlatBtree &kvb) {
  if (this == &kvb) return *this;
  k = kvb.k;
  increment = kvb.increment;
  io_ctx = kvb.io_ctx;
  map_obj_name = kvb.map_obj_name;
  current_op.clear();
  current_op << "constructor";
  return *this;
}

int KvFlatBtree::set(const string &key, const bufferlist &val,
    bool update_on_existing) {
  current_op.clear();
  current_op << "setting " << key << std::endl;
  int err = 0;
  //cout << "setting"  << std::endl;
  //make sure index exists
  librados::ObjectWriteOperation make_index;
  make_index.create(false);
  err = io_ctx.operate(map_obj_name, &make_index);
  if (err < 0) {
    cout << "Making the index failed with code " << err << std::endl;
    return err;
  }

  map<string,bufferlist> to_insert;
  to_insert[key] = val;
  string obj;
  err = oid(key, &obj);
  if (err < 0) {
    cout << "getting oid failed with code " << err;
    cout << std::endl;
    return err;
  }

  //check for duplicates and initial size
  librados::ObjectReadOperation checkread;
  std::set<string> duplicates;
  bufferlist size;
  checkread.omap_get_keys("",LONG_MAX,&duplicates,&err);
  checkread.getxattr("size", &size, &err);
  err = io_ctx.operate(obj, &checkread, NULL);
  if (err < 0){
    cout << "read operation failed with code " << err;
    cout << std::endl;
    return err;
  }

  //handle duplicates
  if (duplicates.count(key) == 1) {
    if (!update_on_existing) {
      //key already exists, so exit
      return -17;
    }
    else {
      librados::ObjectWriteOperation rm_dups;
      std::set<string> dups;
      dups.insert(key);
      rm_dups.omap_rm_keys(dups);
      stringstream sizestrm;
      bufferlist sizebfr;
      string size_str1(size.c_str(), size.length());
      sizestrm << atoi(size_str1.c_str()) - 1;
      sizebfr.append(sizestrm.str());
      size = sizebfr;
      rm_dups.setxattr("size", sizebfr);
      err = io_ctx.operate(obj, &rm_dups);
      if (err < 0){
	cout << "removing duplicates failed with code " << err;
	cout << std::endl;
	return err;
      }
    }
  }

  //this must happen after duplicates are handled
  while (is_full(obj)){
    split(obj);
    oid(key, &obj);
    //cout << "\tnow " << key << " belongs in " << obj << std::endl;

    librados::ObjectReadOperation checkread;
    checkread.getxattr("size", &size, &err);
    err = io_ctx.operate(obj, &checkread, NULL);
    if (err < 0){
      cout << "read operation failed with code " << err;
      cout << std::endl;
      return err;
    }
  }

  //write
  librados::ObjectWriteOperation owo;
  bufferlist size2;
  stringstream size2_strm;
  string size_str = string(size.c_str(), size.length());
  size2_strm << (atoi(size_str.c_str()) + 1);
  size2.append(size2_strm.str());
  owo.omap_set(to_insert);
  owo.setxattr("size",size2);
  err = io_ctx.operate(obj, &owo);
  //cout << "inserted " << key << " with value " << string(to_insert[key].c_str(),
  //    to_insert[key].length()) << " into object " << obj << std::endl;
  //cout << "object " << obj << " now has size " << size2_strm.str() << std::endl;
  if (err < 0) {
    //if (err == -17) err = 0;
    //else {
      cout << "performing insertion failed with code " << err;
      cout << std::endl;
    //}
  }

  if(is_half_empty(obj)) {
    rebalance(obj);
  }

  return err;
}

int KvFlatBtree::remove(const string &key) {
  current_op.clear();
  current_op << "removing " << key << std::endl;
  int err = 0;
  librados::ObjectWriteOperation rm_op;
  std::set<string> to_remove;
  to_remove.insert(key);
  string obj;
  err = oid(key, &obj);
  if (err < 0) {
    cout << "getting oid failed with code " << err;
    cout << std::endl;
    return err;
  }

  std::set<string> target_keys;
  librados::ObjectReadOperation oro;
  bufferlist size;
  oro.omap_get_keys("", LONG_MAX, &target_keys, &err);
  oro.getxattr("size", &size, &err);
  err = io_ctx.operate(obj, &oro, NULL);
  if (err < 0){
    cout << "read operation failed with code " << err;
    cout << std::endl;
    return err;
  }
  int size_int = atoi(string(size.c_str(), size.length()).c_str());

  if (target_keys.size() == 1) {
    rm_op.remove();
    std::set<string> obj_set;
    obj_set.insert(obj);
    librados::ObjectWriteOperation rm_map;
    rm_map.omap_rm_keys(obj_set);
    io_ctx.operate(map_obj_name, &rm_map);
    io_ctx.operate(obj, &rm_op);
    //cout << "removing " << key << std::endl;
    //cout << "removing  object " << obj << std::endl;
  } else {
    rm_op.omap_rm_keys(to_remove);
    bufferlist new_size;
    stringstream new_size_strm;
    new_size_strm << size_int - 1;
    new_size.append(new_size_strm.str().c_str());
    rm_op.setxattr("size", new_size);
    io_ctx.operate(obj, &rm_op);
    if(is_half_empty(obj)) {
      rebalance(obj);
    }
    return err;
  }

  return err;
}

int KvFlatBtree::remove_all() {
  current_op.clear();
  current_op << "removing all" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::set<string> index_set;
  oro.omap_get_keys("",LONG_MAX,&index_set,&err);
  err = io_ctx.operate(map_obj_name, &oro, NULL);
  if (err < 0){
    if (err == -2) return 0;
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  if (index_set.size() != 0) {
    for (std::set<string>::iterator it = index_set.begin();
	it != index_set.end(); ++it){
      librados::ObjectWriteOperation sub;
      sub.remove();
      io_ctx.operate(*it, &sub);
      //cout << "removed " << *it << std::endl;
    }
  }
  librados::ObjectWriteOperation rm_index;
  rm_index.remove();
  io_ctx.operate(map_obj_name, &rm_index);
  //cout << "removed the index" << std::endl;
  return err;
}

int KvFlatBtree::get(const string &key, bufferlist *val) {
  current_op.clear();
  current_op << "getting " << key << std::endl;
  int err = 0;
  std::set<string> key_set;
  key_set.insert(key);
  map<string,bufferlist> omap;
  string obj;
  err = oid(key, &obj);
  if (err < 0) {
    cout << "getting oid failed with code " << err;
    cout << std::endl;
    return err;
  }
  librados::ObjectReadOperation read;
  read.omap_get_vals_by_keys(key_set, &omap, &err);
  err = io_ctx.operate(obj, &read, NULL);
  if (err < 0) {
    cout << "reading failed " << err;
    cout << std::endl;
    return err;
  }

  *val = omap[key];

  return err;
}

int KvFlatBtree::get_all_keys(std::set<string> *keys) {
  current_op.clear();
  current_op << "getting all keys" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::set<string> index_set;
  oro.omap_get_keys("",LONG_MAX,&index_set,&err);
  io_ctx.operate(map_obj_name, &oro, NULL);
  if (err < 0){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  for (std::set<string>::iterator it = index_set.begin();
      it != index_set.end(); ++it){
    librados::ObjectReadOperation sub;
    std::set<string> ret;
    sub.omap_get_keys("",LONG_MAX,&ret,&err);
    io_ctx.operate(*it, &sub, NULL);
    keys->insert(ret.begin(), ret.end());
  }
  return err;
}

int KvFlatBtree::get_all_keys_and_values(map<string,bufferlist> *kv_map) {
  current_op.clear();
  current_op << "getting all keys and values" << std::endl;
  int err = 0;
  librados::ObjectReadOperation first_read;
  std::set<string> index_set;
  first_read.omap_get_keys("",LONG_MAX,&index_set,&err);
  io_ctx.operate(map_obj_name, &first_read, NULL);
  if (err < 0){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  for (std::set<string>::iterator it = index_set.begin();
      it != index_set.end(); ++it){
    librados::ObjectReadOperation sub;
    map<string, bufferlist> ret;
    sub.omap_get_vals("",LONG_MAX,&ret,&err);
    io_ctx.operate(*it, &sub, NULL);
    kv_map->insert(ret.begin(), ret.end());
  }
  return err;
}

int KvFlatBtree::get_keys_in_range(const string &min_key,
    const string &max_key, std::set<string> *key_set, int max_keys) {
  int err = 0;
  string min_oid;
  string max_oid;
  if (max_keys < 0) max_keys = INT_MAX;
  uint64_t i = max_keys; //the number of keys already gotten
  err = oid(min_key, &min_oid);
  if (err < 0){
    cout << "getting min key failed with code " << err;
    cout << std::endl;
    return err;
  }
  err = oid(max_key, &max_oid);
  if (err < 0){
    cout << "getting max key failed with code " << err;
    cout << std::endl;
    return err;
  }

  librados::ObjectReadOperation index_range;
  std::set<string> index_keys;
  index_range.omap_get_keys(min_oid, max_keys, &index_keys, &err);
  io_ctx.operate(map_obj_name, &index_range, NULL);

  std::set<string>::iterator it = index_keys.begin();
  librados::ObjectReadOperation sub;
  std::set<string> ret;
  bool min_reached;
  sub.omap_get_keys(min_key, i, &ret, &err);
  io_ctx.operate(*(++it), &sub, NULL);
  if (err < 0) {
    cout << "getting first key from first sub object failed with " << err;
    cout << std::endl;
    return err;
  }
  for (std::set<string>::iterator that = ret.begin(); that != ret.end();
      ++that) {
    if (*that == min_key) min_reached = true;
    if (min_reached && i > 0 && *that != max_key){
      key_set->insert(*that);
      --i;
    }
  }

  for (std::set<string>::iterator it = index_keys.begin();
      it != index_keys.end(); ++it){
    librados::ObjectReadOperation sub;
    std::set<string> ret;
    sub.omap_get_keys("", i, &ret, &err);
    io_ctx.operate(*it, &sub, NULL);
    if (err < 0) {
      cout << "getting" << i << "the key from sub object failed with " << err;
      cout << std::endl;
      return err;
    }
    for (std::set<string>::iterator that = ret.begin(); that != ret.end();
        ++that) {
      if (i > 0 && *that != max_key){
        key_set->insert(*that);
        --i;
      }
    }
  }
  return err;
}

int KvFlatBtree::get_key_vals_in_range(string min_key,
      string max_key, map<string,bufferlist> *kv_map, int max_keys) {
  int err = 0;
  string min_oid;
  string max_oid;
  if (max_keys < 0) max_keys = INT_MAX;
  uint64_t i = max_keys; //the number of keys already gotten
  err = oid(min_key, &min_oid);
  if (err < 0){
    cout << "getting min key failed with code " << err;
    cout << std::endl;
    return err;
  }
  err = oid(max_key, &max_oid);
  if (err < 0){
    cout << "getting max key failed with code " << err;
    cout << std::endl;
    return err;
  }

  librados::ObjectReadOperation index_range;
  std::set<string> index_keys;
  index_range.omap_get_keys(min_oid, max_keys, &index_keys, &err);
  io_ctx.operate(map_obj_name, &index_range, NULL);

  std::set<string>::iterator it = index_keys.begin();
  librados::ObjectReadOperation sub;
  map<string,bufferlist> ret;
  bool min_reached;
  sub.omap_get_vals(min_key, i, &ret, &err);
  io_ctx.operate(*(++it), &sub, NULL);
  if (err < 0) {
    cout << "getting first key from first sub object failed with " << err;
    cout << std::endl;
    return err;
  }
  for (map<string,bufferlist>::iterator that = ret.begin(); that != ret.end();
      ++that) {
    if (that->first == min_key) min_reached = true;
    if (min_reached && i > 0 && that->first != max_key){
      kv_map->insert(*that);
      --i;
    } else return err;
  }

  for (std::set<string>::iterator it = index_keys.begin();
      it != index_keys.end(); ++it){
    librados::ObjectReadOperation sub;
    map<string,bufferlist> ret;
    sub.omap_get_vals("", i, &ret, &err);
    io_ctx.operate(*it, &sub, NULL);
    if (err < 0) {
      cout << "getting" << i << "the key from sub object failed with " << err;
      cout << std::endl;
      return err;
    }
    for (map<string,bufferlist>::iterator that = ret.begin();
	that != ret.end(); ++that) {
      if (i > 0 && that->first != max_key){
        kv_map->insert(*that);
        --i;
      } else return err;
    }
  }
  return err;
}

bool KvFlatBtree::is_consistent() {
  int err;
  bool ret = true;
  std::set<string> keys;
  map<string, std::set<string> > sub_objs;
  librados::ObjectReadOperation oro;
  oro.omap_get_keys("",LONG_MAX,&keys,&err);
  io_ctx.operate(map_obj_name, &oro, NULL);
  if (err < 0){
    //probably because the index doesn't exist - this might be ok.
    for (librados::ObjectIterator oit = io_ctx.objects_begin();
        oit != io_ctx.objects_end(); ++oit) {
      //if this executes, there are floating objects.
      cout << "Not consistent! found floating object " << oit->first;
      cout << std::endl;
      ret = false;
    }
    return ret;
  }

  //make sure that an object exists iff it either is the index
  //or is listed in the index
  for (librados::ObjectIterator oit = io_ctx.objects_begin();
      oit != io_ctx.objects_end(); ++oit) {
    string name = oit->first;
    if (name != map_obj_name && keys.count(name) == 0) {
      cout << "Not consistent! found floating object " << name;
      ret = false;
    }
  }

  //check objects
  string prev = "";
  for (std::set<string>::iterator it = keys.begin(); it != keys.end(); ++it) {
    librados::ObjectReadOperation read;
    bufferlist size;
    read.getxattr("size", &size, &err);
    read.omap_get_keys("", LONG_MAX, &sub_objs[*it], &err);
    io_ctx.operate(*it, &read, NULL);
    int size_int = atoi(string(size.c_str(), size.length()).c_str());

    //check that size is right
    if (size_int != (int)sub_objs[*it].size()) {
      cout << "Not consistent! Object " << *it << " has size xattr "
	  << size_int << " when it contains " << sub_objs[*it].size()
	  << " keys!" << std::endl;
      ret = false;
    }

    //check that size is in the right range
    if ((size_int > 2*k || size_int < k) && keys.size() > 1) {
      cout << "Not consistent! Object " << *it << " has size " << size_int
	  << ", which is outside the acceptable range." << std::endl;
      ret = false;
    }

    //check that all keys belong in that object
    for(std::set<string>::iterator subit = sub_objs[*it].begin();
	subit != sub_objs[*it].end(); ++subit) {
      if (*subit <= prev || *subit > *it) {
	cout << "Not consistent! key " << *subit << " does not belong in "
	    << *it << std::endl;
	ret = false;
      }
    }
  }

  if (!ret) {
    cout << "trace:" << std::endl << current_op << std::endl;
    cout << str();
  }
  return ret;
}

string KvFlatBtree::str() {
  stringstream ret;
  ret << "Top-level map:" << std::endl;
  int err = 0;
  std::set<string> keys;
  librados::ObjectReadOperation oro;
  oro.omap_get_keys("",LONG_MAX,&keys,&err);
  io_ctx.operate(map_obj_name, &oro, NULL);
  if (err < 0 && err != -5){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return ret.str();
  }
  if(keys.size() == 0) {
    ret << "There are no objects!" << std::endl;
    return ret.str();
  }
  vector<string> all_names;
  vector<bufferlist> all_sizes(keys.size());
  vector<map<string,bufferlist> > all_maps(keys.size());
  vector<map<string,bufferlist>::iterator> its(keys.size());
  unsigned done = 0;
  vector<bool> dones(keys.size());
  ret << "---------------------" << std::endl;
  for (std::set<string>::iterator it = keys.begin(); it != keys.end(); ++it){
    ret << "|";
    ret << string((19 - it->length())/2, ' ');
    ret << *it;
    ret << string((19 - it->length())/2, ' ');
    ret << "|" << std::endl;
    ret << "---------------------" << std::endl;
    all_names.push_back(*it);
  }
  ret << std::endl;
  int indexer = 0;

  //get the object names and sizes
  for(vector<string>::iterator it = all_names.begin(); it != all_names.end();
      ++it) {
    librados::ObjectReadOperation oro;
    bufferlist size;
    oro.getxattr("size",&size, &err);
    oro.omap_get_vals("", LONG_MAX, &all_maps[indexer], &err);
    io_ctx.operate(*it, &oro, NULL);
    all_sizes[indexer] = size;
    indexer++;
  }

  ret << "///////////////////OBJECT NAMES////////////////" << std::endl;
  //HEADERS
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
   ret << "---------------------\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "|" << string((19 -
	(string("Bucket: ").length() + all_names[i].length()))/2, ' ');
    ret << "Bucket: " << all_names[i];
    ret << string((19 -
    	(string("Bucket: ").length() + all_names[i].length()))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    its[i] = all_maps[i].begin();
    ret << "|" << string((19 - (string("size: ").length()
	  + all_sizes[i].length()))/2, ' ');
    ret << "size: " << string(all_sizes[i].c_str(),all_sizes[i].length());
    ret << string((19 - (string("size: ").length()
	  + all_sizes[i].length()))/2, ' ') << "|\t";

  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "---------------------\t";
  }
  ret << std::endl;
  ret << "///////////////////THE ACTUAL BLOCKS////////////////" << std::endl;


  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "---------------------\t";
  }
  ret << std::endl;
  //each time through this part is two lines
  while(done < keys.size()) {
    for(int i = 0; i < indexer; i++) {
      if(dones[i]){
	ret << "                    \t";
      } else {
	if (its[i] == all_maps[i].end()){
	  done++;
	  dones[i] = true;
	  ret << "                    \t";
	}
	else {
	  ret << "|" << string((19 -
	      ((*its[i]).first.length()+its[i]->second.length()+3))/2,' ');
	  ret << (*its[i]).first;
	  ret << " | ";
	  ret << string(its[i]->second.c_str(), its[i]->second.length());
	  ret << string((19 -
	      ((*its[i]).first.length()+its[i]->second.length()+3))/2,' ');
	  ret << "|\t";
	  ++(its[i]);
	}

      }
    }
    ret << std::endl;
    for (int i = 0; i < indexer; i++) {
      if(dones[i]){
	ret << "                    \t";
      } else {
	ret << "---------------------\t";
      }
    }
    ret << std::endl;

  }
  return ret.str();
}
