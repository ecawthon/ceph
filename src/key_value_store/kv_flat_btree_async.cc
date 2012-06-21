/*
 * KvFlatBtreeAsyncParallel.cc
 *
 *  Created on: Jun 18, 2012
 *      Author: eleanor
 */

#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree.h"
#include "key_value_store/kv_flat_btree_async.h"
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

int KvFlatBtreeAsync::next(const string &key, string *ret) {
  current_op << "getting next of " << key << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::set<string> keys;
  oro.omap_get_keys(key,LONG_MAX,&keys,&err);
  io_ctx.operate(index_name, &oro, NULL);
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

int KvFlatBtreeAsync::prev(const string &oid, string *ret) {
  current_op << "getting prev of " << oid << std::endl;
  int err = 0;
  *ret = oid;
  librados::ObjectReadOperation oro;
  std::set<string> keys;
  oro.omap_get_keys("",LONG_MAX,&keys,&err);
  io_ctx.operate(index_name, &oro, NULL);
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

int KvFlatBtreeAsync::oid(const string &key, bufferlist * raw_val) {
  current_op << "getting oid for " << key << std::endl;
  librados::ObjectReadOperation oro;
  int err = 0;
  std::map<string, bufferlist> kvmap;
  oro.omap_get_vals("",LONG_MAX,&kvmap,&err);
  err = io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << current_op.str();
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }

  if (kvmap.size() > 0) {
    for (map<string, bufferlist>::iterator it = kvmap.begin();
	it != kvmap.end(); ++it){
      if(string(it->first.substr(1,it->first.size() - 1)) >= key){
	*raw_val = it->second;
	cout << "oid is returning "
	    << string(raw_val->c_str(), raw_val->length()) << std::endl;
	return err;
      }
    }
  }
  //if we haven't returned yet, it's because the key is higher than the highest
  //existing bucket.
  *raw_val = to_bl(prefix);
  return err;
}

int KvFlatBtreeAsync::oid(const string &key, bufferlist * raw_val,
    string * max_key) {
  current_op << "getting oid for " << key << std::endl;
  librados::ObjectReadOperation oro;
  int err = 0;
  std::map<string, bufferlist> kvmap;
  oro.omap_get_vals("",LONG_MAX,&kvmap,&err);
  err = io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << current_op.str();
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }

  if (kvmap.size() > 0) {
    for (map<string, bufferlist>::iterator it = kvmap.begin();
	it != kvmap.end(); ++it){
      if(it->first >= key){
	*max_key = it->first;
	*raw_val = it->second;
	cout << "oid is returning "
	    << string(raw_val->c_str(), raw_val->length()) << std::endl;
	return err;
      }
    }
  }
  //if we haven't returned yet, it's because the key is higher than the highest
  //existing bucket.
  *max_key = "";
  *raw_val = to_bl(prefix);
  return err;
}

//safe, theoretically
int KvFlatBtreeAsync::split(const string &obj) {
  current_op << "splitting " << obj << std::endl;
  int err = 0;
  librados::ObjectReadOperation get_obj;
  map<string, bufferlist> lower;
  map<string, bufferlist> all;
  std::set<string> keys;
  bufferlist obj_size;

  //read obj
  librados::AioCompletion * obj_aioc = rados.aio_create_completion();
  get_obj.getxattr("size", &obj_size, &err);
  get_obj.omap_get_vals("", LONG_MAX, &all, &err);
  get_obj.omap_get_vals("", k, &lower, &err);
  err = io_ctx.operate(obj, &get_obj, NULL);
  if (err < 0){
    cout << "split: reading " << obj << " failed with " << err;
    cout << std::endl;
    return err;
  }
  if (atoi(string(obj_size.c_str(), obj_size.length()).c_str()) < 2*k){
    current_op << "Can't split - not full" << std::endl;
    return -1;
  }
  int obj_ver = obj_aioc->get_version();

  //make new object with first half of keys
  map<string, bufferlist>::reverse_iterator rit = lower.rbegin();
  string name = to_string(prefix, prefix_num) + rit->first;
  string key1(rit->first);
  bufferlist name1 = to_bl(name);
  bufferlist size1 = to_bl("", lower.size());
  librados::ObjectWriteOperation create_new_small_obj;
  create_new_small_obj.create(true);
  create_new_small_obj.omap_set(lower);
  create_new_small_obj.setxattr("size", size1);
  err = io_ctx.operate(name, &create_new_small_obj);
  if (err < 0) {
    cout << "splitting failed - creating object 1 failed with" << err
	 << std::endl;
    return err;
  }

  //make new object with second half of keys
  bufferlist size2 = to_bl("",
      atoi(string(obj_size.c_str(), obj_size.length()).c_str())
      - atoi(string(size1.c_str(), size1.length()).c_str()));
  map<string,bufferlist> high;
  map<string,bufferlist>::iterator allit = all.find(rit->first);
  high.insert(++allit, all.end());
  bufferlist name2 = to_bl(to_string(prefix,prefix_num) + high.rbegin()->first);
  string key2(high.rbegin()->first);
  librados::ObjectWriteOperation create_new_high_obj;
  create_new_high_obj.create(true);
  create_new_high_obj.omap_set(high);
  create_new_high_obj.setxattr("size", size2);
  err = io_ctx.operate(string(name2.c_str(), name2.length()),
      &create_new_high_obj);
  if (err < 0) {
    cout << "splitting failed - creating second object failed"
	 << " with code " << err << std::endl;
    librados::ObjectWriteOperation clean1;
    clean1.remove();
    io_ctx.operate(name, &clean1);
    return err;
  }

  ///REMOVE THIS PART WHEN NOT DEBUGGING - IT IS O(n)!
  /*cout << "\tmoved the following keys from " << obj << " to " << name
   * << std::endl;
  for (std::set<string>::iterator it = keys.begin(); it != keys.end(); it++) {
    cout << "\t" << *it << std::endl;
  }*/

  //delete obj, asserting the version number
  //index hasn't been updated yet, but that's ok - anything that sees
  //the in between state will die.
  librados::ObjectWriteOperation delete_old;
  librados::AioCompletion * aioc_obj = rados.aio_create_completion();
  io_ctx.set_assert_version(obj_ver);
  delete_old.remove();
  err = io_ctx.operate(obj, &delete_old);
  if (err < 0 || aioc_obj->get_return_value() < 0) {
    if (aioc_obj->get_return_value() < 0)
      err = aioc_obj->get_return_value();
    cout << "rewriting the index failed with code" << err << std::endl;
    librados::ObjectWriteOperation clean2;
    clean2.remove();
    io_ctx.operate(string(name2.c_str(), name2.length()), &clean2);
    librados::ObjectWriteOperation clean1;
    clean1.remove();
    io_ctx.operate(name, &clean1);
    return err;
  }

  //update the index
  //this is safe as long as the split doesn't die before here. if it does,
  //one option would be to have this assert the index version, but that is
  //a bottleneck.
  librados::AioCompletion * aioc = rados.aio_create_completion();
  //io_ctx.set_assert_version(index_ver);
  librados::ObjectWriteOperation update_map_object;
  map<string,bufferlist> map_obj_map;
  map_obj_map.insert(pair<string,bufferlist>(key1, name1));
  map_obj_map.insert(pair<string,bufferlist>(key2, name2));
  update_map_object.omap_set(map_obj_map);
  err = io_ctx.aio_operate(index_name, aioc, &update_map_object);
  aioc->wait_for_safe();
  if (err < 0 || aioc->get_return_value() < 0) {
    cout << "rewriting the index failed - we don't handle this correctly, so"
	 << "don't let this happen" << std::endl;
    return err;
  }

  prefix_num++;

  return err;
}

int KvFlatBtreeAsync::rebalance(const string &oid) {
  current_op << "rebalancing " << oid << std::endl;
  int err = 0;
  string next_obj;
  next(oid, &next_obj);
  if (oid == next_obj) {
    prev(oid, &next_obj);
    if (oid == next_obj) {
      cout << "only one node - cannot rebalance" << std::endl;
      return -1;
    }
    rebalance(next_obj);
    return -1;
  }
  cout << "rebalance: oid is " << oid << " , next_obj is " << next_obj << std::endl;
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
    io_ctx.operate(index_name, &update_index);

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
    /*cout << "\tmoved the following keys from " << big << " to " << new_name << std::endl;
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
    io_ctx.operate(index_name, &update_index);

    librados::ObjectWriteOperation rm_old_obj;
    rm_old_obj.remove();
    io_ctx.operate(oid, &rm_old_obj);
  }

  return err;
}

/*KvFlatBtreeAsync::~KvFlatBtreeAsync() {
  current_op.clear();
  current_op << "removing all" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  librados::AioCompletion * oro_aioc = rados.aio_create_completion();
  std::set<string> index_set;
  oro.omap_get_keys("",LONG_MAX,&index_set,&err);
  err = io_ctx.aio_operate(map_obj_name, oro_aioc, &oro, NULL);
  oro_aioc->wait_for_complete();
  int index_ver = oro_aioc->get_version();

  librados::ObjectWriteOperation rm_index;
  librados::AioCompletion * rm_index_aioc  = rados.aio_create_completion();
  io_ctx.set_assert_version(index_ver);
  rm_index.remove();
  io_ctx.operate(map_obj_name, &rm_index);
  //cout << "removed the index" << std::endl;
  err = rm_index_aioc->get_return_value();
  if (err < 0) {
    cout << "rm index aioc failed - probably failed assertion. " << err;
    cout << std::endl;
    delete this;
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
}*/

string KvFlatBtreeAsync::to_string(string s, int i) {
  stringstream ret;
  ret << s << i;
  return ret.str();
}

bufferlist KvFlatBtreeAsync::to_bl(string s) {
  stringstream strm;
  strm << s;
  bufferlist bl;
  bl.append(strm.str());
  return bl;
}

bufferlist KvFlatBtreeAsync::to_bl(string s, int i) {
  stringstream strm;
  bufferlist bl;
  if (s != "") {
    strm << s;
  }
  strm << i;
  bl.append(strm.str());
  return bl;
}

int KvFlatBtreeAsync::bl_to_int(bufferlist *bl) {
  return atoi(string(bl->c_str(), bl->length()).c_str());
}

int KvFlatBtreeAsync::setup(int argc, const char** argv) {
  int r = rados.init(rados_id.c_str());
  if (r < 0) {
    cout << "error during init" << std::endl;
    return r;
  }
  r = rados.conf_parse_argv(argc, argv);
  if (r < 0) {
    cout << "error during parsing args" << std::endl;
    return r;
  }
  r = rados.conf_parse_env(NULL);
  if (r < 0) {
    cout << "error during parsing env" << std::endl;
    return r;
  }
  r = rados.conf_read_file(NULL);
  if (r < 0) {
    cout << "error during read file" << std::endl;
    return r;
  }
  r = rados.connect();
  if (r < 0) {
    cout << "error during connect" << std::endl;
    return r;
  }
  r = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (r < 0) {
    cout << "error creating io ctx" << std::endl;
    rados.shutdown();
    return r;
  }

  librados::ObjectIterator it;
  for (it = io_ctx.objects_begin(); it != io_ctx.objects_end(); ++it) {
    librados::ObjectWriteOperation rm;
    rm.remove();
    io_ctx.operate(it->first, &rm);
  }

  librados::ObjectWriteOperation make_max_obj;
  make_max_obj.create(false);
  make_max_obj.setxattr("size", to_bl("",0));
  io_ctx.operate(prefix, &make_max_obj);

  librados::ObjectWriteOperation make_index;
  make_index.create(false);
  map<string,bufferlist> index_map;
  index_map[""] = to_bl(prefix);
  make_index.omap_set(index_map);
  r = io_ctx.operate(index_name, &make_index);
  if (r < 0) {
    cout << "Making the index failed with code " << r << std::endl;
    return r;
  }
  return r;
}

//safe, theoretically
int KvFlatBtreeAsync::set(const string &key, const bufferlist &val,
    bool update_on_existing) {
  current_op.clear();
  current_op << "setting " << key << std::endl;
  int err = 0;
  string obj;
  err = oid(key, &obj);
  if (err < 0) {
    cout << "getting oid failed with code " << err;
    cout << std::endl;
    return err;
  }
  cout << "setting key " << key << ": obj is " << obj << std::endl;

  //check for duplicates and initial size
  librados::ObjectReadOperation checkread;
  std::set<string> duplicates;
  bufferlist size;
  checkread.omap_get_keys("",LONG_MAX,&duplicates,&err);
  checkread.getxattr("size", &size, &err);
  err = io_ctx.operate(obj, &checkread, NULL);
  if (err < 0){
    if (err == -61 || err == -2) {
      //object got deleted, we lost the race
      return set(key, val, update_on_existing);
    }
    cout << "read operation failed with code " << err;
    cout << std::endl;
    return err;
  }
  if (duplicates.count(key) == 1 && !update_on_existing) return -17;

  //handle full objects
  while (err != -1){
    err = split(obj);
    if (err < 0 && err != -1) {
      if (err == -61) {
	//we lost the race - start over
	return set(key, val, update_on_existing);
      }
      cout << "split encountered an unexpected error: " << err << std::endl;
      return err;
    }
    oid(key, &obj);
    //cout << "\tnow " << key << " belongs in " << obj << std::endl;
  }

  librados::ObjectReadOperation checkobj;
  librados::AioCompletion * checkobj_aioc = rados.aio_create_completion();
  checkobj.getxattr("size", &size, &err);
  err = io_ctx.aio_operate(obj, checkobj_aioc, &checkobj, NULL);
  if (err < 0){
    if (err == -61) {
      //we lost the race - start over
      return set(key, val, update_on_existing);
    }
    cout << "read operation failed with code " << err;
    cout << std::endl;
    return err;
  }
  checkobj_aioc->wait_for_safe();
  if (checkobj_aioc->get_return_value() < 0) {
    if (checkobj_aioc->get_return_value() == -61) {
      //we lost the race - start over
      return set(key, val, update_on_existing);
    }
    cout << "AioCompletion * failed on reading " << obj << ": "
      << checkobj_aioc->get_return_value() << std::endl;
    return checkobj_aioc->get_return_value();
  }
  int obj_ver = checkobj_aioc->get_version();

  //write
  librados::ObjectWriteOperation owo;
  librados::AioCompletion * write_aioc = rados.aio_create_completion();
  io_ctx.set_assert_version(obj_ver);
  bufferlist size2;
  map<string,bufferlist> to_insert;
  to_insert[key] = val;
  if (duplicates.count(key) == 0) {
    size2 = to_bl("", bl_to_int(&size) + 1);
  } else {
    size2 = to_bl("", bl_to_int(&size));
  }
  owo.omap_set(to_insert);
  owo.setxattr("size",size2);
  err = io_ctx.aio_operate(obj, write_aioc, &owo);
  //cout << "inserted " << key << " with value " << string(to_insert[key].c_str(),
  //    to_insert[key].length()) << " into object " << obj << std::endl;
  //cout << "object " << obj << " now has size " << size2_strm.str() << std::endl;
  if (err < 0) {
    cout << "performing insertion failed with code " << err;
    cout << std::endl;
  }
  write_aioc->wait_for_safe();
  err = write_aioc->get_return_value();
  if (err < 0) {
    cout << "aioc failed - probably failed assert. " << err << std::endl;
    return set(key, val, update_on_existing);
  }

  //rebalance(obj);

  return err;
}

int KvFlatBtreeAsync::remove(const string &key) {
  current_op.clear();
  current_op << "removing " << key << std::endl;
  int err = 0;
  librados::ObjectWriteOperation rm_op;
  std::set<string> to_remove;
  to_remove.insert(key);
  string obj;
  string max_key;
  err = oid(key, &obj, &max_key);
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
  cout << "removing: target_keys size is " << target_keys.size() << std::endl;
  if (target_keys.count(key) == 0) {
    cout << "key does not exist" << std::endl;
    return -2;
  }
  else if (target_keys.size() == 1) {
    rm_op.remove();
    std::set<string> obj_set;
    obj_set.insert(max_key);
    librados::ObjectWriteOperation rm_map;
    rm_map.omap_rm_keys(obj_set);
    io_ctx.operate(index_name, &rm_map);
    io_ctx.operate(obj, &rm_op);
    cout << "removing " << key << std::endl;
    cout << "removing  object " << obj << std::endl;
  } else {
    rm_op.omap_rm_keys(to_remove);
    bufferlist new_size;
    stringstream new_size_strm;
    new_size_strm << size_int - 1;
    new_size.append(new_size_strm.str().c_str());
    rm_op.setxattr("size", new_size);
    io_ctx.operate(obj, &rm_op);
    /*if(is_half_empty(obj)) {
      rebalance(obj);
    }*/
    return err;
  }
  return err;
}

int KvFlatBtreeAsync::remove_all() {
  current_op.clear();
  current_op << "removing all" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  librados::AioCompletion * oro_aioc = rados.aio_create_completion();
  std::set<string> index_set;
  oro.omap_get_keys("",LONG_MAX,&index_set,&err);
  err = io_ctx.aio_operate(index_name, oro_aioc, &oro, NULL);
  if (err < 0){
    if (err == -2) return 0;
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  oro_aioc->wait_for_complete();
  int index_ver = oro_aioc->get_version();

/*  librados::ObjectWriteOperation rm_index;
  librados::AioCompletion * rm_index_aioc  = rados.aio_create_completion();
  io_ctx.set_assert_version(index_ver);
  rm_index.remove();
  io_ctx.operate(map_obj_name, &rm_index);
  //cout << "removed the index" << std::endl;
  err = rm_index_aioc->get_return_value();
  if (err < 0) {
    cout << "rm index aioc failed - probably failed assertion. " << err;
    cout << std::endl;
    return remove_all();
  }*/

  if (index_set.size() != 0) {
    for (std::set<string>::iterator it = index_set.begin();
	it != index_set.end(); ++it){
      librados::ObjectWriteOperation sub;
      sub.remove();
      io_ctx.operate(*it, &sub);
      //cout << "removed " << *it << std::endl;
    }
  }
  return err;
}

int KvFlatBtreeAsync::get(const string &key, bufferlist *val) {
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

int KvFlatBtreeAsync::get_all_keys(std::set<string> *keys) {
  current_op.clear();
  current_op << "getting all keys" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::set<string> index_set;
  oro.omap_get_keys("",LONG_MAX,&index_set,&err);
  io_ctx.operate(index_name, &oro, NULL);
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

int KvFlatBtreeAsync::get_all_keys_and_values(map<string,bufferlist> *kv_map) {
  current_op.clear();
  current_op << "getting all keys and values" << std::endl;
  int err = 0;
  librados::ObjectReadOperation first_read;
  std::set<string> index_set;
  first_read.omap_get_keys("",LONG_MAX,&index_set,&err);
  io_ctx.operate(index_name, &first_read, NULL);
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

int KvFlatBtreeAsync::get_keys_in_range(const string &min_key,
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
  io_ctx.operate(index_name, &index_range, NULL);

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

int KvFlatBtreeAsync::get_key_vals_in_range(string min_key,
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
  io_ctx.operate(index_name, &index_range, NULL);

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

bool KvFlatBtreeAsync::is_consistent() {
  int err;
  bool ret = true;
  std::map<string,bufferlist> index;
  map<string, std::set<string> > sub_objs;
  librados::ObjectReadOperation oro;
  oro.omap_get_vals("",LONG_MAX,&index,&err);
  io_ctx.operate(index_name, &oro, NULL);
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

  std::set<string> keys;
  keys.insert(prefix);
  for (map<string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it) {
    if (it->first != "") {
      keys.insert(string(it->second.c_str(), it->second.length()));
    }
  }

  //make sure that an object exists iff it either is the index
  //or is listed in the index
  for (librados::ObjectIterator oit = io_ctx.objects_begin();
      oit != io_ctx.objects_end(); ++oit) {
    string name = oit->first;
    if (name != index_name && keys.count(name) == 0) {
      cout << "Not consistent! found floating object " << name << std::endl;
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
    if ((size_int > 2*k || size_int < k) && keys.size() > 1
	&& *it != prefix) {
      cout << "Not consistent! Object " << *it << " has size " << size_int
	  << ", which is outside the acceptable range." << std::endl;
      ret = false;
    }

    //check that all keys belong in that object
    for(std::set<string>::iterator subit = sub_objs[*it].begin();
	subit != sub_objs[*it].end(); ++subit) {
      if (*subit <= prev || (*subit > *it && *subit != prefix)) {
	cout << "Not consistent! key " << *subit << " does not belong in "
	    << *it << std::endl;
	ret = false;
      }
    }
  }

  if (!ret) {
    cout << "trace:" << std::endl << current_op.str() << std::endl;
    cout << str();
  }
  return ret;
}

string KvFlatBtreeAsync::str() {
  stringstream ret;
  ret << "Top-level map:" << std::endl;
  int err = 0;
  std::set<string> keys;
  std::map<string,bufferlist> index;
  librados::ObjectReadOperation oro;
  oro.omap_get_vals("",LONG_MAX,&index,&err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0 && err != -5){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return ret.str();
  }
  if(index.size() == 0) {
    ret << "There are no objects!" << std::endl;
    return ret.str();
  }

  keys.insert(prefix);
  for (map<string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it) {
    if (it->first != "") {
      keys.insert(string(it->second.c_str(), it->second.length()));
    }
  }

  vector<string> all_names;
  vector<bufferlist> all_sizes(index.size());
  vector<map<string,bufferlist> > all_maps(keys.size());
  vector<map<string,bufferlist>::iterator> its(keys.size());
  unsigned done = 0;
  vector<bool> dones(keys.size());
  ret << "---------------------" << std::endl;
  ret << "|" << string((19 -
  	(string("").length()+prefix.length()+3))/2,' ');
  ret << "";
  ret << " | ";
  ret << prefix;
  ret << string((19 -
    (string("").length()+prefix.length()+3))/2,' ');
  ret << "|";
  all_names.push_back(prefix);
  cout << std::endl;
  ret << std::endl << "---------------------" << std::endl;

  for (map<string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it){
    if (it->first != "") {
      ret << "|" << string((19 -
	  ((*it).first.length()+it->second.length()+3))/2,' ');
      ret << (*it).first;
      ret << " | ";
      ret << string(it->second.c_str(), it->second.length());
      ret << string((19 -
	  ((*it).first.length()+it->second.length()+3))/2,' ');
      ret << "|\t";
      all_names.push_back(string(it->second.c_str(), it->second.length()));
      ret << std::endl << "---------------------" << std::endl;
    }
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
