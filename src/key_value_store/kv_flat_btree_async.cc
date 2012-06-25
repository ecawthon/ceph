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

int KvFlatBtreeAsync::next(const string &obj_high_key, const string &obj,
    string * ret_high_key, string *ret) {
  current_op << "getting next of " << obj << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<string, bufferlist> kvs;
  oro.omap_get_vals(obj,LONG_MAX,&kvs,&err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "getting kvs failed with error " <<err;
    cout << std::endl;
    return err;
  }

  std::map<string, bufferlist>::iterator it = kvs.upper_bound(obj_high_key);
  if (it == kvs.end()) {
    it--;
    if (it->first != obj) {
      cout << "error: obj not found in index";
      return -2;
    }
  }
  *ret_high_key = it->first;
  prefix_data p;
  int err = parse_prefix(&it->second, &p);
  if (err < 0) {
    cout << "next: invalid prefix found. " << err << std::endl;
    return err;
  }
  *ret = string(p.val.c_str(), p.val.length());
  return err;
}

int KvFlatBtreeAsync::prev(const string &obj_high_key, const string &obj,
    string * ret_high_key, string *ret) {
  current_op << "getting next of " << obj << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<string, bufferlist> kvs;
  oro.omap_get_vals(obj,LONG_MAX,&kvs,&err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "getting kvs failed with error " <<err;
    cout << std::endl;
    return err;
  }

  std::map<string, bufferlist>::iterator it =
      kvs.lower_bound(obj_high_key);
  if (it->first != obj) {
    cout << "error: obj not found in index";
    return -2;
  }
  if (it != kvs.begin()) {
    it--;
  }
  *ret_high_key = it->first;
  prefix_data p;
  int err = parse_prefix(&it->second, &p);
  if (err < 0) {
    cout << "next: invalid prefix found. " << err << std::endl;
    return err;
  }
  *ret = string(p.val.c_str(), p.val.length());
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
  *raw_val = to_bl(client_name);
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
  *raw_val = to_bl(client_name);
  return err;
}

//safe, theoretically
int KvFlatBtreeAsync::split(const string &obj, const string &high_key) {
  current_op << "splitting " << obj << std::endl;
  int err = 0;
  librados::ObjectReadOperation get_obj;
  map<string, bufferlist> lower;
  map<string, bufferlist> all;
  std::set<string> index_keyset;
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
    current_op << "Can't split " << obj << " - not full" << std::endl;
    return -1;
  }
  int obj_ver = obj_aioc->get_version();

  ///////preparations that happen outside the critical section
  //for lower half object
  map<string, bufferlist>::reverse_iterator rit = lower.rbegin();
  string o1w = to_string(client_name, client_index++);
  string key1(rit->first);
  bufferlist size1 = to_bl("", lower.size());
  librados::ObjectWriteOperation write1;
  write1.create(true);
  write1.omap_set(lower);
  write1.setxattr("size", size1);

  //for upper half object
  bufferlist size2 = to_bl("",
      atoi(string(obj_size.c_str(), obj_size.length()).c_str())
      - atoi(string(size1.c_str(), size1.length()).c_str()));
  map<string,bufferlist> high;
  high.insert(++all.find(rit->first), all.end());
  string o2w = to_string(client_name,client_index++);
  string key2(high.rbegin()->first);
  librados::ObjectWriteOperation write2;
  write2.create(true);
  write2.omap_set(high);
  write2.setxattr("size", size2);

  //unwritabling old object
  librados::ObjectWriteOperation unwritable_old;
  io_ctx.set_assert_version(obj_ver);
  unwritable_old.setxattr("unwritable",to_bl("1"));

  //deleting old object
  librados::ObjectWriteOperation delete_old;
  delete_old.remove();

  //index updating
  librados::ObjectWriteOperation update_map_object;
  //TODO: assert entry hasn't changed
  map<string,bufferlist> index_obj_map;
  index_keyset.insert(high_key);
  index_obj_map.insert(pair<string,bufferlist>(key1, to_bl(o1w)));
  index_obj_map.insert(pair<string,bufferlist>(key2, to_bl(o2w)));
  update_map_object.omap_set(index_obj_map);
  update_map_object.omap_rm_keys(index_keyset);

  //setting up initial write of index
  stringstream strm;
  strm << pair_init << key1 << sub_separator << o1w << pair_end
//      << separator
      << pair_init << key2 << sub_separator << o2w << pair_end
      << sub_terminator
      << pair_init << high_key << sub_separator << obj << pair_end
      << terminator << obj;
  bufferlist index_bl = to_bl_f(strm.str());
  std::map<string, bufferlist> prefixed;
  prefixed[high_key] = index_bl;
  librados::ObjectWriteOperation prefix_index;
  //TODO: assert old value of index (should be high key -> obj)
  prefix_index.omap_set(prefixed);


  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for obj
  io_ctx.operate(index_name, &prefix_index);

  //TODO: clone the old object first

  //make new object with first half of keys
  librados::AioCompletion * aioc1 = rados.aio_create_completion();
  librados::AioCompletion * aioc2 = rados.aio_create_completion();
  io_ctx.aio_operate(o1w, aioc1, &write1);
  io_ctx.aio_operate(o2w, aioc2, &write2);
  aioc1->wait_for_safe();
  aioc2->wait_for_safe();
  err = aioc1->get_return_value();
  if (err < 0) {
    //This is not an error that should happen, so not catching it for now.
    cout << "splitting failed - creating first object failed"
	 << " with code " << err << std::endl;
    librados::ObjectWriteOperation clean2;
    clean2.remove();
    io_ctx.operate(o2w, &clean2);
    return err;
  }
  err = aioc2->get_return_value();
  if (err < 0) {
    //This is not an error that should happen, so not catching it for now.
    cout << "splitting failed - creating second object failed"
	 << " with code " << err << std::endl;
    librados::ObjectWriteOperation clean1;
    clean1.remove();
    io_ctx.operate(o1w, &clean1);
    return err;
  }

  //mark the object unwritable, asserting the version number
  librados::AioCompletion * aioc_obj = rados.aio_create_completion();
  err = io_ctx.aio_operate(obj, aioc_obj, &unwritable_old);
  aioc_obj->wait_for_safe();
  err = aioc_obj->get_return_value();
  if (err < 0) {
    cout << "marking the old obj failed with code" << err << std::endl;
    librados::AioCompletion * a1;
    librados::AioCompletion * a2;
    aio_remove_obj(o2w, a2);
    aio_remove_obj(o1w, a1);
    a1->wait_for_safe();
    a2->wait_for_safe();
    return err;
  }

  //delete the unwritable object
  err = io_ctx.operate(obj, &delete_old);
  if (err < 0) {
    cout << "failed to delete " << obj << std::endl;
    return err;
  }

  //update the index
  librados::AioCompletion * index_aioc = rados.aio_create_completion();
  io_ctx.aio_operate(index_name, index_aioc, &update_map_object);
  index_aioc->wait_for_safe();
  err = index_aioc->get_return_value();
  if (err < 0) {
    cout << "rewriting the index failed with code " << err;
    cout << ". this shouldn't happen and is probably a bug." << std::endl;
    librados::ObjectWriteOperation restore;
    restore.setxattr("unwritable",to_bl("0"));
    io_ctx.operate(obj, &restore);
    librados::AioCompletion * a1;
    librados::AioCompletion * a2;
    aio_remove_obj(o2w, a2);
    aio_remove_obj(o1w, a1);
    a1->wait_for_safe();
    a2->wait_for_safe();
    return err;
  }
  /////END CRITICAL SECTION/////

  return err;
}

int KvFlatBtreeAsync::rebalance(const string &o1, const string &hk1) {
  current_op << "rebalancing " << o1 << std::endl;
  int err = 0;
  string o2;
  string hk2;
  next(hk1, o1, &hk2, &o2);
  if (o1 == o2) {
    prev(hk1, o1, &hk2, &o2);
    if (o1 == o2) {
      cout << "only one node - cannot rebalance" << std::endl;
      return -1;
    }
    rebalance(o2, hk2);
    return -1;
  }
  cout << "rebalance: o1 is " << o1 << " , o2 is "
      << o2 << std::endl;

  //read o1
  librados::ObjectReadOperation read_o1;
  librados::AioCompletion * read_o1_aioc = rados.aio_create_completion();
  map<string,bufferlist> o1_map;
  bufferlist size1_bfr;
  bufferlist unw1;
  read_o1.omap_get_vals("", LONG_MAX, &o1_map, &err);
  read_o1.getxattr("size", &size1_bfr, &err);
  read_o1.getxattr("unwritable", &unw1, &err);
  io_ctx.aio_operate(o1, read_o1_aioc, &read_o1, NULL);
  read_o1_aioc->wait_for_safe();
  err = read_o1_aioc->get_return_value();
  if (err < 0 || string(unw1.c_str(), unw1.length()) == "1") {
    if (err == -61 || err == 0) return err;
    else {
      cout << "rebalance found an unexpected error reading"
	  " " << o1 << ": " << err << std::endl;
      return err;
    }
  }
  int vo1 = read_o1_aioc->get_version();
  int size1 = atoi(string(size1_bfr.c_str(), size1_bfr.length()).c_str());

  //read o2
  librados::ObjectReadOperation read_o2;
  librados::AioCompletion * read_o2_aioc = rados.aio_create_completion();
  map<string,bufferlist> o2_map;
  bufferlist size2_bfr;
  bufferlist unw2;
  read_o2.omap_get_vals("",LONG_MAX, &o2_map, &err);
  read_o2.getxattr("size", &size2_bfr, &err);
  read_o2.getxattr("unwritable", &unw2, &err);
  io_ctx.aio_operate(o2, read_o2_aioc, &read_o2, NULL);
  read_o2_aioc->wait_for_safe();
  err = read_o2_aioc->get_return_value();
  if (err < 0 || string(unw2.c_str(), unw2.length()) == "1") {
    if (err == -61 || err == 0) return err;
    else {
      cout << "rebalance found an unexpected error reading"
	  " " << o2 << ": " << err << std::endl;
      return err;
    }
  }
  int vo2 = read_o2_aioc->get_version();
  int size2 = atoi(string(size2_bfr.c_str(), size2_bfr.length()).c_str());

  //calculations
  if (size1 >= k && size1 <= 2*k && size2 >= k && size2 <= 2*k) {
    //nothing to do
    return -1;
  }
  bool rebalance;
  string o1w;
  string o2w;
  librados::ObjectWriteOperation write2;
  //index skeleton
  librados::ObjectWriteOperation prefix_index;
  //TODO: assert index has expected values for both objects
  bufferlist index_bl;
  std::map<string,bufferlist> prefixed_entries;

  //creating first new object skeleton
  librados::ObjectWriteOperation write1;
  map<string, bufferlist> write1_map;
  bufferlist size1w;
  write1.create(true);
  write1.setxattr("unwritable", to_bl("0"));

  //unwritabling old objects
  librados::ObjectWriteOperation flag1;
  flag1.setxattr("unwritable", to_bl("1"));
  librados::ObjectWriteOperation flag2;
  flag2.setxattr("unwritable", to_bl("1"));

  //deleting old objects
  librados::ObjectWriteOperation rm1;
  rm1.remove();
  librados::ObjectWriteOperation rm2;
  rm2.remove();

  //reseting the index
  map<string, bufferlist> new_index;
  std::set<string> index_keyset;
  index_keyset.insert(hk1);
  index_keyset.insert(hk2);
  librados::ObjectWriteOperation fix_index;
  fix_index.omap_rm_keys(index_keyset);

  if (size1 + size2 <= 2*k) {
    //merge
    rebalance = false;
    write1_map.insert(o1_map.begin(), o1_map.end());
    write1_map.insert(o2_map.begin(), o2_map.end());
    string o1w = to_string(client_name, client_index++);

    stringstream pre;
    pre << "1" << ceph_clock_now(g_ceph_context)
	<< pair_init << hk2 << sub_separator << o1w << pair_end
	<< sub_terminator
	<< pair_init << hk1 << sub_separator << o1 << pair_end
	<< sub_terminator
	<< pair_init << hk2 << sub_separator << o2 << pair_end
	<< terminator;
    prefixed_entries[hk1] = pre.str() + o1;
    prefixed_entries[hk2] = pre.str() + o2;


    //deal with ops
    prefix_index.omap_set(prefixed_entries);
    write1.omap_set(write1_map);
    write1.setxattr("size", to_bl(to_string("", write1_map.size())));
    new_index[hk2] = to_bl("0" + o1w);
    fix_index.omap_set(new_index);
  } else {
    rebalance = true;
    map<string, bufferlist> write2_map;
    map<string, bufferlist>::iterator it;
    o1w = to_string(client_name, client_index++);
    o2w = to_string(client_name, client_index++);
    for (it = o1_map.begin();
	it != o1_map.end() && write1_map.size() <= (size1 + size2) / 2; ++it) {
      write1_map.insert(*it);
    }
    if (it != o1_map.end()){
      //write1_map is full, so put the rest in write2_map
      write2_map.insert(it, o1_map.end());
      write2_map.insert(o2_map.begin(), o2_map.end());
    } else {
      //o1_map was small, and write1_map still needs more
      map<string, bufferlist>::iterator it2;
      for(it2 = o2_map.begin();
	  it2 != write1_map.size() <= (size1 + size2) / 2;
	  ++it) {
	write1_map.insert(*it);
      }
      write2_map.insert(it, o2_map.end());
    }

    string hk1w = "0" + write1_map.rbegin()->first;
    string hk2w = "0" + write2_map.rbegin()->first;

    //at this point, write1_map and write2_map should have the correct pairs
    stringstream pre;
    pre << "1" << ceph_clock_now(g_ceph_context)
	<< pair_init << hk1w << sub_separator << o1w << pair_end
	<< pair_init << hk2w << sub_separator << o2w << pair_end
	<< sub_terminator
	<< pair_init << hk1 << sub_separator << o1 << pair_end
	<< sub_terminator
	<< pair_init << hk2 << sub_separator << o2 << pair_end
	<< terminator;
    prefixed_entries[hk1] = pre.str() + o1;
    prefixed_entries[hk2] = pre.str() + o2;
    
    prefix_index.omap_set(prefixed_entries);
    write1.omap_set(write1_map);
    write1.setxattr("size", to_bl(to_string("", write1_map.size())));
    librados::ObjectWriteOperation write2;
    write2.omap_set(write2_map);
    write2.setxattr("size", to_bl(to_string("", write2_map.size())));
    new_index[hk1w] = to_bl("0" + o1w);
    new_index[hk2w] = to_bl("0" + o2w);
    fix_index.omap_set(new_index);
  }
  
  //at this point, all operations should be completely set up.
  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for obj
  io_ctx.operate(index_name, &prefix_index);

  //TODO: clone the old object first

  //make new object with first half of keys
  librados::AioCompletion * aioc1 = rados.aio_create_completion();
  librados::AioCompletion * aioc2 = rados.aio_create_completion();
  io_ctx.aio_operate(o1w, aioc1, &write1);
  if (rebalance) {
    io_ctx.aio_operate(o2w, aioc2, &write2);
    aioc2->wait_for_safe();
    err = aioc2->get_return_value();
      if (err < 0) {
        //This is not an error that should happen, so not catching it for now.
        cout << "splitting failed - creating second object failed"
    	 << " with code " << err << std::endl;
        librados::ObjectWriteOperation clean1;
        clean1.remove();
        io_ctx.operate(o1w, &clean1);
        return err;
      }
  }
  aioc1->wait_for_safe();
  err = aioc1->get_return_value();
  if (err < 0) {
    //This is not an error that should happen, so not catching it for now.
    cout << "splitting failed - creating first object failed"
	 << " with code " << err << std::endl;
    if (rebalance) {
      librados::ObjectWriteOperation clean2;
      clean2.remove();
      io_ctx.operate(o2w, &clean2);
    }
    return err;
  }
  

  //mark the objects unwritable, asserting the version number
  io_ctx.set_assert_version(vo1);
  librados::AioCompletion * aioc1 = rados.aio_create_completion();
  err = io_ctx.aio_operate(o1, aioc1, &flag1);
  io_ctx.set_assert_version(vo2);
  //TODO: is this valid, the way I'm using assert versions?
  librados::AioCompletion * aioc2 = rados.aio_create_completion();
  err = io_ctx.aio_operate(o2, aioc2, &flag2);
  aioc1->wait_for_safe();
  err = aioc1->get_return_value();
  if (err < 0) {
    cout << "marking the old obj failed with code" << err << std::endl;
    aioc2->wait_for_safe();
    librados::ObjectWriteOperation restore2;
    restore2.setxattr("unwritable", to_bl("0"));
    io_ctx.operate(o2, &restore2);
    librados::AioCompletion * a1;
    librados::AioCompletion * a2;
    aio_remove_obj(o2w, a2);
    aio_remove_obj(o1w, a1);
    a1->wait_for_safe();
    a2->wait_for_safe();
    return err;
  }
  aioc2->wait_for_safe();
  err = aioc2->get_return_value();
  if (err < 0) {
    cout << "marking the old obj failed with code" << err << std::endl;
    aioc1->wait_for_safe();
    librados::ObjectWriteOperation restore1;
    restore1.setxattr("unwritable", to_bl("0"));
    io_ctx.operate(o1, &restore1);
    librados::AioCompletion * a1;
    librados::AioCompletion * a2;
    aio_remove_obj(o2w, a2);
    aio_remove_obj(o1w, a1);
    a1->wait_for_safe();
    a2->wait_for_safe();
    return err;
  }
  
  //delete the unwritable object
  err = io_ctx.operate(o1, &rm1);
  if (err < 0) {
    cout << "failed to delete " << o1 << std::endl;
    return err;
  }
  err = io_ctx.operate(o2, &rm2);
  if (err < 0) {
    cout << "failed to delete " << o1 << std::endl;
    return err;
  }

  //update the index
  librados::AioCompletion * index_aioc = rados.aio_create_completion();
  io_ctx.aio_operate(index_name, index_aioc, &fix_index);
  index_aioc->wait_for_safe();
  err = index_aioc->get_return_value();
  if (err < 0) {
    cout << "rewriting the index failed with code " << err;
    cout << ". this shouldn't happen and is probably a bug." << std::endl;
    librados::AioCompletion * a1;
    librados::AioCompletion * a2;
    aio_remove_obj(o2w, a2);
    aio_remove_obj(o1w, a1);
    a1->wait_for_safe();
    a2->wait_for_safe();
    return err;
  }
  /////END CRITICAL SECTION/////
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
  oro_aioc->wait_for_safe();
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

bufferlist KvFlatBtreeAsync::to_bl_f(string s) {
  stringstream ret;
  for (int i = 0; i < s.length(); i++) {
    if (string[i] == pair_init
	|| string[i] == sub_separator
	|| string[i] ==  pair_end
//	|| string[i] == separator
	|| string[i] == sub_terminator
	|| string[i] == terminator) {
      ret << "\\" << string[i];
    }
    else {
      ret << string[i];
    }
  }
  return to_bl(ret.str());
}

int KvFlatBtreeAsync::bl_to_int(bufferlist *bl) {
  return atoi(string(bl->c_str(), bl->length()).c_str());
}

int KvFlatBtreeAsync::parse_prefix(bufferlist * bl, prefix_data * ret) {
  string s(bl->c_str(), bl->length());
  bool escape = false;
  bool end_ts = false;
  int val_index = 1;
  bool val;
  vector<pair<string, string> > * current = ret->to_create;
  pair<stringstream, stringstream> this_pair;
  stringstream ts;
  stringstream * dump_ptr = ts;
  if (s[0] == "0") {
    ret->val = bl;
    return 0;
  }
  for (int i = 0; i < bl->length(); i++) {
    if (!escape) {
      if (s[i] == '\\') {
	escape = true;
      }
      else if (s[i] == pair_init) {
	if (!end_ts) {
	  end_ts = true;
	  dump_ptr = this_pair.first;
	  ret->ts = atoi(ts.str().c_str());
	}
	else if (dump_ptr != &this_pair.second) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  return -2;
	}
	dump_ptr = &this_pair.first;
      }
      else if (s[i] == sub_separator) {
	if (!end_ts || dump_ptr != &this_pair.first) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  return -2;
	}
	dump_ptr = &this_pair.second;
      }
      else if (s[i] == pair_end) {
	if (!end_ts || dump_ptr != &this_pair.second) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  return -2;
	}
	current->push_back(
	    pair<string,string>(this_pair.first.str(), this_pair.second.str()));
      }
      else if (s[i] == sub_terminator) {
	if (!end_ts || !(dump_ptr == &this_pair.second
	    && current == &(ret->to_create))) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  return -2;
	}
	current = ret->to_delete;
      } else if (!end_ts || s[i] == terminator) {
	if (!(dump_ptr == &this_pair.second
	    && current == &(ret->to_delete))) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  return -2;
	}
	val_index = i + 1;
	val = true;
      } else {
	*dump_ptr << s[i];
      }
    }
    else {
      if (val) {
	string value(s.substr(val_index, s.length() - 1));
	ret->val = to_bl(value);
	break;
      }
      else {
	*dump_ptr << s[i];
      }
    }
  }
  return 0;
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
  io_ctx.operate(client_name, &make_max_obj);

  librados::ObjectWriteOperation make_index;
  make_index.create(false);
  map<string,bufferlist> index_map;
  index_map["1"] = to_bl(client_name);
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
  oro_aioc->wait_for_safe();
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
  keys.insert(client_name);
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
	&& *it != client_name) {
      cout << "Not consistent! Object " << *it << " has size " << size_int
	  << ", which is outside the acceptable range." << std::endl;
      ret = false;
    }

    //check that all keys belong in that object
    for(std::set<string>::iterator subit = sub_objs[*it].begin();
	subit != sub_objs[*it].end(); ++subit) {
      if (*subit <= prev || (*subit > *it && *subit != client_name)) {
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

  keys.insert(client_name);
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
  	(string("").length()+client_name.length()+3))/2,' ');
  ret << "";
  ret << " | ";
  ret << client_name;
  ret << string((19 -
    (string("").length()+client_name.length()+3))/2,' ');
  ret << "|";
  all_names.push_back(client_name);
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
