/*
 * KvFlatBtreeAsyncParallel.cc
 *
 *  Created on: Jun 18, 2012
 *      Author: eleanor
 */

#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree_async.h"
#include "include/rados/librados.hpp"
#include "/usr/include/asm-generic/errno.h"
#include "/usr/include/asm-generic/errno-base.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "common/Clock.h"
#include "include/rados.h"
#include "include/types.h"


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

int KvFlatBtreeAsync::interrupt() {
  //if (client_name == "rados1."  && waits[wait_index++] > 0) {
  if (client_name != "client0." && rand() % 10 == 0) {
    cout << client_name << " is suiciding"
    // << because waits[" << wait_index - 1 << "] is " << waits[wait_index - 1]
    << std::endl;
    return 1;
  }/* else if (client_name == "client0.") {
    usleep(1000);
  } else if(wait_index < (int)waits.size()) {*/
  /*if (rand() % 10 == 0) {
    usleep(500000);
  }*/
  return 0;
}

int KvFlatBtreeAsync::parse_prefix(bufferlist * bl, index_data * ret) {
  bufferlist::iterator p = bl->begin();
  ret->decode(p);
  cout << "\t\t" << client_name << ": parsing prefix from "
      //<< string(bl->c_str(), bl->length())
      << ret->str()
      << std::endl;
  return 0;
  string s(bl->c_str(), bl->length());
  bool escape = false;
  bool end_ts = false;
  bool creating = true;
  int val_index = 1;
  bool val = false;
  vector<vector<string> > * current = &ret->to_create;
  stringstream this_pair[2];
  stringstream triple[3];
  stringstream ts_sec;
  stringstream ts_usec;
  stringstream * dump_ptr = &ts_sec;
  if (s[0] == '0') {
    ret->val =
	string(bl->c_str(), bl->length()).substr(1,bl->length() - 1);
    return 0;
  }
  for (int i = 1; i < (int)bl->length(); i++) {
    if (val) {
      string value(s.substr(val_index, s.length() - 1));

      ret->val = value;
      break;
    } else if (!end_ts && s[i] == '.') {
      dump_ptr = &ts_usec;
    } else if (!escape) {
      if (s[i] == '\\') {
	escape = true;
      } else if (s[i] == pair_init) {
	if (!end_ts) {
	  end_ts = true;
	  ret->ts = utime_t(atoi(ts_sec.str().c_str()),
	      atoi(ts_usec.str().c_str()));
	} else if (dump_ptr != &this_pair[1] && dump_ptr != &triple[2]) {
	  cout << "\t\t\t" << client_name << "-parse_prefix: erring after"
	      << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	if (creating) {
	  dump_ptr = &this_pair[0];
	} else {
	  dump_ptr = &triple[0];
	}
	dump_ptr->str("");
      } else if (s[i] == sub_separator) {
	if (!end_ts || (dump_ptr != &this_pair[0] && dump_ptr != &triple[0]
	                                             && dump_ptr != &triple[1]))
	{
	  cout << "\t\t\t" << client_name << "-parse_prefix: erring after"
	      << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	if (creating) {
	  dump_ptr = &this_pair[1];
	} else if (dump_ptr == &triple[0]) {
	  dump_ptr = &triple[1];
	} else if (dump_ptr == &triple[1]) {
	  dump_ptr = &triple[2];
	}
	dump_ptr->str("");
      } else if (s[i] == pair_end) {
	if (!end_ts || (dump_ptr != &this_pair[1]
	    && dump_ptr != &triple[2]))
	{
	  cout << "\t\t\t" << client_name << "-parse_prefix: erring after"
	      << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	vector<string> v;
	if (creating) {
	  v.push_back(this_pair[0].str());
	  v.push_back(this_pair[1].str());
	} else {
	  v.push_back(triple[0].str());
	  v.push_back(triple[1].str());
	  v.push_back(triple[2].str());
	}
	current->push_back(v);
      } else if (s[i] == sub_terminator) {
	if (!end_ts || !(dump_ptr == &this_pair[1]
	    && current == &(ret->to_create))) {
	  cout << "\t\t\t" << client_name << "-parse_prefix: erring after"
	      << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	creating = false;
	current = &ret->to_delete;
      } else if (s[i] == terminator) {
	if (!(dump_ptr == &ts_sec || (dump_ptr == &triple[2]
	    && current == &(ret->to_delete)))) {
	  cout << "\t\t\t" << client_name << "-parse_prefix: erring after"
	      << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	val_index = i + 1;
	ret->prefix = s.substr(0,val_index);
	val = true;
      } else {
	*dump_ptr << s[i];
      }
    } else {
      *dump_ptr << s[i];
      escape = false;
    }
  }
  cout << "\t\t\t" << client_name << "-parse_prefix: val = "
      << string(ret->val.c_str(), ret->val.length())
      << std::endl;
  return 0;
}

int KvFlatBtreeAsync::next(const string &obj_high_key, const string &obj,
    string * ret_high_key, string *ret) {
  cout << "\t\t" << client_name << "-next: finding next of (" << obj_high_key
      << "," << obj << ")" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<std::string, bufferlist> kvs;
  oro.omap_get_vals(obj_high_key,1,&kvs,&err);
  err = io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "\t\t\t" << client_name << "-next: getting index failed with error "
	<<err;
    cout << std::endl;
    return err;
  }
  if (kvs.size() > 0) {
    *ret_high_key = kvs.begin()->first;
    index_data idata;
    err = parse_prefix(&kvs.begin()->second, &idata);
    if (err < 0) {
      cerr << "\t\t\t" << client_name << "-next: invalid prefix found. "
	  << err << std::endl;
      return err;
    }
    if (idata.prefix != "" && ceph_clock_now(g_ceph_context) - idata.ts > TIMEOUT) {
      cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
      //the client died after deleting the object. clean up.
      cleanup(idata, err);
    }
    *ret = idata.val;
  } else {
    *ret_high_key = obj_high_key;
    *ret = obj;
  }
  return err;
}

int KvFlatBtreeAsync::prev(const string &obj_high_key, const string &obj,
    string * ret_high_key, string *ret) {
  cout << "\t\t" << client_name << "-prev: finding prev of ("
      << obj_high_key << "," << obj << ")" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<std::string, bufferlist> kvs;
  oro.omap_get_vals("",LONG_MAX,&kvs,&err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "\t\t\t" << client_name << "-prev: getting kvs failed with error "
	<< err << std::endl;
    return err;
  }

  std::map<std::string, bufferlist>::iterator it =
      kvs.lower_bound(obj_high_key);
  if (it->first != obj_high_key) {
    cout << "error: obj not found in index" << std::endl;
    return -ENOENT;
  }
  if (it != kvs.begin()) {
    it--;
  }
  *ret_high_key = it->first;
  index_data idata;
  err = parse_prefix(&it->second, &idata);
  if (err < 0) {
    cerr << "\t\t\t" << client_name << "-next: invalid prefix found. " << err
	<< std::endl;
    return err;
  }
  if (idata.prefix != "" && ceph_clock_now(g_ceph_context) - idata.ts > TIMEOUT) {
    cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
    //the client died after deleting the object. clean up.
    cleanup(idata, err);
  }
  *ret = idata.val;
  return err;
}

int KvFlatBtreeAsync::oid(const string &key, bufferlist * raw_val,
    string * max_key) {
  cout << "\t" << client_name << "-oid: getting oid for " << key << std::endl;
  librados::ObjectReadOperation oro;
  int err = 0;
  std::set<std::string> key_set;
  key_set.insert(key);
  std::map<std::string, bufferlist> kvmap;
  std::map<std::string, bufferlist> dupmap;
  oro.omap_get_vals_by_keys(key_set, &dupmap, &err);
  oro.omap_get_vals(key,1,&kvmap,&err);
  err = io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "\t" << client_name << "-oid: getting keys failed with error "
	<< err << std::endl;
    return err;
  }
  if (dupmap.size() > 0) {
    *max_key = key;
    *raw_val = dupmap[key];
    return err;
  } else if (kvmap.size() == 0) {
    return oid(key, raw_val, max_key);
  } else {
    *max_key = kvmap.begin()->first;
    *raw_val = kvmap.begin()->second;
  }
  return err;
}

int KvFlatBtreeAsync::split(const string &obj, const string &high_key,
    object_info *info) {
  cout << "\t\t" << client_name << "-split: splitting " << obj << std::endl;
  int err = 0;
  info->key = high_key;

  //read obj
  cout << "\t\t" << client_name << "-split: reading " << obj << std::endl;
  err = read_object(obj, info);
  if (err < 0){
    //possibly -ENOENT, meaning someone else deleted it.
    cerr << "\t\t" << client_name << "-split: reading " << obj
	<< " failed with " << err;
    cout << std::endl;
    return err;
  }
  cout << "\t\t" << client_name << "-split: size of " << obj << " is "
      << info->size << ", version is " << info->version << std::endl;
  if (info->size < 2*k){
    return -1;
  }

  ///////preparations that happen outside the critical section
  //for prefix index
  index_data idata;
  std::map<std::string, std::string> to_create;
  vector<std::map<std::string, bufferlist> > maps_to_create(2);
  vector<object_info*> to_delete;
  to_delete.push_back(info);

  //for lower half object
  map<std::string, bufferlist>::iterator it = info->omap.begin();
  for (int i = 0; i < k; i++) {
    maps_to_create[0].insert(*it);
    it++;
  }
  string low_key = "0" + maps_to_create[0].rbegin()->first;
  to_create[low_key] =
      to_string(client_name, client_index++);

  //for upper half object
  maps_to_create[1].insert(
      ++info->omap.find(maps_to_create[0].rbegin()->first), info->omap.end());
  to_create[high_key] = to_string(client_name,client_index++);

  //setting up operations
  librados::ObjectWriteOperation owos[6];
  vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > ops;
  set_up_prefix_index(to_create, to_delete, &owos[0], &idata, &err);
  ops.push_back(make_pair(
      pair<int, string>(ADD_PREFIX, index_name),
      &owos[0]));
  for (int i = 1; i < 6; i++) {
    ops.push_back(make_pair(make_pair(0,""), &owos[i]));
  }
  set_up_ops(maps_to_create, to_delete, &ops, idata, &err);

  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for obj
  err = perform_ops("\t\t" + client_name + "-split:", idata, &ops);
  if (err < 0) {
    return err;
  }
  cout << "\t\t" << client_name << "-split: done splitting." << std::endl;
  /////END CRITICAL SECTION/////

  return err;
}

int KvFlatBtreeAsync::rebalance(const string &o1, const string &hk1,
    object_info *outfo,//like info, but it is output
    bool reverse){
  int err = 0;
  object_info info1;
  object_info info2;
  bool only = false;
  info1.name = o1;
  info1.key = hk1;
  err = next(hk1, o1, &info2.key, &info2.name);
  if (err < 0) {
    return err;
  }
  cout << "\t\t" << client_name << "-rebalance: next is (" << info2.key << ","
      << info2.name << ")" << std::endl;
  if (o1 == info2.name) {
    prev(hk1, o1, &info2.key, &info2.name);
    cout << "\t\t" << client_name << "-rebalance: prev is (" << info2.key << ","
	<< info2.name << ")" << std::endl;
    if (o1 == info2.name) {
      only = true;
    } else {
      return rebalance(info2.name, info2.key, outfo, true);
    }
  }
  cout << "\t\t" << client_name << "-rebalance: o1 is " << o1 << " , o2 is "
      << info2.name << std::endl;

  //read o1
  err = read_object(info1.name, &info1);
  if (err < 0 ||
      string(info1.unwritable.c_str(), info1.unwritable.length()) == "1") {
    if (err == 0) {
      cout << "\t\t" << client_name << "-rebalance: " << o1
	  << " is unwritable" << std::endl;
      return -ECANCELED;
    } else {
      cerr << "\t\t" << client_name << "-rebalance: error " << err
	  << " reading " << o1 << std::endl;
      return err;
    }
  }
  cout << "\t\t" << client_name << "-rebalance: read " << o1 << ". size: "
      << info1.size << " version: " << info1.version
      << std::endl;

  if (only) {
    *outfo = info1;
    cout << "\t\t" << client_name << "-rebalance: this is the only node, "
	  << "so aborting" << std::endl;
    return -1;
  }

  //read o2
  err = read_object(info2.name, &info2);
  if (err < 0 ||
      string(info2.unwritable.c_str(), info2.unwritable.length()) == "1") {
    if (err == 0 || err == -ENOENT) {
      cerr << "\t\t" << client_name << "-rebalance: error " << err
	  << " reading " << info2.name << std::endl;
      return -ECANCELED;
    } else {
      cerr << "rebalance found an unexpected error reading"
	  " " << info2.name << "-rebalance: " << err << std::endl;
      return err;
    }
  }
  cout << "\t\t" << client_name << "-rebalance: read " << info2.name
      << ". size: " << info2.size << " version: " << info2.version
      << std::endl;

  if (reverse) {
    *outfo = info2;
  } else {
    *outfo = info1;
  }

  //calculations
  if (info1.size >= k && info1.size <= 2*k && info2.size >= k
      && info2.size <= 2*k) {
    //nothing to do
    cout << "\t\t" << client_name << "-rebalance: both sizes in range, so"
	<< " aborting with ver " << outfo->version << std::endl;
    return -1;
  }
  //this is the high object. it gets created regardless of rebalance or merge.
  string o2w = to_string(client_name, client_index++);
  index_data idata;
  std::map<std::string, std::string> to_create;
  vector<object_info*> to_delete;
  vector<map<std::string, bufferlist> > write_maps;
  librados::ObjectWriteOperation create[2];//possibly only 1 will be used
  librados::ObjectWriteOperation other_ops[6];
  vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > ops;
  ops.push_back(make_pair(
      pair<int, string>(ADD_PREFIX, index_name),
      &other_ops[0]));

  if (info1.size + info2.size <= 2*k) {
    //merge
    cout << "\t\t" << client_name << "-rebalance: merging " << o1
	<< " and " << info2.name << " to get " << o2w
	<< std::endl;
    map<string, bufferlist> write2_map;
    write2_map.insert(info1.omap.begin(), info1.omap.end());
    write2_map.insert(info2.omap.begin(), info2.omap.end());
    write_maps.push_back(write2_map);
    to_create[info2.key] = o2w;
    ops.push_back(make_pair(
	pair<int, string>(MAKE_OBJECT, o2w),
	&create[0]));
  } else {
    //rebalance
    cout << "\t\t" << client_name << "-rebalance: rebalancing" << o1
	<< " and " << info2.name << std::endl;
    map<std::string, bufferlist> write1_map;
    map<std::string, bufferlist> write2_map;
    map<std::string, bufferlist>::iterator it;
    string o1w = to_string(client_name, client_index++);
    for (it = info1.omap.begin();
	it != info1.omap.end() && (int)write1_map.size()
	    <= (info1.size + info2.size) / 2;
	++it) {
      write1_map.insert(*it);
    }
    if (it != info1.omap.end()){
      //write1_map is full, so put the rest in write2_map
      write2_map.insert(it, info1.omap.end());
      write2_map.insert(info2.omap.begin(), info2.omap.end());
    } else {
      //info1.omap was small, and write2_map still needs more
      map<std::string, bufferlist>::iterator it2;
      for(it2 = info2.omap.begin();
	  (it2 != info2.omap.end()) && ((int)write1_map.size()
	      <= (info1.size + info2.size) / 2);
	  ++it2) {
	write1_map.insert(*it2);
      }
      write2_map.insert(it2, info2.omap.end());
    }
    string hk1w = "0" + write1_map.rbegin()->first;
    //at this point, write1_map and write2_map should have the correct pairs
    to_create[hk1w] = o1w;
    to_create[info2.key] = o2w;
    write_maps.push_back(write1_map);
    write_maps.push_back(write2_map);
    ops.push_back(make_pair(
	pair<int, string>(MAKE_OBJECT, o1w),
	&create[0]));
    ops.push_back(make_pair(
	pair<int, string>(MAKE_OBJECT, o2w),
	&create[1]));
  }

  to_delete.push_back(&info1);
  to_delete.push_back(&info2);
  for (int i = 1; i < 6; i++) {
    ops.push_back(make_pair(make_pair(0,""), &other_ops[i]));
  }
  
  set_up_prefix_index(to_create, to_delete, &other_ops[0], &idata, &err);
  set_up_ops(write_maps, to_delete, &ops, idata, &err);

  //at this point, all operations should be completely set up.
  /////BEGIN CRITICAL SECTION/////
  err = perform_ops("\t\t" + client_name + "-rebalance:", idata, &ops);
  if (err < 0) {
    return err;
  }
  cout << "\t\t" << client_name << "-rebalance: done rebalancing." << std::endl;
  /////END CRITICAL SECTION/////
  return err;
}

int KvFlatBtreeAsync::read_object(const string &obj, object_info * info) {
  librados::ObjectReadOperation get_obj;
  librados::AioCompletion * obj_aioc = rados.aio_create_completion();
  int err;
  info->name = obj;
  get_obj.omap_get_vals("", LONG_MAX, &info->omap, &err);
  get_obj.getxattr("unwritable", &info->unwritable, &err);
  err = io_ctx.aio_operate(obj, obj_aioc, &get_obj, NULL);
  obj_aioc->wait_for_safe();
  err = obj_aioc->get_return_value();
  if (err < 0){
    //possibly -ENOENT, meaning someone else deleted it.
    return err;
  }
  info->version = obj_aioc->get_version();
  info->size = info->omap.size();
  return 0;
}

void KvFlatBtreeAsync::set_up_prefix_index(
    const map<string, string> &to_create,
    const vector<object_info*> &to_delete,
    librados::ObjectWriteOperation * owo,
    index_data * idata,
    int * err) {
  std::map<std::string, pair<bufferlist, int> > assertions;
  map<string, bufferlist> to_insert;
//  stringstream strm;
//  stringstream clk;
  idata->prefix = "1";
  idata->ts = ceph_clock_now(g_ceph_context);
//  clk << now.sec() << '.' << now.usec();
//  strm << "1" << to_string_f(clk.str());
  for(map<string, string>::const_iterator it = to_create.begin();
      it != to_create.end();
      ++it) {
    vector<string> c;
    c.push_back(it->first);
    c.push_back(it->second);
    idata->to_create.push_back(c);
//    strm << pair_init << to_string_f(it->first) << sub_separator
//	<< to_string_f(it->second) << pair_end;
  }
  //strm << sub_terminator;
  for(vector<object_info*>::const_iterator it = to_delete.begin();
      it != to_delete.end();
      ++it) {
    vector<string> d;
    d.push_back((*it)->key);
    d.push_back((*it)->name);
    d.push_back(to_string("", (*it)->version));
    idata->to_delete.push_back(d);
//    strm << pair_init << to_string_f((*it)->key) << sub_separator
//	  << to_string_f((*it)->name)  << sub_separator
//	  << (*it)->version << pair_end;
  }
  //strm << terminator;
  for(vector<object_info*>::const_iterator it = to_delete.begin();
      it != to_delete.end();
      ++it) {
    idata->val = (*it)->name;
    bufferlist insert;
    idata->encode(insert);
    //to_insert[(*it)->key] = to_bl(strm.str() + (*it)->name);
    to_insert[(*it)->key] = insert;
    index_data this_entry;
    this_entry.val = (*it)->name;
    assertions[(*it)->key] = pair<bufferlist, int>(to_bl(this_entry),
	CEPH_OSD_CMPXATTR_OP_EQ);
    cout << "\t\t\t" << client_name << "-setup_prefix: will assert ("
	<< (*it)->key << "," /*<< string(assertions[(*it)->key].first.c_str(),
	    assertions[(*it)->key].first.length()) << ")" << std::endl;*/
	<< this_entry.str() << ")" << std::endl;
  }
  //bufferlist index_bl = to_bl(strm.str());
  //*err = parse_prefix(&index_bl, p);
  assert(*err == 0);
  owo->omap_cmp(assertions, err);
  owo->omap_set(to_insert);
}

//some args can be null if there are no corresponding entries in p
void KvFlatBtreeAsync::set_up_ops(
    const vector<map<std::string, bufferlist> > &create_maps,
    const vector<object_info*> &delete_infos,
    vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > * ops,
    const index_data &idata,
    int * err) {
  vector<pair<pair<int, string>,
    librados::ObjectWriteOperation* > >::iterator it;
  for(it = ops->begin();
      it->first.first == ADD_PREFIX; it++){
  }
  map<string, bufferlist> to_insert;
  std::set<string> to_remove;
  map<string, pair<bufferlist, int> > assertions;
  for (int i = 0; i < (int)idata.to_create.size(); ++i) {
    index_data this_entry;
    this_entry.val = idata.to_create[i][1];
    to_insert[idata.to_create[i][0]] = to_bl(this_entry);
    if (create_maps.size() > 0) {
      it->first = pair<int, string>(MAKE_OBJECT, idata.to_create[i][1]);
      set_up_make_object(create_maps[i], it->second);
      it++;
    }
  }
  if (delete_infos.size() > 0) {
    for (int i = 0; i < (int)idata.to_delete.size(); ++i) {
      it->first = pair<int, string>(UNWRITE_OBJECT, idata.to_delete[i][1]);
      set_up_unwrite_object(delete_infos[i]->version, it->second);
      it++;
    }
  }
  for (int i = 0; i < (int)idata.to_delete.size(); ++i) {
    index_data this_entry = idata;
    this_entry.val = idata.to_delete[i][1];
    assertions[idata.to_delete[i][0]] = pair<bufferlist, int>(
	to_bl(this_entry), CEPH_OSD_CMPXATTR_OP_EQ);
    to_remove.insert(idata.to_delete[i][0]);
    it->first = pair<int, string>(REMOVE_OBJECT, idata.to_delete[i][1]);
    set_up_delete_object(it->second);
    it++;
  }
  it->second->omap_cmp(assertions, err);
  it->second->omap_rm_keys(to_remove);
  it->second->omap_set(to_insert);

  it->first = pair<int, string>(REMOVE_PREFIX, index_name);
}

void KvFlatBtreeAsync::set_up_make_object(
    const map<std::string, bufferlist> &to_set,
    librados::ObjectWriteOperation *owo) {
  owo->create(true);
  owo->omap_set(to_set);
  owo->setxattr("unwritable", to_bl("0"));
  cout << client_name << "-owo: " << owo << std::endl;
}

void KvFlatBtreeAsync::set_up_unwrite_object(
    const int &ver, librados::ObjectWriteOperation *owo) {
  if (ver > 0) {
    cout << client_name << ": version "  << ver << " > 0, so assert" << std::endl;
    owo->assert_version(ver);
  }
  owo->cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  owo->setxattr("unwritable", to_bl("1"));
}

void KvFlatBtreeAsync::set_up_restore_object(
    librados::ObjectWriteOperation *owo) {
  owo->cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
  owo->setxattr("unwritable", to_bl("0"));
}

void KvFlatBtreeAsync::set_up_delete_object(
    librados::ObjectWriteOperation *owo) {
  owo->cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
  owo->remove();
}

int KvFlatBtreeAsync::perform_ops(const string &debug_prefix,
    const index_data &idata,
    vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > *ops) {
  int err = 0;
  for (vector<pair<pair<int, string>,
      librados::ObjectWriteOperation*> >::iterator it = ops->begin();
      it != ops->end(); ++it) {
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    switch (it->first.first) {
    case ADD_PREFIX://prefixing
      cout << debug_prefix << " adding prefix" << std::endl;
      err = io_ctx.operate(index_name, it->second);
      if (err < 0) {
        cerr << debug_prefix << " prefixing the index failed with "
            << err << std::endl;
        return err;
      }
      cout << debug_prefix << " prefix added." << std::endl;
      break;
    case MAKE_OBJECT://creating
      cout << debug_prefix << " creating " << it->first.second
	<< ", address = " << it->second << std::endl;
      err = io_ctx.operate(it->first.second, it->second);
      if (err < 0) {
	//this can happen if someone else was cleaning up after us.
	cerr << debug_prefix << " creating " << it->first.second << " failed"
	    << " with code " << err << std::endl;
	if (err == -EEXIST) {
	  //someone thinks we died, so die
	  assert(false);
	  cerr << client_name << " is suiciding!" << std::endl;
	  return -ESUICIDE;
	} else {
	  assert(false);
	}
	return err;
      }
      cout << debug_prefix << " created " << it->first.second << std::endl;
      break;
    case UNWRITE_OBJECT://marking
      cout << debug_prefix << " marking " << it->first.second << std::endl;
      err = io_ctx.operate(it->first.second, it->second);
      if (err < 0) {
	//most likely because it changed, in which case it will be -ERANGE
	cerr << debug_prefix << " marking " << it->first.second
	    << "failed with code" << err << std::endl;
	cleanup(idata, -ETIMEDOUT);
	return -ECANCELED;
      }
      cout << debug_prefix << " marked " << it->first.second << std::endl;
      break;
    case RESTORE_OBJECT:
      cout << debug_prefix << " retsoring " << it->first.second << std::endl;
      err = io_ctx.operate(it->first.second, it->second);
      if (err < 0) {
	cerr << debug_prefix << "restoring " << it->first.second << " failed"
	    << " with " << err << std::endl;
	return err;
      }
      cout << debug_prefix << " restored " << it->first.second << std::endl;
      break;
    case REMOVE_OBJECT://deleting
      cout << debug_prefix << " deleting " << it->first.second << std::endl;
      err = io_ctx.operate(it->first.second, it->second);
      if (err < 0) {
	//if someone else called cleanup on this prefix first
	cerr << debug_prefix << " deleting " << it->first.second
	    << "failed with code" << err << std::endl;
      }
      cout << debug_prefix << " deleted " << it->first.second << std::endl;
      break;
    case REMOVE_PREFIX://rewriting index
      cout << debug_prefix << " updating index " << std::endl;
      err = io_ctx.operate(index_name, it->second);
      if (err < 0) {
        cerr << debug_prefix
    	<< " rewriting the index failed with code " << err
        << ". someone else must have thought we died, so dying" << std::endl;
        return -ESUICIDE;
      }
      cout << debug_prefix << " updated index." << std::endl;
      break;
    default:
      cout << debug_prefix << " performing unknown op on " << it->first.second
	<< std::endl;
      err = io_ctx.operate(index_name, it->second);
      if (err < 0) {
	cerr << debug_prefix << " unknown op on " << it->first.second
	    << " failed with " << err << std::endl;
	return err;
      }
      cout << debug_prefix << " unknown op on " << it->first.second
	  << " succeeded." << std::endl;
      break;
    }
  }
  return err;
}

int KvFlatBtreeAsync::cleanup(const index_data &idata, const int &errno) {
  cout << "\t\t" << client_name << ": cleaning up after " << idata.str()
      << std::endl;
  int err = 0;
  assert(idata.prefix != "");
  map<std::string,bufferlist> new_index;
  map<std::string, pair<bufferlist, int> > assertions;
  switch (errno) {
  case -ENOENT: {
    cout << "\t\t" << client_name << "-cleanup: rolling forward" << std::endl;
    //all changes were created except for updating the index and possibly
    //deleting the objects. roll forward.
    vector<pair<pair<int, string>, librados::ObjectWriteOperation*> > ops;
    librados::ObjectWriteOperation owos[idata.to_delete.size() + 1];
    for (int i = 0; i <= (int)idata.to_delete.size(); ++i) {
      ops.push_back(make_pair(pair<int, string>(0, ""), &owos[i]));
    }
    set_up_ops(vector<map<std::string, bufferlist> >(),
	vector<object_info*>(), &ops, idata, &err);
    err = perform_ops("\t\t" + client_name + "-cleanup:", idata, &ops);
    if (err < 0) {
      cerr << "\t\t\t" << client_name << "-cleanup: rewriting failed with "
	  << err << ". returning -ECANCELED" << std::endl;
      return -ECANCELED;
    }
    cout << "\t\t\t" << client_name << "-cleanup: updated index" << std::endl;
    break;
  }
  default: {
    //roll back all changes.
    cout << "\t\t" << client_name << "-cleanup: rolling back" << std::endl;
    map<std::string,bufferlist> new_index;
    std::set<string> to_remove;
    map<std::string, pair<bufferlist, int> > assertions;
    //mark the objects to be created. if someone else already has, die.
    for(vector<vector<string> >::const_reverse_iterator it =
	idata.to_create.rbegin();
	it != idata.to_create.rend(); ++it) {
      librados::ObjectWriteOperation rm;
      set_up_unwrite_object(0, &rm);
      if (interrupt() == 1 ) {
	return -ESUICIDE;
      }
      cout << "\t\t\t" << client_name << "-cleanup: marking " << (*it)[1]
	<< std::endl;
      err = io_ctx.operate((*it)[1], &rm);
      if (err < 0) {
	cerr << "\t\t\t" << client_name << "-cleanup: marking " << (*it)[1]
            << " failed with " << err << std::endl;
      }
    }
    //restore objects that had been marked unwritable.
    for(vector<vector<string> >::const_iterator it =
	idata.to_delete.begin();
	it != idata.to_delete.end(); ++it) {
      index_data this_entry;
      this_entry.val = (*it)[1];
      new_index[(*it)[0]] = to_bl(this_entry);
      this_entry = idata;
      this_entry.val = (*it)[1];
      assertions[(*it)[0]] =
	  pair<bufferlist, int>(to_bl(this_entry),
	      CEPH_OSD_CMPXATTR_OP_EQ);
      librados::ObjectWriteOperation restore;
      set_up_restore_object(&restore);
      cout << "\t\t\t" << client_name << "-cleanup: will assert index contains "
	  << "(" << (*it)[0] << "," << idata.prefix << (*it)[1]
	       << ")" << std::endl;
      if (interrupt() == 1 ) {
	return -ESUICIDE;
      }
      cout << "\t\t\t" << client_name << "-cleanup: restoring " << (*it)[1]
          << std::endl;
      err = io_ctx.operate((*it)[1], &restore);
      if (err == -ENOENT) {
	//it had gotten far enough to be rolled forward - unmark the objects
	//and roll forward.
	for(vector<vector<string> >::const_iterator cit =
	    idata.to_create.begin();
	    cit != idata.to_create.end(); ++cit) {
	  librados::ObjectWriteOperation res;
	  set_up_restore_object(&res);
	  if (interrupt() == 1 ) {
	    return -ECANCELED;
	  }
	  cout << "\t\t\t" << client_name << "-cleanup: marking " << (*cit)[1]
	    << std::endl;
	  err = io_ctx.operate((*cit)[1], &res);
	  if (err < 0) {
	    cerr << "\t\t\t" << client_name << "-cleanup: marking " << (*cit)[1]
		<< " failed with " << err << std::endl;
	  }
	}
	return cleanup(idata, -ENOENT);
      }
      cout << "\t\t\t" << client_name << "-cleanup: restored " << (*it)[1]
          << std::endl;
    }
    for(vector<vector<string> >::const_reverse_iterator it =
	idata.to_create.rbegin();
	it != idata.to_create.rend(); ++it) {
      to_remove.insert((*it)[0]);
      librados::ObjectWriteOperation rm;
      rm.remove();
      cout << "\t\t\t" << client_name << "-cleanup: removing " << (*it)[1]
          << std::endl;
      if (interrupt() == 1 ) {
	return -ESUICIDE;
      }
      io_ctx.operate((*it)[1], &rm);
    }
    librados::ObjectWriteOperation update_index;
    update_index.omap_cmp(assertions, &err);
    update_index.omap_rm_keys(to_remove);
    update_index.omap_set(new_index);
    cout << "\t\t\t" << client_name << "-cleanup: updating index" << std::endl;
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    err = io_ctx.operate(index_name, &update_index);
    if (err < 0) {
      cerr << "\t\t\t" << client_name << "-cleanup: rewriting failed with "
	  << err << ". returning -ECANCELED" << std::endl;
      return -ECANCELED;
    }
    cout << "\t\t\t" << client_name << "-cleanup: updated index. cleanup done."
	<< std::endl;
    break;
  }
  }
  return err;
}

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
bufferlist KvFlatBtreeAsync::to_bl(const index_data &idata) {
  bufferlist bl;
  idata.encode(bl);
  return bl;
}

string KvFlatBtreeAsync::to_string_f(string s) {
  stringstream ret;
  for (int i = 0; i < (int)s.length(); i++) {
    if (s[i] == pair_init
	|| s[i] == sub_separator
	|| s[i] ==  pair_end
	|| s[i] == sub_terminator
	|| s[i] == terminator) {
      ret << "\\" << s[i];
    } else {
      ret << s[i];
    }
  }
  return ret.str();
}

string KvFlatBtreeAsync::get_name() {
  return rados_id;
}

void KvFlatBtreeAsync::set_waits(const vector<__useconds_t> &wait) {
  waits = wait;
  wait_index = 0;
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
    cout << "error during connect: " << r << std::endl;
    return r;
  }
  r = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (r < 0) {
    cout << "error creating io ctx" << std::endl;
    rados.shutdown();
    return r;
  }

  librados::ObjectWriteOperation make_index;
  make_index.create(true);
  map<std::string,bufferlist> index_map;
  index_data idata;
  idata.val = client_name;
  index_map["1"] = to_bl(idata);
  make_index.omap_set(index_map);
  r = io_ctx.operate(index_name, &make_index);
  if (r < 0) {
    cerr << client_name << ": Making the index failed with code " << r
	<< std::endl;
    return r;
  }

  librados::ObjectWriteOperation make_max_obj;
  make_max_obj.create(true);
  make_max_obj.setxattr("unwritable", to_bl("0"));
  r = io_ctx.operate(client_name, &make_max_obj);

  return r;

}

int KvFlatBtreeAsync::set(const string &key, const bufferlist &val,
    bool update_on_existing) {
  cout << client_name << " is setting " << key << std::endl;
  int err = 0;
  bufferlist objb;
  string obj;
  string hk;
  utime_t mytime;
  index_data idata;
  object_info o;

  while (err != -1) {
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t" << client_name << ": finding oid" << std::endl;
    err = oid("0"+key, &objb, &hk);
    mytime = ceph_clock_now(g_ceph_context);
    if (err < 0) {
      cerr << "\t" << client_name << ": getting oid failed with code "
	  << err << std::endl;
      return err;
    }
    idata.clear();
    err = parse_prefix(&objb, &idata);
    if (err < 0) {
      return err;
    }
    cout << "\t" << client_name << ": raw oid is " << idata.str() << std::endl;
    obj = idata.val;

    cout << "\t" << client_name << ": obj is " << obj << std::endl;
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t" << client_name << ": running split on " << obj << std::endl;
    err = split(obj, hk, &o);
    if (err < 0) {
      cerr << "\t" << client_name << ": split returned " << err << std::endl;
      if (idata.prefix == "" && err == -EPREFIX) {
        oid("0"+key, &objb, &hk);
        parse_prefix(&objb, &idata);
        mytime = ceph_clock_now(g_ceph_context);
      }
      if (idata.prefix != "" && mytime - idata.ts > TIMEOUT) {
        cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
        //the client died after deleting the object. clean up.
        cleanup(idata, err);
      } else if (idata.prefix != "") {
        cout << "\t" << client_name << ": prefix and not timed out, "
  	  << "so restarting ( it has been " << (mytime - idata.ts).sec()
  	  << '.' << (mytime - idata.ts).usec()
  	  << ", timeout is " << TIMEOUT << ")" << std::endl;
      } else if (err != -1 && err != -ECANCELED && err != -ENOENT
	  && err != -EPREFIX){
	cerr << "\t" << client_name
	    << ": split encountered an unexpected error: " << err << std::endl;
	return err;
      }
      if (err != -1 && wait_index >= 3) {
	wait_index -= 3;
      }
    }
    if (!update_on_existing && o.omap.count(key)){
      cout << "\t" << client_name << ": key exists, so returning " << std::endl;
      return -EEXIST;
    }
  }

  cout << "\t" << client_name << ": got our object: " << obj << std::endl;

  //write
  librados::ObjectWriteOperation owo;
  librados::AioCompletion * write_aioc = rados.aio_create_completion();
  owo.assert_version(o.version);
  map<std::string, pair<bufferlist, int> > assertions;
  map<std::string,bufferlist> to_insert;
  to_insert[key] = val;
  owo.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  owo.omap_set(to_insert);
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  err = io_ctx.aio_operate(obj, write_aioc, &owo);
  cout << "\t" << client_name << ": inserting " << key << " with value "
      << string(to_insert[key].c_str(),
      to_insert[key].length()) << " into object " << obj
      << " with version " << o.version << std::endl;
  if (err < 0) {
    cerr << "performing insertion failed with code " << err;
    cout << std::endl;
  }
  write_aioc->wait_for_safe();
  err = write_aioc->get_return_value();
  cerr << "\t" << client_name << ": write finished with " << err << std::endl;

  if (idata.prefix != "" && mytime - idata.ts >= TIMEOUT) {
    //client died before objects were deleted
    cleanup(idata,-ETIMEDOUT);
  }

  if (err < 0) {
    cerr << "\t" << client_name << ": writing obj failed. "
	<< "probably failed assert. " << err << std::endl;
    return set(key, val, update_on_existing);
  }
  //err = rebalance(obj, hk, &o, false);
  if (err == -ESUICIDE) {
    return err;
  }
  if (idata.prefix != "" && mytime - idata.ts > TIMEOUT) {
    cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
    //the client died after deleting the object. clean up.
    cleanup(idata, err);
  }
  err = 0;
  cout << "\t" << client_name << ": finished set and exiting" << std::endl;
  return err;
}

int KvFlatBtreeAsync::remove(const string &key) {
  cout << client_name << ": removing " << key << std::endl;
  int err = 0;
  bufferlist objb;
  string obj;
  string hk;
  utime_t mytime;
  object_info info;
  index_data idata;

  //while (err != -1) {
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t" << client_name << ": finding oid" << std::endl;
    err = oid("0"+key, &objb, &hk);
    mytime = ceph_clock_now(g_ceph_context);
    if (err < 0) {
      cerr << "getting oid failed with code " << err << std::endl;
      return err;
    }
    err = parse_prefix(&objb, &idata);
    if (err < 0) {
      cerr << "\t" << client_name << ": parsing prefix failed - bad prefix. "
	  << err <<  std::endl;
      return err;
    }
    obj = idata.val;
    cout << "\t" << client_name << ": raw oid is " << idata.str() << std::endl;

    cout << "\t" << client_name << ": obj is " << obj << std::endl;
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t" << client_name << ": rebalancing " << obj << std::endl;
    //err = rebalance(obj, hk, &info, false);
    err = read_object(obj, &info);
    if (err < 0) {
      cerr << "\t" << client_name << ": read object returned " << err
	  << std::endl;
      if (err == -ENOENT) {
	if (idata.prefix != "" && mytime - idata.ts <= TIMEOUT) {
	  return remove(key);
	} else if (idata.prefix != "") {
	  //the client died after deleting the object. clean up.
	  cout << "\t" << client_name << ": need to cleanup" << std::endl;
	  cleanup(idata, err);
	}
      } else {
	cerr << "read object encountered an unexpected error: "
	    << err << std::endl;
	return err;
      }
      if (err != -1 && wait_index > 3) {
	wait_index -= 3;
      }
    }
  //}

  //write
  librados::ObjectWriteOperation owo;
  librados::AioCompletion * write_aioc = rados.aio_create_completion();
  owo.assert_version(info.version);
  owo.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  std::set<std::string> to_rm;
  to_rm.insert(key);
  owo.omap_rm_keys(to_rm);
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  cout << "\t" << client_name << ": removing from " << obj
      << ". asserting version " << info.version << std::endl;
  err = io_ctx.aio_operate(obj, write_aioc, &owo);
  write_aioc->wait_for_safe();
  cout << "\t" << client_name << ": remove finished."
        << std::endl;
  err = write_aioc->get_return_value();
  if (err < 0) {
    cout << client_name << "-remove: writing failed - probably failed assert. "
	<< err << std::endl;
    return remove(key);
  }
  if (idata.prefix != "" && mytime - idata.ts > TIMEOUT) {
    cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
    //the client died after deleting the object. clean up.
    cleanup(idata, err);
  }
  do {
    err = rebalance(obj, hk, &info, false);
    cerr << "\t" << client_name << ": rebalance after remove got " << err
	<< ". prefix is " << idata.str() << std::endl;
    if (err == -ESUICIDE) {
      return err;
    } else if (idata.prefix != "") {
      mytime = ceph_clock_now(g_ceph_context);
    } else if (idata.prefix == "" && err == -EPREFIX) {
      oid("0"+key, &objb, &hk);
      parse_prefix(&objb, &idata);
      mytime = ceph_clock_now(g_ceph_context);
    }
    if (idata.prefix != "" && mytime - idata.ts > TIMEOUT) {
      cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
      //the client died after deleting the object. clean up.
      cleanup(idata, err);
      oid("0"+key, &objb, &hk);
      parse_prefix(&objb, &idata);
      mytime = ceph_clock_now(g_ceph_context);
    } else if (idata.prefix != "") {
      cout << "\t" << client_name << ": prefix and not timed out, "
	  << "so restarting ( it has been " << (mytime - idata.ts).sec()
	  << '.' << (mytime - idata.ts).usec()
	  << ", timeout is " << TIMEOUT <<")" << std::endl;
    }
  } while (!(err == -1 || err == -ENOENT || err == 0));
  return err;
}

int KvFlatBtreeAsync::get(const string &key, bufferlist *val) {
  cout << client_name << ": getting " << key << std::endl;
  int err = 0;
  std::set<std::string> key_set;
  key_set.insert(key);
  map<std::string,bufferlist> omap;
  bufferlist objb;
  string obj;
  string hk;
  utime_t mytime;

  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  err = oid("0"+key, &objb, &hk);
  mytime = ceph_clock_now(g_ceph_context);
  if (err < 0) {
    cerr << "getting oid failed with code " << err;
    cout << std::endl;
    return err;
  }
  index_data idata;
  err = parse_prefix(&objb, &idata);
  if (err < 0) {
    cout << "\t" << client_name << "parsing prefix failed - bad prefix. "
	<< err << std::endl;
    return err;
  }
  obj = idata.val;
  if (err < 0) {
    cerr << "getting oid failed with code " << err;
    cout << std::endl;
    return err;
  }

  librados::ObjectReadOperation read;
  read.omap_get_vals_by_keys(key_set, &omap, &err);
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  err = io_ctx.operate(obj, &read, NULL);
  if (err < 0 && err != -1 && err != -ECANCELED) {
    if (err == -ENODATA) {
      if (mytime - idata.ts <= TIMEOUT) {
	return get(key, val);
      } else {
	//the client died after deleting the object. clean up.
       cleanup(idata, err);
      }
    } else {
      cerr << "split encountered an unexpected error: " << err << std::endl;
      return err;
    }
  }

  *val = omap[key];

  return err;
}

int KvFlatBtreeAsync::remove_all() {
  cout << client_name << ": removing all" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  librados::AioCompletion * oro_aioc = rados.aio_create_completion();
  std::map<std::string, bufferlist> index_set;
  oro.omap_get_vals("",LONG_MAX,&index_set,&err);
  err = io_ctx.aio_operate(index_name, oro_aioc, &oro, NULL);
  if (err < 0){
    if (err == -ENOENT) {
      return 0;
    }
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  oro_aioc->wait_for_safe();

  librados::ObjectWriteOperation rm_index;
  librados::AioCompletion * rm_index_aioc  = rados.aio_create_completion();
  map<std::string,bufferlist> new_index;
  new_index["1"] = index_set["1"];
  rm_index.omap_clear();
  rm_index.omap_set(new_index);
  io_ctx.aio_operate(index_name, rm_index_aioc, &rm_index);
  err = rm_index_aioc->get_return_value();
  if (err < 0) {
    cerr << "rm index aioc failed - probably failed assertion. " << err;
    cerr << std::endl;
    return remove_all();
  }

  if (index_set.size() != 0) {
    for (std::map<std::string,bufferlist>::iterator it = index_set.begin();
        it != index_set.end(); ++it){
      librados::ObjectWriteOperation sub;
      if (it->first == "1") {
	sub.omap_clear();
      } else {
	sub.remove();
      }
      index_data idata;
      err = parse_prefix(&it->second, &idata);
      if (err < 0) {
	cout << "error parsing prefix" << std::endl;
	return err;
      }
      io_ctx.operate(idata.val, &sub);
    }
  }
  return err;
}

int KvFlatBtreeAsync::get_all_keys(std::set<std::string> *keys) {
  cout << client_name << ": getting all keys" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<std::string,bufferlist> index_set;
  oro.omap_get_vals("",LONG_MAX,&index_set,&err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  for (std::map<std::string,bufferlist>::iterator it = index_set.begin();
      it != index_set.end(); ++it){
    librados::ObjectReadOperation sub;
    std::set<std::string> ret;
    sub.omap_get_keys("",LONG_MAX,&ret,&err);
    index_data idata;
    err = parse_prefix(&it->second, &idata);
    if (err < 0) {
      cout << "error parsing prefix" << std::endl;
      return err;
    }
    io_ctx.operate(idata.val, &sub, NULL);
    keys->insert(ret.begin(), ret.end());
  }
  return err;
}

int KvFlatBtreeAsync::get_all_keys_and_values(
    map<std::string,bufferlist> *kv_map) {
  cout << client_name << ": getting all keys and values" << std::endl;
  int err = 0;
  librados::ObjectReadOperation first_read;
  std::set<std::string> index_set;
  first_read.omap_get_keys("",LONG_MAX,&index_set,&err);
  io_ctx.operate(index_name, &first_read, NULL);
  if (err < 0){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  for (std::set<std::string>::iterator it = index_set.begin();
      it != index_set.end(); ++it){
    librados::ObjectReadOperation sub;
    map<std::string, bufferlist> ret;
    sub.omap_get_vals("",LONG_MAX,&ret,&err);
    io_ctx.operate(*it, &sub, NULL);
    kv_map->insert(ret.begin(), ret.end());
  }
  return err;
}

bool KvFlatBtreeAsync::is_consistent() {
  int err;
  bool ret = true;
  cout << client_name << ": checking consistency" << std::endl;
  std::map<std::string,bufferlist> index;
  map<std::string, std::set<std::string> > sub_objs;
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

  std::map<std::string, string> parsed_index;
  std::set<std::string> onames;
  for (map<std::string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it) {
    if (it->first != "") {
      index_data idata;
      err = parse_prefix(&it->second, &idata);
      if (err < 0) {
        cout << "Not consistent! Bad prefix on obj for key " << it->first
            << std::endl;
	ret = false;
      }
      if (idata.prefix != "") {
	for(vector<vector<string> >::iterator dit = idata.to_delete.begin();
	    dit != idata.to_delete.end(); dit++) {
	  librados::ObjectReadOperation oro;
	  librados::AioCompletion * aioc = rados.aio_create_completion();
	  bufferlist un;
	  oro.getxattr("unwritable", &un, &err);
	  err = io_ctx.aio_operate((*dit)[1], aioc, &oro, NULL);
	  aioc->wait_for_safe();
	  err = aioc->get_return_value();
	  if (ceph_clock_now(g_ceph_context) - idata.ts > TIMEOUT) {
	    if (err < 0) {
	      if (err == -ENOENT) {
		continue;
	      } else {
		cerr << "Not consistent! reading object " << (*dit)[1]
		<< "returned " << err << std::endl;
		ret = false;
		break;
	      }
	    }
	    if (string(un.c_str(), un.length()) != "1" &&
		aioc->get_version() != atoi((*dit)[2].c_str())) {
	      cout << "Not consistent! object " << (*dit)[1] << " has been "
		  << " modified since the client died was not cleaned up."
		  << std::endl;
	      ret = false;
	    }
	  }
	  onames.insert((*dit)[1]);
	}
	for(vector<vector<string> >::iterator cit = idata.to_create.begin();
	    cit != idata.to_create.end(); cit++) {
	  onames.insert((*cit)[1]);
	}
      }
      parsed_index.insert(make_pair(it->first, idata.val));
      onames.insert(idata.val);
    }
  }

  //make sure that an object exists iff it either is the index
  //or is listed in the index
  for (librados::ObjectIterator oit = io_ctx.objects_begin();
      oit != io_ctx.objects_end(); ++oit) {
    string name = oit->first;
    if (name != index_name && onames.count(name) == 0) {
      cout << "Not consistent! found floating object " << name << std::endl;
      ret = false;
    }
  }

  //check objects
  string prev = "";
  for (std::map<std::string, string>::iterator it = parsed_index.begin();
      it != parsed_index.end();
      ++it) {
    librados::ObjectReadOperation read;
    read.omap_get_keys("", LONG_MAX, &sub_objs[it->second], &err);
    io_ctx.operate(it->second, &read, NULL);
    int size_int = (int)sub_objs[it->second].size();

    //check that size is in the right range
    if (it->first != "1" &&
	(size_int > 2*k || size_int < k) && parsed_index.size() > 1) {
      cout << "Not consistent! Object " << *it << " has size " << size_int
	  << ", which is outside the acceptable range." << std::endl;
      ret = false;
    }

    //check that all keys belong in that object
    for(std::set<std::string>::iterator subit = sub_objs[it->second].begin();
	subit != sub_objs[it->second].end(); ++subit) {
      if ((it->first != "1"
	  && *subit > it->first.substr(1,it->first.length()))
	  || *subit <= prev) {
	cout << "Not consistent! key " << *subit << " does not belong in "
	    << *it << std::endl;
	cout << "not last element, i.e. " << it->first << " not equal to 1? "
	    << (it->first != "1") << std::endl
	    << "greater than " << it->first.substr(1,it->first.length())
	    <<"? " << (*subit > it->first.substr(1,it->first.length()))
	    << std::endl
	    << "less than or equal to " << prev << "? "
	    << (*subit <= prev) << std::endl;
	ret = false;
      }
    }

    prev = it->first.substr(1,it->first.length());
  }

  if (!ret) {
    cout << str();
  }
  return ret;
}

string KvFlatBtreeAsync::str() {
  stringstream ret;
  ret << "Top-level map:" << std::endl;
  int err = 0;
  std::set<std::string> keys;
  std::map<std::string,bufferlist> index;
  librados::ObjectReadOperation oro;
  librados::AioCompletion * top_aioc = rados.aio_create_completion();
  oro.omap_get_vals("",LONG_MAX,&index,&err);
  io_ctx.aio_operate(index_name, top_aioc, &oro, NULL);
  top_aioc->wait_for_safe();
  err = top_aioc->get_return_value();
  if (err < 0 && err != -5){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return ret.str();
  }
  if(index.size() == 0) {
    ret << "There are no objects!" << std::endl;
    return ret.str();
  }

  for (map<std::string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it) {
    keys.insert(string(it->second.c_str(), it->second.length())
	.substr(1,it->second.length()));
  }

  vector<std::string> all_names;
  vector<int> all_sizes(index.size());
  vector<int> all_versions(index.size());
  vector<bufferlist> all_unwrit(index.size());
  vector<map<std::string,bufferlist> > all_maps(keys.size());
  vector<map<std::string,bufferlist>::iterator> its(keys.size());
  unsigned done = 0;
  vector<bool> dones(keys.size());
  ret << std::endl << string(150,'-') << std::endl;

  for (map<std::string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it){
    index_data idata;
    bufferlist::iterator b = it->second.begin();
    idata.decode(b);
    string s = idata.str();
    ret << "|" << string((148 -
	((*it).first.length()+s.length()+3))/2,' ');
    ret << (*it).first;
    ret << " | ";
    ret << string(idata.str());
    ret << string((148 -
	((*it).first.length()+s.length()+3))/2,' ');
    ret << "|\t";
    all_names.push_back(idata.val);
    ret << std::endl << string(150,'-') << std::endl;
  }

  int indexer = 0;

  //get the object names and sizes
  for(vector<std::string>::iterator it = all_names.begin(); it
  != all_names.end();
      ++it) {
    librados::ObjectReadOperation oro;
    librados::AioCompletion *aioc = rados.aio_create_completion();
    oro.omap_get_vals("", LONG_MAX, &all_maps[indexer], &err);
    oro.getxattr("unwritable", &all_unwrit[indexer], &err);
    io_ctx.aio_operate(*it, aioc, &oro, NULL);
    aioc->wait_for_safe();
    if (aioc->get_return_value() < 0) {
      ret << "reading" << *it << "failed: " << err << std::endl;
      //return ret.str();
    }
    all_sizes[indexer] = all_maps[indexer].size();
    all_versions[indexer] = aioc->get_version();
    indexer++;
  }

  ret << "///////////////////OBJECT NAMES////////////////" << std::endl;
  //HEADERS
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
   ret << "---------------------------\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "|" << string((25 -
	(string("Bucket: ").length() + all_names[i].length()))/2, ' ');
    ret << "Bucket: " << all_names[i];
    ret << string((25 -
    	(string("Bucket: ").length() + all_names[i].length()))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    its[i] = all_maps[i].begin();
    ret << "|" << string((25 - (string("size: ").length()
	+ to_string("",all_sizes[i]).length()))/2, ' ');
    ret << "size: " << all_sizes[i];
    ret << string((25 - (string("size: ").length()
	  + to_string("",all_sizes[i]).length()))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    its[i] = all_maps[i].begin();
    ret << "|" << string((25 - (string("version: ").length()
	+ to_string("",all_versions[i]).length()))/2, ' ');
    ret << "version: " << all_versions[i];
    ret << string((25 - (string("version: ").length()
	  + to_string("",all_versions[i]).length()))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    its[i] = all_maps[i].begin();
    ret << "|" << string((25 - (string("unwritable? ").length()
	+ 1))/2, ' ');
    ret << "unwritable? " << string(all_unwrit[i].c_str(),
	all_unwrit[i].length());
    ret << string((25 - (string("unwritable? ").length()
	  + 1))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "---------------------------\t";
  }
  ret << std::endl;
  ret << "///////////////////THE ACTUAL BLOCKS////////////////" << std::endl;


  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    ret << "---------------------------\t";
  }
  ret << std::endl;
  //each time through this part is two lines
  while(done < keys.size()) {
    for(int i = 0; i < indexer; i++) {
      if(dones[i]){
	ret << "                          \t";
      } else {
	if (its[i] == all_maps[i].end()){
	  done++;
	  dones[i] = true;
	  ret << "                          \t";
	} else {
	  ret << "|" << string((25 -
	      ((*its[i]).first.length()+its[i]->second.length()+3))/2,' ');
	  ret << (*its[i]).first;
	  ret << " | ";
	  ret << string(its[i]->second.c_str(), its[i]->second.length());
	  ret << string((25 -
	      ((*its[i]).first.length()+its[i]->second.length()+3))/2,' ');
	  ret << "|\t";
	  ++(its[i]);
	}

      }
    }
    ret << std::endl;
    for (int i = 0; i < indexer; i++) {
      if(dones[i]){
	ret << "                          \t";
      } else {
	ret << "---------------------------\t";
      }
    }
    ret << std::endl;

  }
  return ret.str();
}
