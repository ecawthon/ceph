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
  /*if (client_name != "client0." && rand() % 10 == 0) {
    cout << client_name << " is suiciding"
    // << because waits[" << wait_index - 1 << "] is " << waits[wait_index - 1]
    << std::endl;
    return 1;
  } else if (client_name == "client0.") {
    usleep(1000);
  } else if(wait_index < (int)waits.size()) {*/
  if (rand() % 10 == 0) {
    usleep(500000);
  }
  return 0;
}

int KvFlatBtreeAsync::parse_prefix(bufferlist * bl, prefix_data * ret) {
  cout << "\t\t" << client_name << ": parsing prefix from "
      << string(bl->c_str(), bl->length())
      << std::endl;
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
    ret->val = to_bl(
	string(bl->c_str(), bl->length()).substr(1,bl->length() - 1));
    return 0;
  }
  for (int i = 1; i < (int)bl->length(); i++) {
    if (val) {
      string value(s.substr(val_index, s.length() - 1));

      ret->val = to_bl(value);
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
    prefix_data p;
    err = parse_prefix(&kvs.begin()->second, &p);
    if (err < 0) {
      cout << "\t\t\t" << client_name << "-next: invalid prefix found. "
	  << err << std::endl;
      return err;
    }
    if (p.prefix != "" && ceph_clock_now(g_ceph_context) - p.ts > TIMEOUT) {
      cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
      //the client died after deleting the object. clean up.
      cleanup(p, err);
    }
    *ret = string(p.val.c_str(), p.val.length());
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
  prefix_data p;
  err = parse_prefix(&it->second, &p);
  if (err < 0) {
    cout << "\t\t\t" << client_name << "-next: invalid prefix found. " << err
	<< std::endl;
    return err;
  }
  if (p.prefix != "" && ceph_clock_now(g_ceph_context) - p.ts > TIMEOUT) {
    cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
    //the client died after deleting the object. clean up.
    cleanup(p, err);
  }
  *ret = string(p.val.c_str(), p.val.length());
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
    return oid("0"+key, raw_val, max_key);
  } else {
    *max_key = kvmap.begin()->first;
    *raw_val = kvmap.begin()->second;
    cout << "\t" << client_name << ": oid is returning "
	    << string(raw_val->c_str(), raw_val->length()) << std::endl;
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
    cout << "\t\t" << client_name << "-split: reading " << obj
	<< " failed with " << err;
    cout << std::endl;
    return err;
  }
  cout << "\t\t" << client_name << "-split: size of " << obj << "is "
      << info->size << ", version is " << info->version << std::endl;
  if (info->size < 2*k){
    return -1;
  }

  ///////preparations that happen outside the critical section
  //for prefix index
  std::map<std::string, std::string> to_create;
  std::set<object_info*> to_delete;
  prefix_data p;
  vector<librados::ObjectWriteOperation*> owos;
  vector<pair<string, librados::ObjectWriteOperation*> > ops[5];
  to_delete.insert(info);

  //for lower half object
  map<std::string, bufferlist> lower;
  map<std::string, bufferlist>::iterator it = info->omap.begin();
  for (int i = 0; i < k; i++) {
    lower.insert(*it);
  }
  string low_key = "0" + lower.rbegin()->first;
  to_create[low_key] =
      to_string(client_name, client_index++);


  //for upper half object
  map<std::string,bufferlist> high;
  high.insert(++info->omap.find(lower.rbegin()->first), info->omap.end());
  to_create[high_key] = to_string(client_name,client_index++);

  //setting up operations
  librados::ObjectWriteOperation create[2];
  librados::ObjectWriteOperation other_ops[4];
  ops[0].push_back(make_pair(index_name, &other_ops[0]));
  ops[1].push_back(make_pair(to_create[low_key], &create[0]));
  ops[1].push_back(make_pair(to_create[high_key], &create[1]));
  ops[2].push_back(make_pair(obj, &other_ops[1]));
  ops[3].push_back(make_pair(obj, &other_ops[2]));
  ops[4].push_back(make_pair(index_name, &other_ops[3]));

  set_up_prefix_index(to_create, to_delete, ops[0][0].second, &p, &err);
  set_up_make_object(lower, ops[1][0].second);
  set_up_make_object(high, ops[1][1].second);
  set_up_unwrite_object(info->version, ops[2][0].second);
  set_up_delete_object(ops[3][0].second);
  set_up_remove_prefix(p, ops[4][0].second, &err);

  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for obj
  err = perform_ops("\t\t" + client_name + "-split:", p, &ops);
  if (err < 0) {
    return err;
  }
  cout << "\t\t" << client_name << "-split: done splitting."
      << std::endl;
  /////END CRITICAL SECTION/////

  return err;
}

int KvFlatBtreeAsync::rebalance(const string &o1, const string &hk1, int *ver,
    bool reverse){
  int err = 0;
  string o2;
  string hk2;
  err = next(hk1, o1, &hk2, &o2);
  if (err < 0) {
    return err;
  }
  cout << "\t\t" << client_name << "-rebalance: next is (" << hk2 << "," << o2
      << ")" << std::endl;
  if (o1 == o2) {
    prev(hk1, o1, &hk2, &o2);
    cout << "\t\t" << client_name << "-rebalance: prev is (" << hk2 << ","
	<< o2 << ")" << std::endl;
    if (o1 == o2) {
      librados::ObjectReadOperation read_o1;
      librados::AioCompletion * read_o1_aioc = rados.aio_create_completion();
      map<std::string,bufferlist> o1_map;
      bufferlist unw1;
      read_o1.omap_get_vals("", LONG_MAX, &o1_map, &err);
      read_o1.getxattr("unwritable", &unw1, &err);
      if (interrupt() == 1) {
	return -ESUICIDE;
      }
      io_ctx.aio_operate(o1, read_o1_aioc, &read_o1, NULL);
      read_o1_aioc->wait_for_safe();
      err = read_o1_aioc->get_return_value();
      if (err < 0 || string(unw1.c_str(), unw1.length()) == "1") {
	if (err == -ENOENT || err == 0) {
	  cout << "\t\t" << client_name << "-rebalance: ENOENT on reading "
	      << o1 << std::endl;
	  return -ECANCELED;
	} else {
	  cout << "\t\t" << client_name << "-rebalance: found an unexpected "
	      << "error reading"
	" " << o1 << "-rebalance: " << err << std::endl;
	  return err;
	}
      }
      *ver = read_o1_aioc->get_version();
      cout << "\t\t" << client_name << "-rebalance: this is the only node, "
	  << "so aborting" << std::endl;
      return -1;
    }
    return rebalance(o2, hk2, ver, true);
  }
  cout << "\t\t" << client_name << "-rebalance: o1 is " << o1 << " , o2 is "
      << o2 << std::endl;

  //read o1
  librados::ObjectReadOperation read_o1;
  librados::AioCompletion * read_o1_aioc = rados.aio_create_completion();
  map<std::string,bufferlist> o1_map;
  bufferlist unw1;
  read_o1.omap_get_vals("", LONG_MAX, &o1_map, &err);
  read_o1.getxattr("unwritable", &unw1, &err);
  io_ctx.aio_operate(o1, read_o1_aioc, &read_o1, NULL);
  read_o1_aioc->wait_for_safe();
  err = read_o1_aioc->get_return_value();
  if (err < 0 || string(unw1.c_str(), unw1.length()) == "1") {
    if (err == -ENODATA || err == -ENOENT) {
      cout << "\t\t" << client_name << "-rebalance: error " << err
	  << " reading " << o1 << std::endl;
      return err;
    } else if (err == 0) {
      cout << "\t\t" << client_name << "-rebalance: " << o1
	  << " is unwritable" << std::endl;
      return -ECANCELED;
    } else {
      cout << "rebalance found an unexpected error reading"
	  " " << o1 << "-rebalance: " << err << std::endl;
      return err;
    }
  }
  int vo1 = read_o1_aioc->get_version();
  int size1 = o1_map.size();
  cout << "\t\t" << client_name << "-rebalance: read " << o1 << ". size: "
      << size1 << " version: " << vo1
      << std::endl;

  //read o2
  librados::ObjectReadOperation read_o2;
  librados::AioCompletion * read_o2_aioc = rados.aio_create_completion();
  map<std::string,bufferlist> o2_map;
  bufferlist unw2;
  read_o2.omap_get_vals("",LONG_MAX, &o2_map, &err);
  read_o2.getxattr("unwritable", &unw2, &err);
  io_ctx.aio_operate(o2, read_o2_aioc, &read_o2, NULL);
  read_o2_aioc->wait_for_safe();
  err = read_o2_aioc->get_return_value();
  if (err < 0 || string(unw2.c_str(), unw2.length()) == "1") {
    if (err == -ENODATA || err == -ENOENT) {
      cout << "\t\t" << client_name << "-rebalance: error " << err
	  << " reading " << o2 << std::endl;
      return -ECANCELED;
    } else if (err == 0) {
      cout << "\t\t" << client_name << "-rebalance: " << o2
	  << " is unwritable" << std::endl;
      return -ECANCELED;
    } else {
      cout << "rebalance found an unexpected error reading"
	  " " << o2 << "-rebalance: " << err << std::endl;
      return err;
    }
  }
  int vo2 = read_o2_aioc->get_version();
  int size2 = o2_map.size();
  cout << "\t\t" << client_name << "-rebalance: read " << o2 << ". size: "
      << size2 << " version: " << vo2
      << std::endl;

  if (reverse) {
    *ver = vo2;
  } else {
    *ver = vo1;
  }

  //calculations
  if (size1 >= k && size1 <= 2*k && size2 >= k && size2 <= 2*k) {
    //nothing to do
    cout << "\t\t" << client_name << "-rebalance: both sizes in range, so"
	<< " aborting" << std::endl;
    return -1;
  }
  bool rebalance; //true if rebalancing, false if merging

  string o2w = to_string(client_name, client_index++);  //this is the high
							//object. it gets
							//created regardless of
							//rebalance or merge.
  string o1w; //this is the low new object. it is not used for merges.
  librados::ObjectWriteOperation write1; //this is also not used for merge

  //index skeleton
  librados::ObjectWriteOperation prefix_index;
  map<std::string, pair<bufferlist, int> > assertions;
  assertions[hk1] = pair<bufferlist, int>(to_bl("0"+o1),
      CEPH_OSD_CMPXATTR_OP_EQ);
  assertions[hk2] = pair<bufferlist, int>(to_bl("0"+o2),
      CEPH_OSD_CMPXATTR_OP_EQ);
  prefix_index.omap_cmp(assertions, &err);
  std::map<std::string,bufferlist> prefixed_entries;

  //creating higher new object skeleton
  librados::ObjectWriteOperation write2;
  map<std::string, bufferlist> write2_map;
  write2.create(true);
  write2.setxattr("unwritable", to_bl("0"));

  //unwritabling old objects
  librados::ObjectWriteOperation flag1;
  flag1.assert_version(vo1);
  flag1.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  flag1.setxattr("unwritable", to_bl("1"));
  librados::ObjectWriteOperation flag2;
  flag2.assert_version(vo2);
  flag2.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  flag2.setxattr("unwritable", to_bl("1"));

  //deleting old objects
  librados::ObjectWriteOperation rm1;
  rm1.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
  rm1.remove();
  librados::ObjectWriteOperation rm2;
  rm2.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
  rm2.remove();

  //reseting the index
  map<std::string, bufferlist> new_index;
  std::set<std::string> index_keyset;
  index_keyset.insert(hk1);
  index_keyset.insert(hk2);
  librados::ObjectWriteOperation fix_index;
  fix_index.omap_rm_keys(index_keyset);

  if (size1 + size2 <= 2*k) {
    //merge
    cout << "\t\t" << client_name << "-rebalance: merging " << o1
	<< " and " << o2 << " to get " << o2w
	<< std::endl;
    rebalance = false;
    write2_map.insert(o1_map.begin(), o1_map.end());
    write2_map.insert(o2_map.begin(), o2_map.end());

    stringstream pre;
    stringstream clk;
    utime_t now = ceph_clock_now(g_ceph_context);
    clk << now.sec() << '.' << now.usec();
    pre << "1" << to_string_f(clk.str())
	<< pair_init << to_string_f(hk2) << sub_separator
	    << to_string_f(o2w) << pair_end
	<< sub_terminator
	<< pair_init << to_string_f(hk1) << sub_separator
	    << to_string_f(o1) << sub_separator
	    << vo1 << pair_end
	<< pair_init << to_string_f(hk2) << sub_separator
	    << to_string_f(o2) << sub_separator
	    << vo2 << pair_end
	<< terminator;
    prefixed_entries[hk1] = to_bl(pre.str() + o1);
    prefixed_entries[hk2] = to_bl(pre.str() + o2);


    //deal with ops
    prefix_index.omap_set(prefixed_entries);
    write2.omap_set(write2_map);
    new_index[hk2] = to_bl("0" + o2w);//high key of the high object won't change
    fix_index.omap_set(new_index);
  } else {
    //rebalance
    rebalance = true;
    cout << "\t\t" << client_name << "-rebalance: rebalancing" << o1
	<< " and " << o2 << std::endl;
    map<std::string, bufferlist> write1_map;
    map<std::string, bufferlist>::iterator it;
    o1w = to_string(client_name, client_index++);
    for (it = o1_map.begin();
	it != o1_map.end() && (int)write1_map.size() <= (size1 + size2) / 2;
	++it) {
      write1_map.insert(*it);
    }
    if (it != o1_map.end()){
      //write1_map is full, so put the rest in write2_map
      write2_map.insert(it, o1_map.end());
      write2_map.insert(o2_map.begin(), o2_map.end());
    } else {
      //o1_map was small, and write2_map still needs more
      map<std::string, bufferlist>::iterator it2;
      for(it2 = o2_map.begin();
	  (it2 != o2_map.end()) && ((int)write1_map.size()
	      <= (size1 + size2) / 2);
	  ++it2) {
	write1_map.insert(*it2);
      }
      write2_map.insert(it2, o2_map.end());
    }
    string hk1w = "0" + write1_map.rbegin()->first;

    //at this point, write1_map and write2_map should have the correct pairs
    stringstream pre;
    stringstream clk;
    utime_t now = ceph_clock_now(g_ceph_context);
    clk << now.sec() << '.' << now.usec();
    pre << "1" << to_string_f(clk.str())
	<< pair_init << to_string_f(hk1w) << sub_separator
	<< to_string_f(o1w) << pair_end
	<< pair_init << to_string_f(hk2) << sub_separator
	<< to_string_f(o2w) << pair_end
	<< sub_terminator
	<< pair_init << to_string_f(hk1) << sub_separator
	    << to_string_f(o1) << sub_separator
	    << vo1 << pair_end
	<< pair_init << to_string_f(hk2) << sub_separator
	    << to_string_f(o2) << sub_separator
	    << vo2 << pair_end
	<< terminator;
    prefixed_entries[hk1] = to_bl(pre.str() + o1);
    prefixed_entries[hk2] = to_bl(pre.str() + o2);
    
    prefix_index.omap_set(prefixed_entries);
    write2.omap_set(write2_map);
    write1.setxattr("unwritable",to_bl("0"));
    write1.omap_set(write1_map);
    new_index[hk2] = to_bl("0" + o2w);//high key of the high object won't change
    new_index[hk1w] = to_bl("0" + o1w);

    fix_index.omap_set(new_index);
  }
  
  (assertions[hk1]).first = prefixed_entries[hk1];
  (assertions[hk2]).first = prefixed_entries[hk2];
  fix_index.omap_cmp(assertions, &err);

  //at this point, all operations should be completely set up.
  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for obj
  cout << "\t\t" << client_name << "-rebalance: adding prefix " << std::endl;
  err = io_ctx.operate(index_name, &prefix_index);
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: failed to add prefix: " << err
	<< std::endl;
    return -EPREFIX;
  }
  cout << "\t\t" << client_name << "-rebalance: prefix added." << std::endl;

  //make new objects
  if (rebalance) {
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t\t" << client_name << "-rebalance: creating " << o1w
	<< std::endl;
    err = io_ctx.operate(o1w, &write1);
    if (err < 0) {
      //this can happen if someone else was cleaning up after us.
      cout << "\t\t" << client_name << "-split: creating " << o1w << " failed"
  	 << " with code " << err << std::endl;
      if (err == -EEXIST) {
        //someone thinks we died, so die
        return -ESUICIDE;
      } else {
        assert(false);
      }
      return err;
    }
    cout << "\t\t" << client_name << "-rebalance: created " << o1w << std::endl;
  }
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  cout << "\t\t" << client_name << "-rebalance: creating " << o2w << std::endl;
  err = io_ctx.operate(o2w, &write2);
  if (err < 0) {
    //this can happen if someone else was cleaning up after us.
    cout << "\t\t" << client_name << "-split: creating " << o2w << " failed"
	 << " with code " << err << std::endl;
    if (err == -EEXIST) {
      //someone thinks we died, so die
      return -ESUICIDE;
    } else {
      assert(false);
    }
    return err;
  }
  cout << "\t\t" << client_name << "-rebalance: created " << o2w << std::endl;
  
  //mark the objects unwritable, asserting the version number
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  cout << "\t\t" << client_name << "-rebalance: marking " << o1 << std::endl;
  err = io_ctx.operate(o1, &flag1);
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: marking " << o1
    	<< " failed with code" << err << std::endl;
    prefix_data p;
    err = parse_prefix(&prefixed_entries[hk2], &p);
    if (err < 0) {
      cout << "\t\t" << client_name << "-rebalance: bad prefix!" << std::endl;
      assert(false);
      return err;
    }
    cleanup(p, -ETIMEDOUT);
    return -ECANCELED;
  }
  cout << "\t\t" << client_name << "-rebalance: marked " << o1 << std::endl;

  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  cout << "\t\t" << client_name << "-rebalance: marking " << o2 << std::endl;
  err = io_ctx.operate(o2, &flag2);
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: marking " << o2
	<< " failed with code" << err << std::endl;
    prefix_data p;
    err = parse_prefix(&prefixed_entries[hk1], &p);
    if (err < 0) {
      cout << "\t\t" << client_name << "-rebalance: bad prefix!" << std::endl;
      assert(false);
      return -err;
    }
    cleanup(p, -ETIMEDOUT);
    return -ECANCELED;
  }
  cout << "\t\t" << client_name << "-rebalance: marked " << o2 << std::endl;
  
  //delete the unwritable object
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  cout << "\t\t" << client_name << "-rebalance: removing " << o1 << std::endl;
  err = io_ctx.operate(o1, &rm1);
  if (err < 0) {
    //someone else is cleaning up after us
    cout << "\t\t" << client_name << "-rebalance: failed to delete " << o1
	<< ": " << err << std::endl;
    return -ESUICIDE;
  }
  cout << "\t\t" << client_name << "-rebalance: removed " << o1 << std::endl;
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  cout << "\t\t" << client_name << "-rebalance: removing " << o2 << std::endl;
  err = io_ctx.operate(o2, &rm2);
  cout << "\t\t" << client_name << "-rebalance: removed " << o2 << std::endl;
  if (err < 0) {
    //someone else is cleaning up after us
    cout << "\t\t" << client_name << "-rebalance: failed to delete " << o2
	<< ": " << err << std::endl;
    return -ESUICIDE;
  }

  //update the index
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  cout << "\t\t" << client_name << "-rebalance: updating index" << std::endl;
  err =io_ctx.operate(index_name, &fix_index);
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: rewriting the index failed "
	<< "with code "
	<< err << ". someone else must have thought we died, so dying"
	<< std::endl;
    return -ESUICIDE;
  }
  cout << "\t\t" << client_name << "-rebalance: updated index" << std::endl;
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
    const std::set<object_info*> &to_delete,
    librados::ObjectWriteOperation * owo,
    prefix_data * p,
    int * err) {
  std::map<std::string, pair<bufferlist, int> > assertions;
  map<string, bufferlist> to_insert;
  stringstream strm;
  stringstream clk;
  utime_t now = ceph_clock_now(g_ceph_context);
  clk << now.sec() << '.' << now.usec();
  strm << "1" << to_string_f(clk.str());
  for(map<string, string>::const_iterator it = to_create.begin();
      it != to_create.end();
      ++it) {
    strm << pair_init << to_string_f(it->first) << sub_separator
	<< to_string_f(it->second) << pair_end;
  }
  strm << sub_terminator;
  for(std::set<object_info*>::const_iterator it = to_delete.begin();
      it != to_delete.end();
      ++it) {
    strm << pair_init << to_string_f((*it)->key) << sub_separator
	  << to_string_f((*it)->name)  << sub_separator
	  << (*it)->version << pair_end;
  }
  strm << terminator;
  for(std::set<object_info*>::const_iterator it = to_delete.begin();
      it != to_delete.end();
      ++it) {
    to_insert[(*it)->key] = to_bl(strm.str() + (*it)->name);
    assertions[(*it)->key] = pair<bufferlist, int>(to_bl("0"+(*it)->name),
	CEPH_OSD_CMPXATTR_OP_EQ);
  }
  bufferlist index_bl = to_bl(strm.str());
  parse_prefix(&index_bl, p);
  owo->omap_cmp(assertions, err);
  owo->omap_set(to_insert);
}

void KvFlatBtreeAsync::set_up_make_object(
    const map<std::string, bufferlist> &to_set,
    librados::ObjectWriteOperation *owo) {
  owo->create(true);
  owo->omap_set(to_set);
  owo->setxattr("unwritable", to_bl("0"));
}

void KvFlatBtreeAsync::set_up_unwrite_object(
    const int &ver, librados::ObjectWriteOperation *owo) {
  owo->assert_version(ver);
  owo->cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  owo->setxattr("unwritable", to_bl("1"));
}

void KvFlatBtreeAsync::set_up_delete_object(
    librados::ObjectWriteOperation *owo) {
  owo->cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
  owo->remove();
}

void KvFlatBtreeAsync::set_up_remove_prefix(
    const prefix_data &p,
    librados::ObjectWriteOperation * owo,
    int * err) {
  map<string, bufferlist> to_insert;
  map<string, pair<bufferlist, int> > assertions;
  for (vector<vector<string> >::const_iterator it = p.to_create.begin();
      it != p.to_create.end(); ++it) {
    to_insert[(*it)[0]] = to_bl("0" + (*it)[1]);
  }
  for (vector<vector<string> >::const_iterator it = p.to_delete.begin();
      it != p.to_delete.end(); ++it) {
    assertions[(*it)[0]] = pair<bufferlist, int>(
	to_bl(p.prefix + (*it)[1]), CEPH_OSD_CMPXATTR_OP_EQ);
  }
  owo->omap_cmp(assertions, err);
  owo->omap_set(to_insert);
}

int KvFlatBtreeAsync::perform_ops(const string &debug_prefix,
    const prefix_data &p,
    vector<pair<string, librados::ObjectWriteOperation*> > (*ops)[5]) {
  int err = 0;
  for (int i = 0; i < 5; ++i) {
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    switch (i) {
    case 0://prefixing
      cout << debug_prefix << " adding prefix" << std::endl;
      err = io_ctx.operate(index_name, (*ops[i])[0].second);
      if (err < 0) {
        cout << debug_prefix << " prefixing the index failed with "
            << err << std::endl;
        return err;
      }
      cout << debug_prefix << " debug_prefix added." << std::endl;
      break;
    case 1://creating
      for (vector<pair<string, librados::ObjectWriteOperation*> >::iterator it
  	= (*ops)[i].begin(); it != (*ops)[i].end(); ++it) {
        cout << debug_prefix << " creating" << it->first << std::endl;
        err = io_ctx.operate(it->first, it->second);
        if (err < 0) {
          //this can happen if someone else was cleaning up after us.
          cout << debug_prefix << " creating " << it->first << " failed"
              << " with code " << err << std::endl;
          if (err == -EEXIST) {
            //someone thinks we died, so die
            return -ESUICIDE;
          } else {
            assert(false);
          }
          return err;
        }
        cout << debug_prefix << " created " << it->first << std::endl;
        if (interrupt() == 1 ) {
          return -ESUICIDE;
        }
      }
      break;
    case 2://marking
      for (vector<pair<string, librados::ObjectWriteOperation*> >::iterator it
  	= (*ops)[i].begin(); it != (*ops)[i].end(); ++it) {
	cout << debug_prefix << " marking " << it->first << std::endl;
	err = io_ctx.operate(it->first, it->second);
	if (err < 0) {
	  //most likely because it changed, in which case it will be -ERANGE
	  cout << debug_prefix << " marking " << it->first
	      << "failed with code" << err << std::endl;
	  cleanup(p, -ETIMEDOUT);
	  return -ECANCELED;
	}
	cout << debug_prefix << " marked " << it->first << std::endl;
        if (interrupt() == 1 ) {
          return -ESUICIDE;
        }
      }
      break;
    case 3://deleting
      for (vector<pair<string, librados::ObjectWriteOperation*> >::iterator it
  	= (*ops)[i].begin(); it != (*ops)[i].end(); ++it) {
	cout << debug_prefix << " deleting " << it->first << std::endl;
	err = io_ctx.operate(it->first, it->second);
	if (err < 0) {
	  //most likely because it changed, in which case it will be -ERANGE
	  cout << debug_prefix << " deleting " << it->first
	      << "failed with code" << err << std::endl;
	  return -ESUICIDE;
	}
	cout << debug_prefix << " deleted " << it->first << std::endl;
        if (interrupt() == 1 ) {
          return -ESUICIDE;
        }
      }
      break;
    case 4://rewriting index
      cout << debug_prefix << " updating index " << std::endl;
      err = io_ctx.operate(index_name, (*ops)[i][0].second);
      if (err < 0) {
        cout << debug_prefix
    	<< " rewriting the index failed with code " << err
        << ". someone else must have thought we died, so dying" << std::endl;
        assert(false);
        return -ESUICIDE;
      }
      cout << debug_prefix << " updated index." << std::endl;
      break;
    }
  }
  return err;
}

int KvFlatBtreeAsync::cleanup(const prefix_data &p, const int &errno) {
  cout << "\t\t" << client_name << ": cleaning up after " << p.prefix
      << std::endl;
  int err = 0;
  map<std::string,bufferlist> new_index;
  map<std::string, pair<bufferlist, int> > assertions;
  switch (errno) {
  case -ENOENT: {
    cout << "\t\t" << client_name << "-cleanup: rolling forward" << std::endl;
    //all changes were created except for updating the index and possibly
    //deleting the objects. roll forward.
    std::set<std::string> rm_index;
    for(vector<vector<string> >::const_iterator it =
	p.to_create.begin();
	it != p.to_create.end(); ++it) {
      cout << "\t\t\t" << client_name << "-cleanup: will add ("
	  << (*it)[0] << ", " << (*it)[1] << ") to index" << std::endl;
      new_index[(*it)[0]] = to_bl("0" + (*it)[1]);
    }
    for(vector<vector <string> >::const_iterator it =
	p.to_delete.begin();
	it != p.to_delete.end(); ++it) {
      librados::ObjectWriteOperation rm;
      if (interrupt() == 1 ) {
	return -ESUICIDE;
      }
      //this assert should never fail.
      rm.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
      rm.remove();
      cout << "\t\t\t" << client_name << "-cleanup: deleting " << (*it)[1]
	   << std::endl;
      err = io_ctx.operate((*it)[1], &rm);
      if (err < 0 && err != -ENOENT) {
	//someone else probably got here first.
	cout << "\t\t\t" << client_name << "-cleanup: error deleting "
	    << (*it)[1] << ": " << err << std::endl;
	assert(false);
	return err;
      }
      assertions[(*it)[0]] =
	  pair<bufferlist, int>(to_bl(p.prefix + (*it)[1]),
	      CEPH_OSD_CMPXATTR_OP_EQ);
      rm_index.insert((*it)[0]);
    }
    librados::ObjectWriteOperation update_index;
    update_index.omap_cmp(assertions, &err);
    update_index.omap_rm_keys(rm_index);
    update_index.omap_set(new_index);
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t\t\t" << client_name << "-cleanup: updating index" << std::endl;
    err = io_ctx.operate(index_name, &update_index);
    if (err < 0) {
      cout << "\t\t\t" << client_name << "-cleanup: rewriting failed with "
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
    map<std::string, pair<bufferlist, int> > assertions;
    for(vector<vector<string> >::const_reverse_iterator it =
	p.to_create.rbegin();
	it != p.to_create.rend(); ++it) {
      librados::ObjectWriteOperation rm;
      rm.setxattr("unwritable", to_bl("0"));
      cout << "\t\t\t" << client_name << "-cleanup: marking " << (*it)[1]
	  << std::endl;
      if (interrupt() == 1 ) {
        return -ESUICIDE;
      }
      err = io_ctx.operate((*it)[1], &rm);
      if (err < 0) {
	cout << "\t\t\t" << client_name << "-cleanup: marking " << (*it)[1]
	    << " failed with " << err << std::endl;
      }
    }
    for(vector<vector<string> >::const_iterator it =
	p.to_delete.begin();
	it != p.to_delete.end(); ++it) {
      new_index[(*it)[0]] = to_bl("0" + (*it)[1]);
      assertions[(*it)[0]] =
	  pair<bufferlist, int>(to_bl(p.prefix + (*it)[1]),
	      CEPH_OSD_CMPXATTR_OP_EQ);
      librados::ObjectWriteOperation restore;
      restore.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("1"));
      restore.setxattr("unwritable", to_bl("0"));
      cout << "\t\t\t" << client_name << "-cleanup: restoring " << (*it)[1]
	  << std::endl;
      cout << "\t\t\t" << client_name << "-cleanup: will assert index contains "
	  << "(" << (*it)[0] << "," << p.prefix << (*it)[1]
	  << ")" << std::endl;
      cout << "\t\t\t" << client_name << "-cleanup: (*it)[1] is " << (*it)[1]
           << std::endl;
      if (interrupt() == 1 ) {
        return -ESUICIDE;
      }
      err = io_ctx.operate((*it)[1], &restore);
      if (err == -ENOENT) {
	//it had gotten far enough to be rolled forward - roll forward.
	return cleanup(p, err);
      }
      cout << "\t\t\t" << client_name << "-cleanup: restored " << (*it)[1]
	  << std::endl;
    }
    for(vector<vector<string> >::const_reverse_iterator it =
	p.to_create.rbegin();
	it != p.to_create.rend(); ++it) {
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
    update_index.omap_set(new_index);
    cout << "\t\t\t" << client_name << "-cleanup: updating index" << std::endl;
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    err = io_ctx.operate(index_name, &update_index);
    if (err < 0) {
      cout << "\t\t\t" << client_name << "-cleanup: rewriting failed with "
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
  index_map["1"] = to_bl("0" + client_name);
  make_index.omap_set(index_map);
  r = io_ctx.operate(index_name, &make_index);
  if (r < 0) {
    cout << client_name << ": Making the index failed with code " << r
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
  //current_op << client_name << " is setting " << key << std::endl;
  cout << client_name << " is setting " << key << std::endl;
  int err = 0;
  bufferlist objb;
  string obj;
  string hk;
  utime_t mytime;
  prefix_data p;
  object_info o;

  while (err != -1) {
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t" << client_name << ": finding oid" << std::endl;
    err = oid("0"+key, &objb, &hk);
    mytime = ceph_clock_now(g_ceph_context);
    if (err < 0) {
      cout << "getting oid failed with code " << err;
      cout << std::endl;
      return err;
    }
    p.clear();
    err = parse_prefix(&objb, &p);
    if (err < 0) {
      return err;
    }
    obj = string(p.val.c_str(), p.val.length());

    cout << "\t" << client_name << ": obj is " << obj << std::endl;
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t" << client_name << ": running split on " << obj << std::endl;
    err = split(obj, hk, &o);
    if (err < 0) {
      cout << "\t" << client_name << ": split returned " << err << std::endl;
      if (p.prefix == "" && err == -EPREFIX) {
        oid("0"+key, &objb, &hk);
        parse_prefix(&objb, &p);
        mytime = ceph_clock_now(g_ceph_context);
      }
      if (p.prefix != "" && mytime - p.ts > TIMEOUT) {
        cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
        //the client died after deleting the object. clean up.
        cleanup(p, err);
      } else if (p.prefix != "") {
        cout << "\t" << client_name << ": prefix and not timed out, "
  	  << "so restarting ( it has been " << (mytime - p.ts).sec()
  	  << '.' << (mytime - p.ts).usec()
  	  << ", timeout is " << TIMEOUT <<")" << std::endl;
      } else if (err != -1 && err != -ECANCELED && err != -ENOENT
	  && err != -EPREFIX){
	cout << "\t" << client_name
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
    cout << "performing insertion failed with code " << err;
    cout << std::endl;
  }
  write_aioc->wait_for_safe();
  err = write_aioc->get_return_value();
  cout << "\t" << client_name << ": write finished with " << err << std::endl;
  if (err < 0) {
    cout << "\t" << client_name << ": writing obj failed. "
	<< "probably failed assert. " << err << std::endl;
    return set(key, val, update_on_existing);
  }
  if (p.prefix != "" && mytime - p.ts >= TIMEOUT) {
    //client died before objects were deleted
    cleanup(p,-ETIMEDOUT);
  }
  err = rebalance(obj, hk, &o.version, false);
  if (p.prefix != "" && mytime - p.ts > TIMEOUT) {
    cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
    //the client died after deleting the object. clean up.
    cleanup(p, err);
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
  int obj_ver;
  prefix_data p;

  while (err != -1) {
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t" << client_name << ": finding oid" << std::endl;
    err = oid("0"+key, &objb, &hk);
    mytime = ceph_clock_now(g_ceph_context);
    if (err < 0) {
      cout << "getting oid failed with code " << err;
      cout << std::endl;
      return err;
    }
    err = parse_prefix(&objb, &p);
    if (err < 0) {
      cout << "\t" << client_name << ": parsing prefix failed - bad prefix. "
	  << err <<  std::endl;
      return err;
    }
    obj = string(p.val.c_str(), p.val.length());
    cout << "\t" << client_name << ": obj is " << obj << std::endl;
    if (interrupt() == 1 ) {
      return -ESUICIDE;
    }
    cout << "\t" << client_name << ": rebalancing " << obj << std::endl;
    err = rebalance(obj, hk, &obj_ver, false);
    if (err < 0) {
      cout << "\t" << client_name << ": rebalance returned " << err
	  << std::endl;
      if (err == -ENOENT) {
	if (mytime - p.ts <= TIMEOUT) {
	  return remove(key);
	} else {
	  //the client died after deleting the object. clean up.
	  cout << "\t" << client_name << ": need to cleanup" << std::endl;
	  cleanup(p, err);
	}
      } else if (err != -1 && err != -ECANCELED) {
	cout << "rebalance encountered an unexpected error: "
	    << err << std::endl;
	return err;
      }
      if (err != -1 && wait_index > 3) {
	wait_index -= 3;
      }
    }
  }

  //write
  librados::ObjectWriteOperation owo;
  librados::AioCompletion * write_aioc = rados.aio_create_completion();
  owo.assert_version(obj_ver);
  owo.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  std::set<std::string> to_rm;
  to_rm.insert(key);
  owo.omap_rm_keys(to_rm);
  if (interrupt() == 1 ) {
    return -ESUICIDE;
  }
  cout << "\t" << client_name << ": removing. asserting version " << obj_ver
      << std::endl;
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
  if (p.prefix != "" && mytime - p.ts > TIMEOUT) {
    cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
    //the client died after deleting the object. clean up.
    cleanup(p, err);
  }
  do {
    err = rebalance(obj, hk, &obj_ver, false);
    cout << "\t" << client_name << ": rebalance after remove got " << err
	<< std::endl;
    if (err == -ESUICIDE) {
      return err;
    } else if (p.prefix != "" && err == -EPREFIX) {
      mytime = ceph_clock_now(g_ceph_context);
    } else if (p.prefix == "" && err == -EPREFIX) {
      oid("0"+key, &objb, &hk);
      parse_prefix(&objb, &p);
      mytime = ceph_clock_now(g_ceph_context);
    }
    if (p.prefix != "" && mytime - p.ts > TIMEOUT) {
      cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
      //the client died after deleting the object. clean up.
      cleanup(p, err);
      oid("0"+key, &objb, &hk);
      parse_prefix(&objb, &p);
      mytime = ceph_clock_now(g_ceph_context);
    } else if (p.prefix != "") {
      cout << "\t" << client_name << ": prefix and not timed out, "
	  << "so restarting ( it has been " << (mytime - p.ts).sec()
	  << '.' << (mytime - p.ts).usec()
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
    cout << "getting oid failed with code " << err;
    cout << std::endl;
    return err;
  }
  prefix_data p;
  err = parse_prefix(&objb, &p);
  if (err < 0) {
    cout << "\t" << client_name << "parsing prefix failed - bad prefix. "
	<< err << std::endl;
    return err;
  }
  obj = string(p.val.c_str(), p.val.length());
  if (err < 0) {
    cout << "getting oid failed with code " << err;
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
      if (mytime - p.ts <= TIMEOUT) {
	return get(key, val);
      } else {
	//the client died after deleting the object. clean up.
       cleanup(p, err);
      }
    } else {
      cout << "split encountered an unexpected error: " << err << std::endl;
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
    cout << "rm index aioc failed - probably failed assertion. " << err;
    cout << std::endl;
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
      prefix_data p;
      err = parse_prefix(&it->second, &p);
      if (err < 0) {
	cout << "error parsing prefix" << std::endl;
	return err;
      }
      io_ctx.operate(string(p.val.c_str(), p.val.length()), &sub);
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
    prefix_data p;
    err = parse_prefix(&it->second, &p);
    if (err < 0) {
      cout << "error parsing prefix" << std::endl;
      return err;
    }
    io_ctx.operate(string(p.val.c_str(), p.val.length()), &sub, NULL);
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
      prefix_data p;
      err = parse_prefix(&it->second, &p);
      if (err < 0) {
        cout << "Not consistent! Bad prefix on obj for key " << it->first
            << std::endl;
	ret = false;
      }
      if (p.prefix != "") {
	for(vector<vector<string> >::iterator dit = p.to_delete.begin();
	    dit != p.to_delete.end(); dit++) {
	  librados::ObjectReadOperation oro;
	  librados::AioCompletion * aioc = rados.aio_create_completion();
	  bufferlist un;
	  oro.getxattr("unwritable", &un, &err);
	  err = io_ctx.aio_operate((*dit)[1], aioc, &oro, NULL);
	  aioc->wait_for_safe();
	  err = aioc->get_return_value();
	  if (ceph_clock_now(g_ceph_context) - p.ts > TIMEOUT) {
	    if (err < 0) {
	      if (err == -ENOENT) {
		continue;
	      } else {
		cout << "Not consistent! reading object " << (*dit)[1]
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
	for(vector<vector<string> >::iterator cit = p.to_create.begin();
	    cit != p.to_create.end(); cit++) {
	  onames.insert((*cit)[1]);
	}
      }
      parsed_index.insert(make_pair(it->first,
	  string(p.val.c_str(), p.val.length())));
      onames.insert(string(p.val.c_str(), p.val.length()));
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
    ret << "|" << string((148 -
	((*it).first.length()+it->second.length()+3))/2,' ');
    ret << (*it).first;
    ret << " | ";
    ret << string(it->second.c_str(), it->second.length());
    ret << string((148 -
	((*it).first.length()+it->second.length()+3))/2,' ');
    ret << "|\t";
    prefix_data p;
    err = parse_prefix(&it->second, &p);
    if (err < 0) {
      return ret.str();
    }
    all_names.push_back(string(p.val.c_str(), p.val.length()));
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
