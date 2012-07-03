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

  librados::ObjectIterator it;
  for (it = io_ctx.objects_begin(); it != io_ctx.objects_end(); ++it) {
    librados::ObjectWriteOperation rm;
    rm.remove();
    io_ctx.operate(it->first, &rm);
  }

  librados::ObjectWriteOperation make_obj;
  librados::AioCompletion * make_obj_aioc = rados.aio_create_completion();
  map<string, bufferlist> to_set;
  to_set["key"] = to_bl("value");
  make_obj.omap_set(to_set);
  io_ctx.aio_operate("object", make_obj_aioc, &make_obj);
  make_obj_aioc->wait_for_safe();
  cout << "return value: " << make_obj_aioc->get_return_value() << " version: "
      << make_obj_aioc->get_version() << std::endl;
  int err = make_obj_aioc->get_return_value();
  if (err < 0) {
    cout << "initial write failed with " << err << std::endl;
    return err;
  }

  librados::ObjectReadOperation read_obj;
  librados::AioCompletion * read_obj_aioc = rados.aio_create_completion();
  map<string, bufferlist> get_omap;
  //read_obj.omap_get_vals("", LONG_MAX, &get_omap, &err);
  uint64_t *psize;
  time_t *pmtime;
  read_obj.stat(psize, pmtime, &err);
  io_ctx.aio_operate("object", read_obj_aioc, &read_obj, NULL);
  read_obj_aioc->wait_for_safe();
  cout << "read found version: " << read_obj_aioc->get_version()
      << " and error: " << read_obj_aioc->get_return_value() << std::endl;

  librados::ObjectWriteOperation modify_obj;
  librados::AioCompletion * modify_obj_aioc = rados.aio_create_completion();
  to_set["key1"] = to_bl("value1");
  modify_obj.omap_set(to_set);
  io_ctx.aio_operate("object", modify_obj_aioc, &modify_obj);
  modify_obj_aioc->wait_for_safe();
  err = modify_obj_aioc->get_return_value();
  if (err < 0) {
    cout << "second write failed with " << err << std::endl;
    return err;
  }

  librados::ObjectReadOperation read_obj_again;
  librados::AioCompletion * read_obj_again_aioc = rados.aio_create_completion();
  read_obj_again.omap_get_vals("", LONG_MAX, &get_omap, &err);
  io_ctx.aio_operate("object", read_obj_again_aioc, &read_obj_again, NULL);
  read_obj_again_aioc->wait_for_safe();
  cout << "read found version: " << read_obj_again_aioc->get_version()
      << " and error: " << read_obj_again_aioc->get_return_value() << std::endl;

/*  librados::ObjectWriteOperation make_max_obj;
  make_max_obj.create(true);
  //make_max_obj.setxattr("size", to_bl("",0));
  make_max_obj.setxattr("unwritable", to_bl("0"));
  io_ctx.operate(client_name, &make_max_obj);

  librados::ObjectWriteOperation make_index;
  make_index.create(true);
  map<string,bufferlist> index_map;
  index_map["1"] = to_bl("0" + client_name);
  make_index.omap_set(index_map);
  r = io_ctx.operate(index_name, &make_index);
  if (r < 0) {
    cout << "Making the index failed with code " << r << std::endl;
    return r;
  }
*/
  return r;

}

void KvFlatBtreeAsync::set_waits(const vector<__useconds_t> &wait) {
  waits = wait;
  wait_index = 0;
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

string KvFlatBtreeAsync::to_string_f(string s) {
  stringstream ret;
  for (int i = 0; i < (int)s.length(); i++) {
    if (s[i] == pair_init
	|| s[i] == sub_separator
	|| s[i] ==  pair_end
	|| s[i] == sub_terminator
	|| s[i] == terminator) {
      ret << "\\" << s[i];
    }
    else {
      ret << s[i];
    }
  }
  return ret.str();
}

int KvFlatBtreeAsync::bl_to_int(bufferlist *bl) {
  return atoi(string(bl->c_str(), bl->length()).c_str());
}

int KvFlatBtreeAsync::next(const string &obj_high_key, const string &obj,
    string * ret_high_key, string *ret) {
  current_op << "\t\tfinding next of (" << obj_high_key << "," << obj << ")"
        << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<string, bufferlist> kvs;
  oro.omap_get_vals(obj_high_key,LONG_MAX,&kvs,&err);
  err = io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "getting kvs failed with error " <<err;
    cout << std::endl;
    return err;
  }
  if (kvs.size() > 0) {
    *ret_high_key = kvs.begin()->first;
    prefix_data p;
    current_op << "\t\t";
    err = parse_prefix(&kvs.begin()->second, &p);
    if (err < 0) {
      cout << "next: invalid prefix found. " << err << std::endl;
      return err;
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
  current_op << "\t\tfinding prev of (" << obj_high_key << "," << obj << ")"
        << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<string, bufferlist> kvs;
  oro.omap_get_vals("",LONG_MAX,&kvs,&err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "getting kvs failed with error " <<err;
    cout << std::endl;
    return err;
  }

  std::map<string, bufferlist>::iterator it =
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
  current_op << "\t\t";
  err = parse_prefix(&it->second, &p);
  if (err < 0) {
    cout << "next: invalid prefix found. " << err << std::endl;
    return err;
  }
  *ret = string(p.val.c_str(), p.val.length());
  return err;
}

int KvFlatBtreeAsync::oid(const string &key, bufferlist * raw_val,
    string * max_key) {
  current_op << "\tgetting oid for " << key << std::endl;
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
	current_op << "\toid is returning "
	    << string(raw_val->c_str(), raw_val->length()) << std::endl;
	return err;
      }
    }
  }
  //if we haven't returned yet, it's because the key is higher than the highest
  //existing bucket.
  *max_key = "1";
  *raw_val = to_bl("0" + client_name);
  return err;
}

//safe, theoretically
int KvFlatBtreeAsync::parse_prefix(bufferlist * bl, prefix_data * ret) {
  current_op << "\tparsing prefix from " << string(bl->c_str(), bl->length())
      << std::endl;
  string s(bl->c_str(), bl->length());
  bool escape = false;
  bool end_ts = false;
  int val_index = 1;
  bool val = false;
  vector<pair<string, string> > * current = &ret->to_create;
  pair<stringstream, stringstream> this_pair;
  stringstream ts;
  stringstream * dump_ptr = &ts;
  if (s[0] == '0') {
    ret->val = to_bl(
	string(bl->c_str(), bl->length()).substr(1,bl->length() - 1));
    return 0;
  }
  current_op << "\t\tdump_ptr set to ts: ";
  for (int i = 1; i < (int)bl->length(); i++) {
    if (val) {
      string value(s.substr(val_index, s.length() - 1));

      ret->val = to_bl(value);
      break;
    }
    if (!escape) {
      if (s[i] == '\\') {
	//current_op << "\t\tescape set" << std::endl;
	escape = true;
      }
      else if (s[i] == pair_init) {
	if (!end_ts) {
	  end_ts = true;
	  ret->ts = utime_t(atoi(ts.str().c_str()), 0);
	}
	else if (dump_ptr != &this_pair.second) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  cout << "\t\terring after" << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	current_op << std::endl << "\t\tdump_ptr set to pair.first: ";
	dump_ptr = &this_pair.first;
      }
      else if (s[i] == sub_separator) {
	if (!end_ts || dump_ptr != &this_pair.first) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  current_op << "\t\terring after" << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	current_op << std::endl << "\t\tdump_ptr set to pair.second: ";
	dump_ptr = &this_pair.second;
      }
      else if (s[i] == pair_end) {
	if (!end_ts || dump_ptr != &this_pair.second) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  current_op << "\t\terring after" << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	current->push_back(
	    pair<string,string>(this_pair.first.str(), this_pair.second.str()));
      }
      else if (s[i] == sub_terminator) {
	if (!end_ts || !(dump_ptr == &this_pair.second
	    && current == &(ret->to_create))) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  current_op << "\t\terring after" << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	current_op << std::endl << "\t\tto_delete: ";
	current = &ret->to_delete;
      } else if (s[i] == terminator) {
	if (!(dump_ptr == &this_pair.second
	    && current == &(ret->to_delete))) {
	  cout << "badly formatted prefix! " << s << std::endl;
	  current_op << "\t\terring after" << s.substr(0,i) << std::endl;
	  return -EINVAL;
	}
	val_index = i + 1;
	ret->prefix = s.substr(0,val_index);
	val = true;
      } else {
	current_op << s[i];
	*dump_ptr << s[i];
      }
    }
    else {
      current_op << s[i];
      *dump_ptr << s[i];
      escape = false;
    }
  }
  current_op << std::endl
      << "\t\tval = " << string(ret->val.c_str(), ret->val.length())
      << std::endl;
  return 0;
}

int KvFlatBtreeAsync::cleanup(const prefix_data &p, const int &errno) {
  current_op << "\tcleaning up after " << p.prefix << std::endl;
  cout << "\t" << client_name << ": cleaning up after " << p.prefix
      << std::endl;
  int err = 0;
  map<string,bufferlist> new_index;
  map<string, pair<bufferlist, int> > assertions;
  switch (errno) {
  case -ENODATA: {
    //all changes were created except for updating the index and possibly
    //deleting the objects. roll forward.
    std::set<string> rm_index;
    for(vector<pair<string, string> >::const_iterator it = p.to_delete.begin();
	it != p.to_delete.end(); ++it) {
      if (it->first != "1") {
	librados::ObjectWriteOperation rm;
	usleep(waits[wait_index++]);
	rm.remove();
	io_ctx.operate(it->first, &rm);
	assertions[it->first] =
	    pair<bufferlist, int>(to_bl(p.prefix + it->second),
		CEPH_OSD_CMPXATTR_OP_EQ);
	rm_index.insert(it->first);
      }
    }
    for(vector<pair<string, string> >::const_iterator it = p.to_create.begin();
	it != p.to_create.end(); ++it) {
      new_index[it->first] = to_bl("0" + it->second);
    }
    librados::ObjectWriteOperation update_index;
    update_index.omap_cmp(assertions, &err);
    update_index.omap_rm_keys(rm_index);
    update_index.omap_set(new_index);
    usleep(waits[wait_index++]);
    io_ctx.operate(index_name, &update_index);
    break;
  }
  case -ETIMEDOUT: {
    //roll back all changes.
    map<string,bufferlist> new_index;
    map<string, pair<bufferlist, int> > assertions;
    for(vector<pair<string, string> >::const_iterator it = p.to_delete.begin();
	it != p.to_create.end(); ++it) {
      new_index[it->first] = to_bl("0" + it->second);
      assertions[it->first] =
	  pair<bufferlist, int>(to_bl(p.prefix + it->second),
	      CEPH_OSD_CMPXATTR_OP_EQ);
      librados::ObjectWriteOperation restore;
      restore.setxattr("unwritable", to_bl("0"));
      usleep(waits[wait_index++]);
      io_ctx.operate(it->first, &restore);
    }
    for(vector<pair<string, string> >::const_iterator it = p.to_create.begin();
	it != p.to_delete.end(); ++it) {
      librados::ObjectWriteOperation rm;
      rm.remove();
      usleep(waits[wait_index++]);
      io_ctx.operate(it->first, &rm);
    }
    librados::ObjectWriteOperation update_index;
    update_index.omap_cmp(assertions, &err);
    update_index.omap_set(new_index);
    usleep(waits[wait_index++]);
    io_ctx.operate(index_name, &update_index);
    break;
  }
  }
  return err;
}

int KvFlatBtreeAsync::split(const string &obj, const string &high_key,
    int * ver, map<string, bufferlist> * all) {
  current_op << "\tsplitting " << obj << std::endl;
  int err = 0;
  librados::ObjectReadOperation get_obj;
  map<string, bufferlist> lower;
  std::set<string> index_keyset;

  //read obj
  current_op << "\t\treading " << obj << std::endl;
  librados::AioCompletion * obj_aioc = rados.aio_create_completion();
  get_obj.omap_get_vals("", LONG_MAX, all, &err);
  get_obj.omap_get_vals("", k, &lower, &err);
  err = io_ctx.aio_operate(obj, obj_aioc, &get_obj, NULL);
  if (err < 0){
    //possibly -ENODATA, meaning someone else deleted it.
    current_op << "\t\treading " << obj << " failed with " << err;
    cout << std::endl;
    return err;
  }
  obj_aioc->wait_for_safe();
  *ver = obj_aioc->get_version();
  current_op << "\t\tfinished reading " << obj << " version " << *ver
      << std::endl;
  int obj_size = all->size();
  cout << "\t\t" << client_name << "-split: size of " << obj << "is "
      << obj_size << std::endl;
  if (obj_size < 2*k){
    current_op << "\t\tCan't split " << obj << " - not full" << std::endl;
    return -1;
  }

  ///////preparations that happen outside the critical section
  //for lower half object
  map<string, bufferlist>::reverse_iterator rit = lower.rbegin();
  string o1w = to_string(client_name, client_index++);
  string key1("0" + rit->first);
  int size1 = lower.size();
  librados::ObjectWriteOperation write1;
  write1.create(true);
  write1.omap_set(lower);
  write1.setxattr("unwritable", to_bl("0"));
  current_op << "\t\twill make object (" << key1 << "," << o1w << ")"
      << std::endl;

  //for upper half object
  bufferlist size2 = to_bl("",
      obj_size
      - size1);
  map<string,bufferlist> high;
  high.insert(++all->find(rit->first), all->end());
  string o2w = to_string(client_name,client_index++);
  librados::ObjectWriteOperation write2;
  write2.create(true);
  write2.omap_set(high);
  write2.setxattr("unwritable", to_bl("0"));
  current_op << "\t\twill make object (" << high_key << "," << o2w << ")"
      << std::endl;

  //unwritabling old object
  librados::ObjectWriteOperation unwritable_old;
  unwritable_old.setxattr("unwritable",to_bl("1"));
  current_op << "\t\twill mark object " << obj << " unwritable."
      << std::endl;

  //deleting old object
  librados::ObjectWriteOperation delete_old;
  delete_old.remove();
  current_op << "\t\twill delete " << obj << std::endl;

  //setting up initial write of index
  stringstream strm;
  stringstream clk;
  clk << ceph_clock_now(g_ceph_context);
  strm << "1" << to_string_f(clk.str())
      << pair_init << to_string_f(key1) << sub_separator
      << to_string_f(o1w) << pair_end
      << pair_init << to_string_f(high_key) << sub_separator
      << to_string_f(o2w) << pair_end
      << sub_terminator
      << pair_init << to_string_f(high_key) << sub_separator
      << to_string_f(obj) << pair_end
      << terminator << obj;
  bufferlist index_bl = to_bl(strm.str());
  std::map<string, bufferlist> prefixed;
  prefixed[high_key] = index_bl;
  std::map<string, pair<bufferlist, int> > assertions;
  assertions[high_key] = pair<bufferlist, int>(to_bl("0"+obj), CEPH_OSD_CMPXATTR_OP_EQ);
  librados::ObjectWriteOperation prefix_index;
  prefix_index.omap_cmp(assertions, &err);
  prefix_index.omap_set(prefixed);


  //index updating
  librados::ObjectWriteOperation update_map_object;
  map<string,bufferlist> index_obj_map;
  current_op << "\t\tops for fixing index:" << std::endl;
  current_op << "\t\t\tremove " << high_key << std::endl;
  index_obj_map.insert(pair<string,bufferlist>(key1, to_bl("0" + o1w)));
  index_obj_map.insert(pair<string,bufferlist>(high_key, to_bl("0" + o2w)));
  current_op << "\t\t\tinsert (" << key1 << ",0" << o1w << ") and ("
      << high_key << ",0" << o2w << ")" << std::endl;
  update_map_object.omap_set(index_obj_map);
  update_map_object.omap_rm_keys(index_keyset);

  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for obj
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-split: adding prefix "
      << high_key << ", "
      << string(index_bl.c_str(), index_bl.length()) << std::endl;
  err = io_ctx.operate(index_name, &prefix_index);
  if (err < 0) {
    cout << "\t\t" << client_name << "-split: prefixing the index failed with "
	<< err << std::endl;
    return err;
  }
  cout << "\t\t" << client_name << "-split: prefix added." << std::endl;

  //make new objects
  librados::AioCompletion * aioc1 = rados.aio_create_completion();
  librados::AioCompletion * aioc2 = rados.aio_create_completion();
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-split: creating object " << o1w << std::endl;
  io_ctx.aio_operate(o1w, aioc1, &write1);
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-split: creating object " << o2w << std::endl;
  io_ctx.aio_operate(o2w, aioc2, &write2);
  aioc1->wait_for_safe();
  cout << "\t\t" << client_name << "-split: created object " << o1w << std::endl;
  aioc2->wait_for_safe();
  cout << "\t\t" << client_name << "-split: created object " << o2w << std::endl;
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
  io_ctx.set_assert_version(*ver);
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-split: marking object " << obj << std::endl;
  err = io_ctx.aio_operate(obj, aioc_obj, &unwritable_old);
  aioc_obj->wait_for_safe();
  err = aioc_obj->get_return_value();
  if (err < 0) {
    //most likely because it changed, in which case it will be -ECANCELED
    current_op << "\t\t" << client_name << "-split: marking " << obj
	<< "failed with code" << err << std::endl;
    librados::AioCompletion * a1 = rados.aio_create_completion();
    librados::AioCompletion * a2 = rados.aio_create_completion();
    librados::ObjectWriteOperation rm1;
    librados::ObjectWriteOperation rm2;
    rm1.remove();
    rm2.remove();
    cout << "\t\t" << client_name << "-split: deleting object " << o2w << std::endl;
    io_ctx.aio_operate(o2w, a2, &rm1);
    cout << "\t\t" << client_name << "-split: deleting object " << o1w << std::endl;
    io_ctx.aio_operate(o1w, a1, &rm2);
    a1->wait_for_safe();
    cout << "\t\t" << client_name << "-split: deleted object " << o2w << std::endl;
    a2->wait_for_safe();
    cout << "\t\t" << client_name << "-split: deleted object " << o1w << std::endl;
    return err;
  }
  cout << "\t\t" << client_name << "-split: marked object " << obj << std::endl;

  //delete the unwritable object
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-split: deleting object " << obj << std::endl;
  err = io_ctx.operate(obj, &delete_old);
  cout << "\t\t" << client_name << "-split: deleted object " << obj << std::endl;
  if (err < 0) {
    //this shouldn't happen
    cout << "failed to delete " << obj << ": " << err << std::endl;
    return err;
  }

  //update the index
  librados::AioCompletion * index_aioc = rados.aio_create_completion();
  assertions[high_key].first = index_bl;
  update_map_object.omap_cmp(assertions, &err);
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-split: updating index " << std::endl;
  io_ctx.aio_operate(index_name, index_aioc, &update_map_object);
  index_aioc->wait_for_safe();
  err = index_aioc->get_return_value();
  if (err < 0) {
    //this shouldn't happen
    cout << "rewriting the index failed with code " << err;
    cout << ". someone else must have thought we died, so dying" << std::endl;
    return err;
  }
  cout << "\t\t" << client_name << "-split: updated index. done splitting."
      << std::endl;
  /////END CRITICAL SECTION/////

  return err;
}

int KvFlatBtreeAsync::rebalance(const string &o1, const string &hk1, int *ver){
  current_op << "\trebalancing " << o1 << std::endl;
  int err = 0;
  string o2;
  string hk2;
  usleep(waits[wait_index++]);
  next(hk1, o1, &hk2, &o2);
  cout << "\t\t" << client_name << "-rebalance: next is (" << hk2 << "," << o2 << ")" << std::endl;
  if (o1 == o2) {
    usleep(waits[wait_index++]);
    prev(hk1, o1, &hk2, &o2);
    cout << "\t\t" << client_name << "-rebalance: prev is (" << hk2 << "," << o2 << ")" << std::endl;
    if (o1 == o2) {
      cout << "\t\t" << client_name << "-rebalance: this is the only node, so aborting" << std::endl;
      return -1;
    }
    return rebalance(o2, hk2, ver);
  }
  cout << "\t\t" << client_name << "-rebalance: o1 is " << o1 << " , o2 is "
      << o2 << std::endl;

  //read o1
  librados::ObjectReadOperation read_o1;
  librados::AioCompletion * read_o1_aioc = rados.aio_create_completion();
  map<string,bufferlist> o1_map;
  bufferlist unw1;
  read_o1.omap_get_vals("", LONG_MAX, &o1_map, &err);
  read_o1.getxattr("unwritable", &unw1, &err);
  usleep(waits[wait_index++]);
  io_ctx.aio_operate(o1, read_o1_aioc, &read_o1, NULL);
  read_o1_aioc->wait_for_safe();
  err = read_o1_aioc->get_return_value();
  if (err < 0 || string(unw1.c_str(), unw1.length()) == "1") {
    if (err == -ENODATA || err == 0) {
      cout << "\t\t" << client_name << "-rebalance: ENODATA on reading " << o1 << std::endl;
      return err;
    }
    else {
      cout << "rebalance found an unexpected error reading"
	  " " << o1 << "-rebalance: " << err << std::endl;
      return err;
    }
  }
  int vo1 = read_o1_aioc->get_version();
  int size1 = o1_map.size();
  cout << "\t\t" << client_name << "-rebalance: read " << o1 << ". size: " << size1 << " version: " << vo1
      << std::endl;

  //read o2
  librados::ObjectReadOperation read_o2;
  librados::AioCompletion * read_o2_aioc = rados.aio_create_completion();
  map<string,bufferlist> o2_map;
  bufferlist unw2;
  read_o2.omap_get_vals("",LONG_MAX, &o2_map, &err);
  read_o2.getxattr("unwritable", &unw2, &err);
  usleep(waits[wait_index++]);
  io_ctx.aio_operate(o2, read_o2_aioc, &read_o2, NULL);
  read_o2_aioc->wait_for_safe();
  err = read_o2_aioc->get_return_value();
  if (err < 0 || string(unw2.c_str(), unw2.length()) == "1") {
    if (err == -ENODATA || err == 0) return err;
    else {
      cout << "rebalance found an unexpected error reading"
	  " " << o2 << "-rebalance: " << err << std::endl;
      return err;
    }
  }
  int vo2 = read_o2_aioc->get_version();
  int size2 = o2_map.size();
  cout << "\t\t" << client_name << "-rebalance: read " << o2 << ". size: " << size2 << " version: " << vo1
      << std::endl;

  //calculations
  if (size1 >= k && size1 <= 2*k && size2 >= k && size2 <= 2*k) {
    //nothing to do
    cout << "\t\t" << client_name << "-rebalance: both sizes in range, so aborting" << std::endl;
    return -1;
  }
  bool rebalance;
  string o2w = to_string(client_name, client_index++);
  string o1w;
  librados::ObjectWriteOperation write1;
  //index skeleton
  librados::ObjectWriteOperation prefix_index;
  map<string, pair<bufferlist, int> > assertions;
  assertions[hk1] = pair<bufferlist, int>(to_bl("0"+o1),
      CEPH_OSD_CMPXATTR_OP_EQ);
  assertions[hk2] = pair<bufferlist, int>(to_bl("0"+o2),
      CEPH_OSD_CMPXATTR_OP_EQ);
  prefix_index.omap_cmp(assertions, &err);
  bufferlist index_bl;
  std::map<string,bufferlist> prefixed_entries;

  //creating higher new object skeleton
  librados::ObjectWriteOperation write2;
  map<string, bufferlist> write2_map;
  write2.create(true);
  write2.setxattr("unwritable", to_bl("0"));

  //unwritabling old objects
  librados::ObjectWriteOperation flag1;
  flag1.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  flag1.setxattr("unwritable", to_bl("1"));
  librados::ObjectWriteOperation flag2;
  flag2.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  flag2.setxattr("unwritable", to_bl("1"));

  //deleting old objects
  librados::ObjectWriteOperation rm1;
  rm1.remove();
  librados::ObjectWriteOperation rm2;
  rm2.remove();
  current_op << "\t\twill delete " << o1 << " and " << o2 << std::endl;

  //reseting the index
  map<string, bufferlist> new_index;
  std::set<string> index_keyset;
  current_op << "\t\tops for fixing index:" << std::endl;
  current_op << "\t\t\tremove " << hk1 << " and " << hk2 << std::endl;
  index_keyset.insert(hk1);
  index_keyset.insert(hk2);
  librados::ObjectWriteOperation fix_index;
  fix_index.omap_rm_keys(index_keyset);

  if (size1 + size2 <= 2*k) {
    //merge
    current_op << "\t\tmerging " << o1 << " and " << o2 << " to get " << o2w
	<< std::endl;
    rebalance = false;
    write2_map.insert(o1_map.begin(), o1_map.end());
    write2_map.insert(o2_map.begin(), o2_map.end());
    string o1w = to_string(client_name, client_index++);

    stringstream pre;
    pre << "1" << ceph_clock_now(g_ceph_context)
	<< pair_init << to_string_f(hk2) << sub_separator
	    << to_string_f(o1w) << pair_end
	<< sub_terminator
	<< pair_init << to_string_f(hk1) << sub_separator
	    << to_string_f(o1) << pair_end
	<< pair_init << to_string_f(hk2) << sub_separator
	    << to_string_f(o2) << pair_end
	<< terminator;
    prefixed_entries[hk1] = to_bl(pre.str() + o1);
    prefixed_entries[hk2] = to_bl(pre.str() + o2);


    //deal with ops
    prefix_index.omap_set(prefixed_entries);
    write2.omap_set(write2_map);
    new_index[hk2] = to_bl("0" + o2w);
    fix_index.omap_set(new_index);
    current_op << "\t\t\tprepared map with " << write2_map.size()
	<< " keys to write to " << o2w << std::endl;
  } else {
    rebalance = true;
    current_op << "\t\trebalancing" << o1 << " and " << o2 << std::endl;
    map<string, bufferlist> write1_map;
    map<string, bufferlist>::iterator it;
    o1w = to_string(client_name, client_index++);
    for (it = o1_map.begin();
	it != o1_map.end() && (int)write1_map.size() <= (size1 + size2) / 2;
	++it) {
      write1_map.insert(*it);
    }
    current_op << "\t\tset up write1_map with size "
	<< write1_map.size() << std::endl;
    if (it != o1_map.end()){
      //write1_map is full, so put the rest in write2_map
      current_op << "\t\twrite1 is full, so put the rest in write2_map" << std::endl;
      write2_map.insert(it, o1_map.end());
      write2_map.insert(o2_map.begin(), o2_map.end());
    } else {
      current_op << "\t\to1_map was small, and write1_map needs more" << std::endl;
      //o1_map was small, and write2_map still needs more
      current_op << "\t\to2_map.size() is " << o2_map.size() << std::endl;
      map<string, bufferlist>::iterator it2;
      for(it2 = o2_map.begin();
	  (it2 != o2_map.end()) && ((int)write1_map.size() <= (size1 + size2) / 2);
	  ++it2) {
	current_op << "\t\tinserting " << it2->first << " into write1_map " << std::endl;
	write1_map.insert(*it2);
      }
      current_op << "finished inserting things into write2_map" << std::endl;
      write2_map.insert(it2, o2_map.end());
    }
    current_op << "\t\tnow write2_map has size " << write2_map.size()
	<< " and write1_map has size " << write1_map.size() << std::endl;
    string hk1w = "0" + write1_map.rbegin()->first;

    //at this point, write1_map and write1_map should have the correct pairs
    stringstream pre;
    stringstream clk;
    clk << ceph_clock_now(g_ceph_context);
    pre << "1" << to_string_f(clk.str())
	<< pair_init << hk1w << sub_separator << o1w << pair_end
	<< pair_init << hk2 << sub_separator << o1w << pair_end
	<< sub_terminator
	<< pair_init << hk1 << sub_separator << o1 << pair_end
	<< pair_init << hk2 << sub_separator << o2 << pair_end
	<< terminator;
    prefixed_entries[hk1] = to_bl(pre.str() + o1);
    prefixed_entries[hk2] = to_bl(pre.str() + o2);
    
    prefix_index.omap_set(prefixed_entries);
    write2.omap_set(write2_map);
    write1.setxattr("unwritable",to_bl("0"));
    write1.omap_set(write1_map);
    new_index[hk2] = to_bl("0" + o2w);
    new_index[hk1w] = to_bl("0" + o1w);

    fix_index.omap_set(new_index);
  }
  
  //at this point, all operations should be completely set up.
  /////BEGIN CRITICAL SECTION/////
  //put prefix on index entry for obj
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-rebalance: adding prefix "
        << string(index_bl.c_str(), index_bl.length()) << std::endl;
  err = io_ctx.operate(index_name, &prefix_index);
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: failed to add prefix: " << err
	<< std::endl;
    return err;
  }

  //make new objects
  librados::AioCompletion * aioc2 = rados.aio_create_completion();
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-rebalance: creating " << o2w << std::endl;
  io_ctx.aio_operate(o2w, aioc2, &write2);
  current_op << "\t\tlaunched write of " << o2w << std::endl;
  if (rebalance) {
    librados::AioCompletion * aioc1 = rados.aio_create_completion();
    usleep(waits[wait_index++]);
    cout << "\t\t" << client_name << "-rebalance: creating " << o1w << std::endl;
    io_ctx.aio_operate(o1w, aioc1, &write1);
    cout << "\t\t" << client_name << "-rebalance: created " << o1w << std::endl;
    aioc1->wait_for_safe();
    err = aioc1->get_return_value();
    if (err < 0) {
      //This is not an error that should happen, so not catching it for now.
      assert(false);
      cout << "rebalancing failed - creating second object failed"
       << " with code " << err << std::endl;
      librados::ObjectWriteOperation clean2;
      clean2.remove();
      usleep(waits[wait_index++]);
      io_ctx.operate(o2w, &clean2);
      return err;
    }

  }
  aioc2->wait_for_safe();
  err = aioc2->get_return_value();
  if (err < 0) {
    //This is not an error that should happen, so not catching it for now.
    cout << "rebalancing failed - creating first object failed"
	 << " with code " << err << std::endl;
    if (rebalance) {
      librados::ObjectWriteOperation clean1;
      clean1.remove();
      usleep(waits[wait_index++]);
      io_ctx.operate(o1w, &clean1);
    }
    return err;
  }
  cout << "\t\t" << client_name << "-rebalance: created " << o2w << std::endl;
  
  //mark the objects unwritable, asserting the version number
  io_ctx.set_assert_version(vo1);
  librados::AioCompletion * un_aioc1 = rados.aio_create_completion();
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-rebalance: marking " << o1 << std::endl;
  err = io_ctx.aio_operate(o1, un_aioc1, &flag1);
  io_ctx.set_assert_version(vo2);
  librados::AioCompletion * un_aioc2 = rados.aio_create_completion();
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-rebalance: marking " << o2 << std::endl;
  err = io_ctx.aio_operate(o2, un_aioc2, &flag2);
  aioc2->wait_for_safe();
  err = un_aioc2->get_return_value();
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: marking " << o2
	<< " failed with code" << err << std::endl;
    un_aioc1->wait_for_safe();
    if (un_aioc1->get_return_value() == 0) {
      librados::ObjectWriteOperation restore2;
      restore2.setxattr("unwritable", to_bl("0"));
      usleep(waits[wait_index++]);
      cout << "\t\t" << client_name << "-rebalance: restoring " << o1 << std::endl;
      io_ctx.operate(o1, &restore2);
      cout << "\t\t" << client_name << "-rebalance: restored " << o1 << std::endl;
    }
    librados::AioCompletion * a1 = rados.aio_create_completion();
    librados::AioCompletion * a2 = rados.aio_create_completion();
    librados::ObjectWriteOperation rm1;
    rm1.remove();
    rm2.remove();
    usleep(waits[wait_index++]);
    cout << "\t\t" << client_name << "-rebalance: removing " << o1w << std::endl;
    io_ctx.aio_operate(o1w, a2, &rm1);
    usleep(waits[wait_index++]);
    cout << "\t\t" << client_name << "-rebalance: removing " << o2w << std::endl;
    io_ctx.aio_operate(o2w, a1, &rm2);
    a1->wait_for_safe();
    if (a1->get_return_value() < 0) {
      cout << "\t\t" << client_name << "-rebalance: removing " << o2w << " failed "
	  << "with code " << a1->get_return_value() << std::endl;
      return a1->get_return_value();
    } else {
      cout << "\t\t" << client_name << "-rebalance: removed " << o2w << std::endl;
    }
    a2->wait_for_safe();
    if (a2->get_return_value() < 0) {
      cout << "\t\t" << client_name << "-rebalance: removing " << o2w << " failed "
	  << "with code " << a2->get_return_value() << std::endl;
      return a2->get_return_value();
    } else {
      cout << "\t\t" << client_name << "-rebalance: removed " << o1w << std::endl;
    }
    return err;
  }
  cout << "\t\t" << client_name << "-rebalance: marked " << o2 << std::endl;
  un_aioc1->wait_for_safe();
  err = un_aioc1->get_return_value();
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: marking " << o1
    	<< " failed with code" << err << std::endl;
    un_aioc2->wait_for_safe();
    if (un_aioc2->get_return_value() == 0) {
      librados::ObjectWriteOperation restore1;
      restore1.setxattr("unwritable", to_bl("0"));
      usleep(waits[wait_index++]);
      cout << "\t\t" << client_name << "-rebalance: restoring " << o2 << std::endl;
      io_ctx.operate(o2, &restore1);
      cout << "\t\t" << client_name << "-rebalance: restored " << o2 << std::endl;
    }
    librados::AioCompletion * a1 = rados.aio_create_completion();
    librados::AioCompletion * a2 = rados.aio_create_completion();
    librados::ObjectWriteOperation rm1;
    rm1.remove();
    rm2.remove();
    usleep(waits[wait_index++]);
    cout << "\t\t" << client_name << "-rebalance: removing " << o2w << std::endl;
    io_ctx.aio_operate(o2w, a2, &rm1);
    usleep(waits[wait_index++]);
    cout << "\t\t" << client_name << "-rebalance: removing " << o1w << std::endl;
    io_ctx.aio_operate(o1w, a1, &rm2);
    a1->wait_for_safe();
    if (a1->get_return_value() < 0) {
      cout << "\t\t" << client_name << "-rebalance: removing " << o2w << " failed "
	  << "with code " << a1->get_return_value() << std::endl;
      return a1->get_return_value();
    } else {
      cout << "\t\t" << client_name << "-rebalance: removed " << o2w << std::endl;
    }
    a2->wait_for_safe();
    if (a2->get_return_value() < 0) {
      cout << "\t\t" << client_name << "-rebalance: removing " << o2w << " failed "
	  << "with code " << a2->get_return_value() << std::endl;
      return a2->get_return_value();
    } else {
      cout << "\t\t" << client_name << "-rebalance: removed " << o1w << std::endl;
    }
    return err;
  }
  
  //delete the unwritable object
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-rebalance: removing " << o1 << std::endl;
  err = io_ctx.operate(o1, &rm1);
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: failed to delete " << o1
	<< ": " << err << std::endl;
    return err;
  }
  cout << "\t\t" << client_name << "-rebalance: removed " << o1 << std::endl;
  usleep(waits[wait_index++]);
  cout << "\t\t" << client_name << "-rebalance: removing " << o2 << std::endl;
  err = io_ctx.operate(o2, &rm2);
  cout << "\t\t" << client_name << "-rebalance: removed " << o2 << std::endl;
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: failed to delete " << o2
	<< ": " << err << std::endl;
    return err;
  }

  //update the index
  librados::AioCompletion * index_aioc = rados.aio_create_completion();
  usleep(waits[wait_index++]);
  (assertions[hk1]).first = prefixed_entries[hk1];
  (assertions[hk2]).first = prefixed_entries[hk2];
  fix_index.omap_cmp(assertions, &err);
  cout << "\t\t" << client_name << "-rebalance: updating index" << std::endl;
  io_ctx.aio_operate(index_name, index_aioc, &fix_index);
  index_aioc->wait_for_safe();
  err = index_aioc->get_return_value();
  if (err < 0) {
    cout << "\t\t" << client_name << "-rebalance: rewriting the index failed with code "
	<< err << ". someone else must have thought we died, so dying"
	<< std::endl;
        return err;
  }
  cout << "\t\t" << client_name << "-rebalance: updated index" << std::endl;
  /////END CRITICAL SECTION/////
  return err;
}

/*KvFlatBtreeAsync::~KvFlatBtreeAsync() {
  current_op.str("");
  current_op << "removing all" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  librados::AioCompletion * oro_aioc = rados.aio_create_completion();
  std::map<string, bufferlist> index_set;
  oro.omap_get_vals("",LONG_MAX,&index_set,&err);
  err = io_ctx.aio_operate(index_name, oro_aioc, &oro, NULL);
  oro_aioc->wait_for_safe();
  int index_ver = oro_aioc->get_version();

  librados::ObjectWriteOperation rm_index;
  librados::AioCompletion * rm_index_aioc  = rados.aio_create_completion();
  io_ctx.set_assert_version(index_ver);
  rm_index.remove();
  io_ctx.operate(index_name, &rm_index);
  //cout << "removed the index" << std::endl;
  err = rm_index_aioc->get_return_value();
  if (err < 0) {
    cout << "rm index aioc failed - probably failed assertion. " << err;
    cout << std::endl;
    delete this;
  }

  if (index_set.size() != 0) {
    for (std::map<string, bufferlist>::iterator it = index_set.begin();
	it != index_set.end(); ++it){
      librados::ObjectWriteOperation sub;
      sub.remove();
      prefix_data p;
      parse_prefix(&it->second, &p);
      io_ctx.operate(string(p.val.c_str(), p.val.length()), &sub);
      //cout << "removed " << *it << std::endl;
    }
  }
}*/

int KvFlatBtreeAsync::set(const string &key, const bufferlist &val,
    bool update_on_existing) {
  current_op.str("");
  //current_op << client_name << " is setting " << key << std::endl;
  cout << client_name << " is setting " << key << std::endl;
  int err = 0;
  bufferlist objb;
  string obj;
  string hk;
  utime_t mytime;
  int obj_ver;

  while (err != -1) {
    usleep(waits[wait_index++]);
    cout << "\t" << client_name << ": finding oid" << std::endl;
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
      current_op << "\tparsing prefix failed - bad prefix. " << err << std::endl;
      return err;
    }
    obj = string(p.val.c_str(), p.val.length());
    cout << "\t" << client_name << ": obj is " << obj << std::endl;
    map<string, bufferlist> duplicates;
    usleep(waits[wait_index++]);
    cout << "\t" << client_name << ": running split on " << obj << std::endl;
    err = split(obj, hk, &obj_ver, &duplicates);
    if (err < 0) {
      if (err == -ENODATA) {
	if (mytime - p.ts <= TIMEOUT) {
	  cout << "\t" << client_name << ": prefix and not timed out, "
	      << "so restarting" << std::endl;
	  return set(key, val, update_on_existing);
	} else {
	  cout << client_name << " THINKS THE OTHER CLIENT DIED." << std::endl;
	  //the client died after deleting the object. clean up.
	 cleanup(p, err);
	}
      } else if (err != -1 && err != -ECANCELED){
	cout << "split encountered an unexpected error: " << err << std::endl;
	return err;
      }
      wait_index -= 3;
    }
    if (!update_on_existing && duplicates.count(key)){
      cout << "\t" << client_name << ": key exists, so returning " << std::endl;
      return -EEXIST;
    }
  }

  cout << "\t" << client_name << ": got our object: " << obj << std::endl;

  //write
  librados::ObjectWriteOperation owo;
  librados::AioCompletion * write_aioc = rados.aio_create_completion();
  io_ctx.set_assert_version(obj_ver);
  map<string, pair<bufferlist, int> > assertions;
  map<string,bufferlist> to_insert;
  to_insert[key] = val;
  owo.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  owo.omap_set(to_insert);
  usleep(waits[wait_index++]);
  err = io_ctx.aio_operate(obj, write_aioc, &owo);
  cout << "\t" << client_name << ": inserting " << key << " with value " << string(to_insert[key].c_str(),
      to_insert[key].length()) << " into object " << obj
      << " with version " << obj_ver << std::endl;
  if (err < 0) {
    cout << "performing insertion failed with code " << err;
    cout << std::endl;
  }
  write_aioc->wait_for_safe();
  err = write_aioc->get_return_value();
  cout << "\t" << client_name << ": write finished with " << err << std::endl;
  if (err < 0) {
    cout << "\t" << client_name << ": writing obj failed - probably failed assert. " << err << std::endl;
    return set(key, val, update_on_existing);
  }

  rebalance(obj, hk, &obj_ver);
  current_op << "\tfinished set" << std::endl;
  //cout << current_op.str() << std::endl;
  return err;
}

int KvFlatBtreeAsync::remove(const string &key) {
  current_op.str("");
  current_op << "removing " << key << std::endl;
  int err = 0;
  bufferlist objb;
  string obj;
  string hk;
  utime_t mytime;
  int obj_ver;

  while (err != -1) {
    usleep(waits[wait_index++]);
    cout << "\t" << client_name << ": finding oid" << std::endl;
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
      current_op << "\tparsing prefix failed - bad prefix. " << err << std::endl;
      return err;
    }
    obj = string(p.val.c_str(), p.val.length());
    current_op << "\tobj is " << obj << std::endl;
    usleep(waits[wait_index++]);
    err = rebalance(obj, hk, &obj_ver);
    if (err < 0) {
      if (err == -ENODATA) {
	if (mytime - p.ts <= TIMEOUT) {
	  return remove(key);
	} else {
	  //the client died after deleting the object. clean up.
	  cout << current_op.str() << std::endl;
	 cleanup(p, err);
	}
      }
      else if (err != -1 && err != -ECANCELED) {
	cout << "rebalance encountered an unexpected error: "
	    << err << std::endl;
	return err;
      }
    }
  }

  //write
  librados::ObjectWriteOperation owo;
  librados::AioCompletion * write_aioc = rados.aio_create_completion();
  io_ctx.set_assert_version(obj_ver);
  owo.cmpxattr("unwritable", CEPH_OSD_CMPXATTR_OP_EQ, to_bl("0"));
  std::set<string> to_rm;
  to_rm.insert(key);
  owo.omap_rm_keys(to_rm);
  usleep(waits[wait_index++]);
  err = io_ctx.aio_operate(obj, write_aioc, &owo);
  write_aioc->wait_for_safe();
  err = write_aioc->get_return_value();
  if (err < 0) {
    cout << "remove: writing failed - probably failed assert. "
	<< err << std::endl;
    return remove(key);
  }
  rebalance(obj, hk, &obj_ver);
  return err;
}

int KvFlatBtreeAsync::remove_all() {
  current_op.str("");
  current_op << "removing all" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  librados::AioCompletion * oro_aioc = rados.aio_create_completion();
  std::map<string, bufferlist> index_set;
  oro.omap_get_vals("",LONG_MAX,&index_set,&err);
  err = io_ctx.aio_operate(index_name, oro_aioc, &oro, NULL);
  if (err < 0){
    if (err == -ENOENT) return 0;
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  oro_aioc->wait_for_safe();

  librados::ObjectWriteOperation rm_index;
  librados::AioCompletion * rm_index_aioc  = rados.aio_create_completion();
  map<string,bufferlist> new_index;
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
    for (std::map<string,bufferlist>::iterator it = index_set.begin();
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

int KvFlatBtreeAsync::get(const string &key, bufferlist *val) {
  current_op.str("");
  current_op << "getting " << key << std::endl;
  int err = 0;
  std::set<string> key_set;
  key_set.insert(key);
  map<string,bufferlist> omap;
  bufferlist objb;
  string obj;
  string hk;
  utime_t mytime;

  usleep(waits[wait_index++]);
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
    current_op << "\tparsing prefix failed - bad prefix. " << err << std::endl;
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
  usleep(waits[wait_index++]);
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

int KvFlatBtreeAsync::get_all_keys(std::set<string> *keys) {
  current_op.str("");
  current_op << "getting all keys" << std::endl;
  int err = 0;
  librados::ObjectReadOperation oro;
  std::map<string,bufferlist> index_set;
  oro.omap_get_vals("",LONG_MAX,&index_set,&err);
  io_ctx.operate(index_name, &oro, NULL);
  if (err < 0){
    cout << "getting keys failed with error " <<err;
    cout << std::endl;
    return err;
  }
  for (std::map<string,bufferlist>::iterator it = index_set.begin();
      it != index_set.end(); ++it){
    librados::ObjectReadOperation sub;
    std::set<string> ret;
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

int KvFlatBtreeAsync::get_all_keys_and_values(map<string,bufferlist> *kv_map) {
  current_op.str("");
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

bool KvFlatBtreeAsync::is_consistent() {
  int err;
  bool ret = true;
  current_op << "checking consistency" << std::endl;
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

  std::map<string, string> parsed_index;
  std::set<string> onames;
  for (map<string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it) {
    if (it->first != "") {
      prefix_data p;
      err = parse_prefix(&it->second, &p);
      if (err < 0) {
        cout << "Not consistent! Bad prefix on obj for key " << it->first
            << std::endl;
	cout << current_op.str() << std::endl;
	ret = false;
      }
      if (p.prefix != "") {
	if (ceph_clock_now(g_ceph_context) - p.ts > TIMEOUT) {
	  cout << "Not consistent! A client has died and left a prefix."
	      << std::endl;
	  cout << it->first << "\t"
	  	    << string(it->second.c_str(), it->second.length()) << std::endl;
	  ret = false;
	}
      } else{
	parsed_index.insert(make_pair(it->first,
	    string(p.val.c_str(), p.val.length())));
	onames.insert(string(p.val.c_str(), p.val.length()));
      }
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
  for (std::map<string, string>::iterator it = parsed_index.begin(); it != parsed_index.end();
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
    for(std::set<string>::iterator subit = sub_objs[it->second].begin();
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

  for (map<string,bufferlist>::iterator it = index.begin();
      it != index.end(); ++it) {
    keys.insert(string(it->second.c_str(), it->second.length())
	.substr(1,it->second.length()));
  }

  vector<string> all_names;
  vector<int> all_sizes(index.size());
  vector<int> all_versions(index.size());
  vector<map<string,bufferlist> > all_maps(keys.size());
  vector<map<string,bufferlist>::iterator> its(keys.size());
  unsigned done = 0;
  vector<bool> dones(keys.size());
  ret << std::endl << string(150,'-') << std::endl;

  for (map<string,bufferlist>::iterator it = index.begin();
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
      cout << current_op.str() << std::endl;
      return ret.str();
    }
    all_names.push_back(string(p.val.c_str(), p.val.length()));
    ret << std::endl << string(150,'-') << std::endl;
  }

  int indexer = 0;

  //get the object names and sizes
  for(vector<string>::iterator it = all_names.begin(); it != all_names.end();
      ++it) {
    librados::ObjectReadOperation oro;
    librados::AioCompletion *aioc = rados.aio_create_completion();
    oro.omap_get_vals("", LONG_MAX, &all_maps[indexer], &err);
    io_ctx.aio_operate(*it, aioc, &oro, NULL);
    aioc->wait_for_safe();
    if (aioc->get_return_value() < 0) {
      ret << "reading the object failed: " << err << std::endl;
      return ret.str();
    }
    all_sizes[indexer] = all_maps[indexer].size();
    all_versions[indexer] = aioc->get_version();
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
	+ to_string("",all_sizes[i]).length()))/2, ' ');
    ret << "size: " << all_sizes[i];
    ret << string((19 - (string("size: ").length()
	  + to_string("",all_sizes[i]).length()))/2, ' ') << "|\t";
  }
  ret << std::endl;
  for (int i = 0; i < indexer; i++) {
    its[i] = all_maps[i].begin();
    ret << "|" << string((19 - (string("version: ").length()
	+ to_string("",all_versions[i]).length()))/2, ' ');
    ret << "version: " << all_versions[i];
    ret << string((19 - (string("version: ").length()
	  + to_string("",all_versions[i]).length()))/2, ' ') << "|\t";
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
