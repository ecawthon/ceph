/*
 * cls_assert_cond.cc
 *
 *  Created on: Aug 10, 2012
 *      Author: eleanor
 */




#include "objclass/objclass.h"
#include "/usr/include/asm-generic/errno-base.h"
#include "/usr/include/asm-generic/errno.h"
#include "key_value_store/kvs_arg_types.h"
#include "include/types.h"
#include <iostream>
#include <climits>


cls_handle_t h_class;
cls_method_handle_t h_get_idata_from_key;
cls_method_handle_t h_get_next_idata;
cls_method_handle_t h_get_prev_idata;
cls_method_handle_t h_check_writable;
cls_method_handle_t h_assert_size_in_bound;
cls_method_handle_t h_omap_insert;
cls_method_handle_t h_omap_remove;
cls_method_handle_t h_maybe_read_for_balance;


/**
 * finds the index_data where a key belongs.
 *
 * @param key: the key to search for
 * @param idata: the index_data for the first index value such that idata.key
 * is greater than key.
 * @param next_idata: the index_data for the next index entry after idata
 * @pre: key is not encoded
 * @post: idata contains complete information
 * stored
 */
static int get_idata_from_key(cls_method_context_t hctx, string key,
    index_data idata, index_data next_idata) {
  bufferlist raw_val;
  int r = 0;
  std::map<std::string, bufferlist> kvmap;
  std::map<std::string, bufferlist> dupmap;

  r = cls_cxx_map_get_vals(hctx, key_data(key).encoded(), "", 2, &kvmap);
  if (r < 0) {
    CLS_ERR("error reading index for %s: %d", key.c_str(), r);
    return r;
  }

  r = cls_cxx_map_get_val(hctx, key_data(key).encoded(), &raw_val);
  if (r == 0){
    idata.kdata = key_data(key);
    bufferlist::iterator b = raw_val.begin();
    idata.decode(b);
    if (next_idata.obj != "" && kvmap.size() != 0) {
      next_idata.kdata.parse(kvmap.begin()->first);
      bufferlist::iterator b = kvmap.begin()->second.begin();
      next_idata.decode(b);
    }
    return r;
  } else if (r == -ENODATA) {
    idata.kdata.parse(kvmap.begin()->first);
    bufferlist::iterator b = kvmap.begin()->second.begin();
    idata.decode(b);
    if (next_idata.obj != "" && idata.kdata.prefix != "1") {
      next_idata.kdata.parse((++kvmap.begin())->first);
      bufferlist::iterator nb = (++kvmap.begin())->second.begin();
      next_idata.decode(nb);
    }
    return 0;
  } else if (r < 0) {
    CLS_ERR("error reading index for %s: %d", key.c_str(), r);
    return r;
  }

  return r;
}


static int get_idata_from_key_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "get_idata_from_key_op");
  idata_from_key_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  int r = get_idata_from_key(hctx, op.key, op.idata, op.next_idata);
  if (r < 0) {
    return r;
  } else {
    op.encode(*out);
    return 0;
  }
}

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
static int get_next_idata(cls_method_context_t hctx, const index_data idata,
    index_data out_data) {
  int r = 0;
  std::map<std::string, bufferlist> kvs;
  r = cls_cxx_map_get_vals(hctx, idata.kdata.encoded(), "", 1, &kvs);
  if (r < 0){
    CLS_LOG(20, "getting kvs failed with error %d", r);
    return r;
  }

  if (kvs.size() > 0) {
    out_data.kdata.parse(kvs.begin()->first);
    bufferlist::iterator b = kvs.begin()->second.begin();
    out_data.decode(b);
  } else {
    r = -EOVERFLOW;
  }

  return r;
}

static int get_next_idata_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "get_next_idata_op");
  idata_from_idata_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  int r = get_next_idata(hctx, op.idata, op.next_idata);
  if (r < 0) {
    return r;
  } else {
    op.encode(*out);
    return 0;
  }
}

/**
 * finds the object in the index with the highest key value that is less
 * than idata.key. If idata.key is the lowest key, returns -ERANGE If
 * idata has a prefix and has timed out, cleans up.
 *
 * @param idata: idata for the object to search for.
 * @param out_data: the idata for the next object.
 *
 * @pre: idata must contain a key.
 * @ost: out_data contains complete information
 */
static int get_prev_idata(cls_method_context_t hctx, const index_data idata,
    index_data out_data) {
  int r = 0;
  std::map<std::string, bufferlist> kvs;
  r = cls_cxx_map_get_vals(hctx, "", "", LONG_MAX, &kvs);
  if (r < 0){
    CLS_LOG(20, "getting kvs failed with error %d", r);
    return r;
  }

  std::map<std::string, bufferlist>::iterator it =
      kvs.lower_bound(idata.kdata.encoded());
  if (it->first != idata.kdata.encoded()) {
    CLS_LOG(20, "object %s not found in the index", idata.str().c_str());
    return -ENOENT;
  }
  if (it == kvs.begin()) {
    //it is the first object, there is no previous.
    return -ERANGE;
  } else {
    it--;
  }
  out_data.kdata.parse(it->first);
  bufferlist::iterator b = it->second.begin();
  out_data.decode(b);

  return 0;
}

static int get_prev_idata_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "get_next_idata_op");
  idata_from_idata_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  int r = get_prev_idata(hctx, op.idata, op.next_idata);
  if (r < 0) {
    return r;
  } else {
    op.encode(*out);
    return 0;
  }
}

/**
 * Checks the unwritable xattr. If it is "1" (i.e., it is unwritable), returns
 * -EACCES. otherwise, returns 0.
 */
static int check_writable(cls_method_context_t hctx) {
  bufferlist bl;
  int r = cls_cxx_getxattr(hctx, "unwritable", &bl);
  if (r < 0) {
    CLS_ERR("error reading xattr %s: %d", "unwritable", r);
    return r;
  }
  if (string(bl.c_str(), bl.length()) == "1") {
    cout << "osd class: it's unwritable" << std::endl;
    return -EACCES;
  } else{
    return 0;
  }
}

static int check_writable_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "check_writable_op");
  return check_writable(hctx);
}

/**
 * returns -EKEYREJECTED if size is outside of bound, according to comparator.
 *
 * @bound: the limit to test
 * @comparator: should be CEPH_OSD_CMPXATTR_OP_[EQ|GT|LT]
 */
static int assert_size_in_bound(cls_method_context_t hctx, int bound,
    int comparator) {
  //determine size
  bufferlist size_bl;
  int r = cls_cxx_getxattr(hctx, "size", &size_bl);
  if (r < 0) {
    CLS_ERR("error reading xattr %s: %d", "unwritable", r);
    return r;
  }

  int size = atoi(string(size_bl.c_str(), size_bl.length()).c_str());

  //compare size to comparator
  switch (comparator) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    if (size != bound) {
      return -EKEYREJECTED;
    }
    break;
  case CEPH_OSD_CMPXATTR_OP_LT:
    if (size >= bound) {
      return -EKEYREJECTED;
    }
    break;
  case CEPH_OSD_CMPXATTR_OP_GT:
    if (size <= bound) {
      return -EKEYREJECTED;
    }
    break;
  default:
    return -EINVAL;
  }
  return 0;
}

static int assert_size_in_bound_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "assert_size_in_bound_op");
  assert_size_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  return assert_size_in_bound(hctx, op.bound, op.comparator);
}

/**
 * Attempts to insert omap into this object's omap.
 *
 * @return:
 * if unwritable, returns -EACCES.
 * if size > bound and key doesn't already exist in the omap, returns -EBALANCE.
 * if exclusive is true, returns -EEXIST if any keys already exist.
 *
 * @post: object has omap entries inserted, and size xattr is updated
 */
static int omap_insert(cls_method_context_t hctx,
    const map<string, bufferlist> omap, int bound, bool exclusive) {

  //first make sure the object is writable
  int r = check_writable(hctx);
  if (r < 0) {
    return r;
  }

  int assert_bound = bound;

  //if this is an exclusive insert, make sure the key doesn't already exist.

  for (map<string, bufferlist>::const_iterator it = omap.begin();
      it != omap.end(); ++it) {
    bufferlist bl;
    std::map<std::string, bufferlist> kvs;
    r = cls_cxx_map_get_val(hctx, it->first, &bl);
    if (r == 0){
      if (exclusive) {
	return -EEXIST;
      }
      assert_bound--;
    } else if (r != -ENODATA) {
      return r;
    }
  }
  r = assert_size_in_bound(hctx, assert_bound, CEPH_OSD_CMPXATTR_OP_LT);
  if (r < 0) {
    return r;
  }

  bufferlist old_size;
  r = cls_cxx_getxattr(hctx, "size", &old_size);
  if (r < 0) {
    CLS_ERR("error reading xattr %s: %d", "unwritable", r);
    return r;
  }

  int old_size_int = atoi(string(old_size.c_str(), old_size.length()).c_str());
  int new_size_int = old_size_int + omap.size() - (bound - assert_bound);
  bufferlist new_size;
  stringstream s;
  s << new_size_int;
  new_size.append(s.str());

  r = cls_cxx_map_set_vals(hctx, &omap);
  if (r < 0) {
    CLS_ERR("error setting omap: %d", r);
    return r;
  }

  r = cls_cxx_setxattr(hctx, "size", &new_size);
  if (r < 0) {
    CLS_ERR("error setting xattr %s: %d", "unwritable", r);
    return r;
  }

  return 0;
}

static int omap_insert_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "assert_size_in_bound_op");
  omap_set_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  return omap_insert(hctx, op.omap, op.bound, op.exclusive);
}

/**
 * Attempts to remove omap from this object's omap.
 *
 * @return:
 * if unwritable, returns -EACCES.
 * if size < bound and key doesn't already exist in the omap, returns -EBALANCE.
 * if any of the keys are not in this object, returns -ENODATA.
 *
 * @post: object has omap entries removed, and size xattr is updated
 */
static int omap_remove(cls_method_context_t hctx,
    const std::set<string> omap, int bound) {

  //first make sure the object is writable
  int r = check_writable(hctx);
  if (r < 0) {
    return r;
  }

  //fail if removing from an object with only bound entries.
  r = assert_size_in_bound(hctx, bound, CEPH_OSD_CMPXATTR_OP_GT);
  if (r < 0) {
    return r;
  }

  bufferlist old_size;
  r = cls_cxx_getxattr(hctx, "size", &old_size);
  if (r < 0) {
    CLS_ERR("error reading xattr %s: %d", "unwritable", r);
    return r;
  }

  for (std::set<string>::const_iterator it = omap.begin();
      it != omap.end(); ++it) {
    r = cls_cxx_map_remove_key(hctx, *it);
    if (r < 0) {
      CLS_ERR("error setting omap: %d", r);
      return r;
    }
  }

  int old_size_int = atoi(string(old_size.c_str(), old_size.length()).c_str());
  int new_size_int = old_size_int - omap.size();
  bufferlist new_size;
  stringstream s;
  s << new_size_int;
  new_size.append(s.str());

  r = cls_cxx_setxattr(hctx, "size", &new_size);
  if (r < 0) {
    CLS_ERR("error setting xattr %s: %d", "unwritable", r);
    return r;
  }

  return 0;
}

static int omap_remove_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "assert_size_in_bound_op");
  omap_rm_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  return omap_remove(hctx, op.omap, op.bound);
}

/**
 * checks to see if this object needs to be split or rebalanced. if so, reads
 * information about it.
 *
 * @post: if assert_size_in_bound(hctx, bound, comparator) succeeds,
 * odata contains the size, omap, and unwritable attributes for this object.
 * Otherwise, odata contains the size and unwritable attribute.
 */
static int maybe_read_for_balance(cls_method_context_t hctx,
    object_data odata, int bound, int comparator) {
  //if unwritable, return
  int r = check_writable(hctx);
  if (r < 0) {
    odata.unwritable = true;
    return r;
  } else {
    odata.unwritable = false;
  }

  //set the size attribute
  bufferlist size;
  r = cls_cxx_getxattr(hctx, "size", &size);
  if (r < 0) {
    return r;
  }
  odata.size = atoi(string(size.c_str(), size.length()).c_str());

  //check if it needs to be balanced
  r = assert_size_in_bound(hctx, bound, comparator);
  if (r < 0) {
    return r;
  }

  //if the assert succeeded, it needs to be balanced
  r = cls_cxx_map_get_vals(hctx, "", "", LONG_MAX, &odata.omap);
  if (r < 0){
    CLS_LOG(20, "getting kvs failed with error %d", r);
    return r;
  }
  return r;
}

static int maybe_read_for_balance_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "assert_size_in_bound_op");
  rebalance_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  int r = maybe_read_for_balance(hctx, op.odata, op.bound, op.comparator);
  if (r < 0) {
    return r;
  } else {
    op.encode(*out);
    return 0;
  }
}


void __cls_init()
{
  CLS_LOG(20, "Loaded assert condition class!");

  cls_register("kvs", &h_class);
  cls_register_cxx_method(h_class, "get_idata_from_key",
                          CLS_METHOD_RD | CLS_METHOD_PUBLIC,
                          get_idata_from_key_op, &h_get_idata_from_key);
  cls_register_cxx_method(h_class, "get_next_idata",
                          CLS_METHOD_RD | CLS_METHOD_PUBLIC,
                          get_next_idata_op, &h_get_next_idata);
  cls_register_cxx_method(h_class, "get_prev_idata",
                          CLS_METHOD_RD | CLS_METHOD_PUBLIC,
                          get_prev_idata_op, &h_get_prev_idata);
  cls_register_cxx_method(h_class, "check_writable",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
                          check_writable_op, &h_check_writable);
  cls_register_cxx_method(h_class, "assert_size_in_bound",
                          CLS_METHOD_RD | CLS_METHOD_PUBLIC,
                          assert_size_in_bound_op, &h_assert_size_in_bound);
  cls_register_cxx_method(h_class, "omap_insert",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
                          omap_insert_op, &h_omap_insert);
  cls_register_cxx_method(h_class, "omap_remove",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
                          omap_remove_op, &h_omap_remove);
  cls_register_cxx_method(h_class, "maybe_read_for_balance",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
                          maybe_read_for_balance_op, &h_maybe_read_for_balance);

  return;
}
