/*
 * kv_store_test.cc
 *
 *  Created on: Jun 11, 2012
 *      Author: eleanor
 */

#include "test/kv_store_test.h"
#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree.h"
#include "include/rados/librados.hpp"
#include "test/omap_bench.h"

#include <string>
#include <climits>
#include <iostream>
#include <sstream>
#include <cmath>

int KvStoreTest::setup(int argc, const char** argv) {
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
  cout << "rados ok" << std::endl;

  kvs = KvFlatBtree(k,io_ctx);

  return 0;
}

int KvStoreTest::generate_small_non_random_omap(
    std::map<std::string,bufferlist> * out_omap) {
  bufferlist bl;
  int err = 0;

  //setup omap
  for (int i = 0; i < entries; i++) {
  //for (int i = entries - 1; i >= 0; i--) {
    stringstream key;
    stringstream valstrm;
    bufferlist val;
    key << "Key " << i;
    valstrm << "Value " << i;
    val.append(valstrm.str());
    (*out_omap)[key.str()]= val;
  }
  if (err < 0) {
    cout << "generating uniform omap failed - "
	<< "appending random string to omap failed" << std::endl;
  }
  return err;
}

string KvStoreTest::random_string(int len) {
  string ret;
  string alphanum = "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  for (int i = 0; i < len; ++i) {
    ret.push_back(alphanum[rand() % (alphanum.size() - 1)]);
  }

  return ret;
}

int KvStoreTest::generate_random_vector(vector<pair<string, bufferlist> >
    *ret) {
  for (int i = 0; i < entries; i++) {
    bufferlist bfr;
    bfr.append(random_string(7));
    ret->push_back(pair<string,bufferlist>(random_string(5), bfr));
  }
  return 0;
}

int KvStoreTest::test_set_get_rm_one_kv() {
  bufferlist val1;
  bufferlist val2;
  val1.clear();
  val1.append("Value 1");
  val2.clear();
  stringstream two;
  two << "Value 2";
  val2.append(two.str());
  string s(val2.c_str(), val2.length());
  cout << s << std::endl;
  int err = kvs.set("Key 1", val1, true);
  if (err < 0){
    cout << "setting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  err = kvs.set("Key 2", val2, true);
  if (err < 0){
    cout << "setting Key 2 failed with code " << err;
    cout << std::endl;
    return err;
  }
  bufferlist ret1;
  bufferlist ret2;
  err = kvs.get("Key 1", &ret1);
  if (err < 0){
    cout << "getting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  cout << "The value of Key 1 is " << string(ret1.c_str(), ret1.length())
      << std::endl;
  err = kvs.get("Key 2", &ret2);
  if (err < 0){
    cout << "getting Key 2 failed with code " << err;
    cout << std::endl;
    return err;
  }
  cout << "The value of Key 2 is " << string(ret2.c_str(), ret2.length())
      << std::endl;

  bufferlist val3;
  bufferlist ret3;
  val3.append("Value 3");
  err = kvs.set("Key 1", val3, true);
  if (err < 0){
    cout << "resetting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  err = kvs.get("Key 1", &ret3);
  if (err < 0){
    cout << "re-getting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  cout << "The new value of Key 1 is " << string(ret3.c_str(), ret3.length())
      << std::endl;
  cout << kvs.str();

  kvs.remove("Key 1");
  kvs.remove("Key 2");
  return 0;
}

int KvStoreTest::test_many_single_ops() {
  int err = 0;
  std::map<string,bufferlist> kvmap;
  err = generate_small_non_random_omap(&kvmap);
  if (err < 0) {
    cout << "error generating kv map: " << err;
    cout << std::endl;
    return err;
  }
  for (int i = kvmap.size() - 1; i >= 0; i--) {
    stringstream key;
    key << "Key " << i;
    err = kvs.set(key.str(), kvmap[key.str()], true);
  }
  if (err < 0) {
    cout << "error setting kv: " << err;
    cout << std::endl;
    return err;
  }

  std::set<string> key_set;
  err = kvs.get_all_keys(&key_set);
  if (err < 0) {
    cout << "error getting key set: " << err;
    cout << std::endl;
    return err;
  }
  cout << "Keys:" << std::endl;
  for (std::set<string>::iterator it = key_set.begin(); it != key_set.end();
      ++it) {
    cout << "\t" << *it << std::endl;
  }
  cout << std::endl;

  std::map<string,bufferlist> out_kvmap;
  err = kvs.get_all_keys_and_values(&out_kvmap);
  cout << "Keys and values: " << std::endl;
  for (std::map<string,bufferlist>::iterator it = out_kvmap.begin();
      it != out_kvmap.end(); ++it) {
    cout << "\t" << it->first << "\t"
	<< string(it->second.c_str(), it->second.length()) << std::endl;
  }

  cout << kvs.str() << std::endl;

  cout << "removing..." << std::endl;
  kvs.remove("Key 4");
  cout << kvs.str() << std::endl;
  kvs.remove("Key 8");

  //err = kvs.remove(key_set);
  if (err < 0) {
    cout << "error removing keys" << err << std::endl;
   return err;
  }
  return err;
}

int KvStoreTest::test_set_get_kv_map() {
  int err = 0;
  std::map<string,bufferlist> kvmap;
  err = generate_small_non_random_omap(&kvmap);
  if (err < 0) {
    cout << "error generating kv map: " << err;
    cout << std::endl;
    return err;
  }
  err = kvs.set(kvmap, true);
  if (err < 0) {
    cout << "error setting kv map: " << err;
    cout << std::endl;
    return err;
  }
  cout << "done setting everything" << std::endl;
  std::set<string> key_set;
  err = kvs.get_all_keys(&key_set);
  if (err < 0) {
    cout << "error getting key set: " << err;
    cout << std::endl;
    return err;
  }
  cout << "Keys:" << std::endl;
  for (std::set<string>::iterator it = key_set.begin(); it != key_set.end();
      ++it) {
    cout << "\t" << *it << std::endl;
  }
  cout << std::endl;

  std::map<string,bufferlist> out_kvmap;
  err = kvs.get_all_keys_and_values(&out_kvmap);
  cout << "Keys and values: " << std::endl;
  for (std::map<string,bufferlist>::iterator it = out_kvmap.begin();
      it != out_kvmap.end(); ++it) {
    cout << "\t" << it->first << "\t"
	<< string(it->second.c_str(), it->second.length()) << std::endl;
  }

  return err;
}

int KvStoreTest::test_random_insertions() {
  int err = 0;
  vector<pair<string, bufferlist> > map_vector;
  cout << "in the test method" << std::endl;
  err = generate_random_vector(&map_vector);
  if (err < 0) {
    cout << "failed to generate omap: " << err << std::endl;
    return err;
  }
  cout << "generated vector" << std::endl;
  for (int i = 0; i < entries; i++) {
    pair<string,bufferlist> this_pair = map_vector[i];
    kvs.set(this_pair.first, this_pair.second, true);
  }

  return err;
}

int KvStoreTest::test_random_ops() {
  int err = 0;
  map<string,bufferlist> elements;
  vector<pair<string, bufferlist> > map_vector;
  cout << "in the test method" << std::endl;

  for (int i = 0; i < ops; i++) {
    int random = rand() % 3;
    if (elements.size() == 0 || random <= 1) {
      bufferlist bfr;
      string key = random_string(5);
      bfr.append(random_string(7));
      map_vector.push_back(pair<string,bufferlist>(key, bfr));
      elements[key] = bfr;
      kvs.set(key,bfr, true);
    }
    if (elements.size() > 0 && ((int)(elements.size()) >=
	entries || random == 2)){
      string map_key;
      while (elements.count(map_key) == 0) {
	int index = rand() % map_vector.size();
	map_key = map_vector[index].first;
      }
      elements.erase(map_key);
      kvs.remove(map_key);
    }
  }

  return err;
}

int KvStoreTest::test_functionality() {
  cout << "sanity check" << std::endl;
  int err = 0;
  kvs.remove_all();
  //return 0;
  //err = test_set_get_rm_one_kv();
  if (err < 0) {
    cout << "set/getting one value failed with code " << err;
    cout << std::endl;
    return err;
  }

  //err = test_set_get_kv_map();
  //err = test_many_single_ops();
  //err = test_random_insertions();
  err = test_random_ops();
  if (err < 0) {
    cout << "set/getting maps failed with code " << err;
    cout << std::endl;
    return err;
  }
  cout << kvs.str();

  //kvs.remove_all();
  return err;
}

int main(int argc, const char** argv) {
  KvStoreTest kvst;
  cout << "attempting to run" << std::endl;
  kvst.setup(argc, argv);
  cout << "setup successful" << std::endl;
  return kvst.test_functionality();

};
