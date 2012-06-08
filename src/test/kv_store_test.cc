/*
 * kv_store_test.cc
 *
 *  Created on: Jun 11, 2012
 *      Author: eleanor
 */

#include "test/kv_store_test.h"
#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree.h"
#include "key_value_store/kv_flat_btree_async.h"
#include "include/rados/librados.hpp"
#include "test/omap_bench.h"
#include "common/ceph_argparse.h"


#include <string>
#include <climits>
#include <iostream>
#include <sstream>
#include <cmath>

int KvStoreTest::setup(int argc, const char** argv) {
  vector<const char*> args;
  argv_to_vec(argc,argv,args);
  for (unsigned i = 0; i < args.size(); i++) {
    if(i < args.size() - 1) {
      if (strcmp(args[i], "--ops") == 0) {
	ops = atoi(args[i+1]);
      } else if (strcmp(args[i], "-n") == 0) {
	entries = atoi(args[i+1]);
      } else if (strcmp(args[i], "-k") == 0) {
	k = atoi(args[i+1]);
      } else if (strcmp(args[i], "--test") == 0) {
/*	if(strcmp("stress",args[i+1]) == 0) {
	  test = KvStoreTest::stress_tests;
	}
	else if (strcmp("function", args[i+1]) == 0) {
	  test = KvStoreTest::functionality_tests;
	}
*/      } else if (strcmp(args[i], "--name") == 0) {
	rados_id = args[i+1];
      }
    } else if (strcmp(args[i], "--help") == 0) {
      cout << "\nUsage: ostorebench [options]\n"
	   << "Generate latency statistics for a configurable number of "
	   << "key value pair operations of\n"
	   << "configurable size.\n\n"
	   << "OPTIONS\n"
	   << "	--ops           number of operations (default "<<ops;
      cout << ")\n"
	   << "	-n              number of pairs to write (default "<<entries;
      cout << ")\n"
	   << "	-k              each object has k < pairs < 2k (default "
	   << k;
      cout << ")\n"
      	   << "	--test          specify the test suite to run - "
      	   << "stress for stress tests,  function for\n"
      	   << "                        short tests to see if it works at all\n";
 //     	   << "                        (default "<<test;
      cout <<"\n  --name          the rados id to use (default "<<rados_id;
      cout<<")\n";
      exit(1);
    }
  }

  //KvFlatBtree * kvb = new KvFlatBtree(k,io_ctx);
  //kvs = kvb;
  KvFlatBtreeAsync * kvba = new KvFlatBtreeAsync(k, "admin");
  int err = kvba->setup(argc, argv);
  if (err < 0) {
    cout << "error during setup: " << err << std::endl;
    return err;
  }
  kvs = kvba;
  return 0;
}

KvStoreTest::~KvStoreTest() {
  delete kvs;
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
  int err = kvs->set("Key 1", val1, true);
  if (err < 0){
    cout << "setting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if (!kvs->is_consistent()) {
    return -134;
  }
  cout << "first set successful " << std::endl;

  err = kvs->set("Key 2", val2, true);
  if (err < 0){
    cout << "setting Key 2 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if (!kvs->is_consistent()) {
    return -134;
  }
  cout << "second set successful " << std::endl;

  bufferlist ret1;
  bufferlist ret2;
  err = kvs->get("Key 1", &ret1);
  if (err < 0){
    cout << "getting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }

  err = kvs->get("Key 2", &ret2);
  assert(kvs->is_consistent());
  if (err < 0){
    cout << "getting Key 2 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if ("Value 1" == string(ret1.c_str(), ret1.length())
        && "Value 2" == string(ret2.c_str(), ret2.length())) {
    cout << "gets successful" << std::endl;
  }
  else {
    cout << "gets did not return the same value" << std::endl;
    return -134;
  }

  bufferlist val3;
  bufferlist ret3;
  val3.append("Value 3");
  err = kvs->set("Key 1", val3, false);
  if (err == -17){
    cout << "test of exclusive set succeeded" << std::endl;
  }
  err = kvs->set("Key 1", val3, true);
  if (err < 0){
    cout << "resetting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if (!kvs->is_consistent()) {
    return -134;
  }
  err = kvs->get("Key 1", &ret3);
  if (err < 0){
    cout << "re-getting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if ("Value 3" == string(ret3.c_str(), ret3.length())) {
    cout << "Non-exclusive set succeeded" << std::endl;
  } else {
    cout << "Non-exclusive set failed" << std::endl;
    return -134;
  }

  kvs->remove("Key 1");
  if (!kvs->is_consistent()) {
    return -134;
  }
  cout << "passed first removal" << std::endl;
  kvs->remove("Key 2");
  if (!kvs->is_consistent()) {
    return -134;
  }
  cout << "passed removing test" << std::endl;
  return 0;
}

int KvStoreTest::test_split_merge() {
  int err = 0;
  kvs->remove_all();
  if (!kvs->is_consistent()) {
    err = -134;
    return err;
  }
  map<string, bufferlist> data;
  for (int i = 0; i < 2 * k; ++i) {
    stringstream key;
    stringstream valstrm;
    bufferlist val;
    key << "Key " << i;
    valstrm << "Value " << i;
    val.append(valstrm.str().c_str());
    data[key.str()] = val;
    kvs->set(key.str(), val, true);
    if (!kvs->is_consistent()) {
      err = -134;
      return err;
    }
  }

  //we should now have one full node. inserting one more should
  //cause a split.
  stringstream nextkey;
  stringstream nextval;
  bufferlist nextvalbfr;
  nextkey << "Key " << 2 * k + 1;
  nextval << "Value " << 2 * k + 1;
  nextvalbfr.append(nextval.str());
  data[nextkey.str()] = nextvalbfr;
  err = kvs->set(nextkey.str(), nextvalbfr, true);
  if (err < 0) {
    cout << "Split failed with error " << err;
    return err;
  }
  if (!kvs->is_consistent()) {
    cout << "Split failed - not consistent" << std::endl;
    err = -134;
    return err;
  }
  cout << "Passed split test" << std::endl;

  //now removing one key should make it shrink
  stringstream midkey;
  midkey << "Key " << k;
  err = kvs->remove(midkey.str());
  if (err < 0) {
    cout << "Merge failed with error " << err;
    return err;
  }
  if (!kvs->is_consistent()) {
    err = -134;
    cout << "Merge failed - not consistent" << std::endl;
    return err;
  }
  cout << "passed merge test" << std::endl;
  return err;
}

int KvStoreTest::test_non_random_insert_gets() {
  int err = 0;
  bool passed = true;

  //generate key set
  std::map<string,bufferlist> kvmap;
  cout << "generating map";
  for (int i = 0; i < entries; i++) {
  //for (int i = entries - 1; i >= 0; i--) {
    cout << ".";
    cout.flush();
    stringstream key;
    stringstream valstrm;
    bufferlist val;
    key << "Key " << i;
    valstrm << "Value " << i;
    val.append(valstrm.str());
    kvmap[key.str()]= val;
  }

  cout << std::endl << "inserting non random elements";
  //insert them
  for (map<string,bufferlist>::iterator it = kvmap.begin();
      it != kvmap.end(); ++it) {
    cout << ".";
    cout.flush();
    err = kvs->set(it->first, it->second, true);
    if (err < 0) {
      cout << "error setting kv: " << err;
      cout << std::endl;
      return err;
    }
    if (!kvs->is_consistent()) {
      cout << "set failed - not consistent" << std::endl;
      passed = false;
      err = -134;
    }
  }
  cout << "insertions complete." << std::endl;
  cout << "Getting them back";

  //get keys
  std::set<string> key_set;
  err = kvs->get_all_keys(&key_set);
  if (err < 0) {
    cout << "error getting key set: " << err;
    cout << std::endl;
    passed = false;
  }
  int keysleft = kvmap.size();
  for (std::set<string>::iterator it = key_set.begin(); it != key_set.end();
      ++it) {
    cout << ".";
    cout.flush();
    if (kvmap.count(*it) == 0) {
      cout << "there is an object that doesn't belong: " << *it << std::endl;
      passed = false;
      err = -134;
    }
    keysleft--;
  }
  if (keysleft > 0) {
    cout << keysleft << " objects were not inserted." << std::endl;
    passed = false;
    err = -134;
  }
  cout << "getting keys succeeded." << std::endl << "Checking values";

  //get keys and values
  std::map<string,bufferlist> out_kvmap;
  err = kvs->get_all_keys_and_values(&out_kvmap);
  if (!kvs->is_consistent()) {
    cout << "get_all_keys_and_values failed - not consistent."
	<< " this is surprising since that method only reads..." << std::endl;
    passed = false;
    err = -134;
  }
  for (std::map<string,bufferlist>::iterator it = out_kvmap.begin();
      it != out_kvmap.end(); ++it) {
    cout << ".";
    cout.flush();
    if (kvmap.count(it->first) == 0) {
      cout << "get_all_keys_and_values failed - there is an object that"
	  << "doesn't belong: " << it->first << std::endl;
      passed = false;
      err = -134;
    }
    else if (string(kvmap[it->first].c_str(), kvmap[it->first].length())
	!= string(it->second.c_str(), it->second.length())) {
      cout << "get_all_keys_and_values has the wrong value for " << it->first
	  << ": expected "
	  << string(kvmap[it->first].c_str(), kvmap[it->first].length())
	  << " but found " << string(it->second.c_str(), it->second.length())
	  << std::endl;
      passed = false;
      err = -134;
    }
  }
  cout << "checking values successful. Removing..." << std::endl;

  kvs->remove_all();
  if (!kvs->is_consistent()) {
    cout << "remove all failed - not consistent" << std::endl;
    passed = false;
    return -134;
  }

  if (passed) {
    cout << "passed forward insertions. starting backwards test...";
    cout << std::endl;
  }
  else {
    cout << "failed forward insertions/deletions. exiting." << std::endl;
    return err;
  }

  //now start over and do it backwards

  //insert them
  cout << "inserting backwards";
  for (map<string,bufferlist>::reverse_iterator it = kvmap.rbegin();
      it != kvmap.rend(); ++it) {
    cout << ".";
    cout.flush();
    err = kvs->set(it->first, it->second, true);
    if (err < 0) {
      cout << "error setting kv: " << err;
      cout << std::endl;
      return err;
    }
    if (!kvs->is_consistent()) {
      cout << "set failed - not consistent" << std::endl;
      passed = false;
      err = -134;
    }
  }
  cout << "insertions complete. Getting keys" << std::endl;

  //get keys
  key_set.clear();
  err = kvs->get_all_keys(&key_set);
  if (err < 0) {
    cout << "error getting key set: " << err;
    cout << std::endl;
    passed = false;
  }
  keysleft = kvmap.size();
  for (std::set<string>::iterator it = key_set.begin(); it != key_set.end();
      ++it) {
    cout << ".";
    cout.flush();
    if (kvmap.count(*it) == 0) {
      cout << "there is an object that doesn't belong: " << *it << std::endl;
      passed = false;
      err = -134;
    }
    keysleft--;
  }
  if (keysleft > 0) {
    cout << keysleft << " objects were not inserted." << std::endl;
    passed = false;
    err = -134;
  }
  cout << "getting keys succeeded." << std::endl << "Checking values";

  //get keys and values
  out_kvmap.clear();
  err = kvs->get_all_keys_and_values(&out_kvmap);
  if (!kvs->is_consistent()) {
    cout << "get_all_keys_and_values failed - not consistent."
	<< " this is surprising since that method only reads..." << std::endl;
    passed = false;
    err = -134;
  }
  for (std::map<string,bufferlist>::iterator it = out_kvmap.begin();
      it != out_kvmap.end(); ++it) {
    cout << ".";
    cout.flush();
    if (kvmap.count(it->first) == 0) {
      cout << "get_all_keys_and_values failed - there is an object that"
	  << "doesn't belong: " << it->first << std::endl;
      passed = false;
      err = -134;
    }
    else if (string(kvmap[it->first].c_str(), kvmap[it->first].length())
	!= string(it->second.c_str(), it->second.length())) {
      cout << "get_all_keys_and_values has the wrong value for " << it->first
	  << ": expected "
	  << string(kvmap[it->first].c_str(), kvmap[it->first].length())
	  << " but found " << string(it->second.c_str(), it->second.length())
	  << std::endl;
      passed = false;
      err = -134;
    }
  }
  cout << "getting values complete. Removing them..." << std::endl;

  //remove them in reverse order
  for (map<string,bufferlist>::reverse_iterator it = kvmap.rbegin();
      it != kvmap.rend(); ++it) {
    err = kvs->remove(it->first);
    if (err < 0) {
      cout << "error removing key: " << err;
      cout << std::endl;
      return err;
    }
    if (!kvs->is_consistent()) {
      cout << "remove failed - not consistent" << std::endl;
      passed = false;
      err = -134;
    }
  }

  if (passed) cout << "testing many key/values successful!" << std::endl;
  else cout << "testing many keys/values failed" << std::endl;
  return err;
}

int KvStoreTest::test_random_insertions() {
  int err = 0;
  vector<pair<string, bufferlist> > map_vector;
  for (int i = 0; i < entries; i++) {
      bufferlist bfr;
      bfr.append(random_string(7));
      map_vector.push_back(pair<string,bufferlist>(random_string(5), bfr));
    }
  cout << "testing random insertions";
  for (int i = 0; i < entries; i++) {
    cout << ".";
    cout.flush();
    pair<string,bufferlist> this_pair = map_vector[i];
    kvs->set(this_pair.first, this_pair.second, true);
    if (!kvs->is_consistent()) {
      cout << "Random insertions failed - not consistent" << std::endl;
      return -134;
    }
  }
  cout << "random insertions successful" << std::endl;

  return err;
}

int KvStoreTest::test_random_ops() {
  int err = 0;
  map<string,bufferlist> elements;
  vector<pair<string, bufferlist> > map_vector;
  cout << "testing random ops";

  for (int i = 0; i < ops; i++) {
    cout << ".";
    cout.flush();
    int random = rand() % 3;
    if (elements.size() == 0 || random <= 1) {
      bufferlist bfr;
      string key = random_string(5);
      bfr.append(random_string(7));
      map_vector.push_back(pair<string,bufferlist>(key, bfr));
      elements[key] = bfr;
      if (!kvs->is_consistent()) {
        return -134;
      }
      kvs->set(key,bfr, true);
      if (!kvs->is_consistent()) {
        return -134;
      }
    }
    if (elements.size() > 0 && ((int)(elements.size()) >=
	entries || random == 2)){
      string map_key;
      while (elements.count(map_key) == 0) {
	int index = rand() % map_vector.size();
	map_key = map_vector[index].first;
      }
      elements.erase(map_key);
      if (!kvs->is_consistent()) {
        return -134;
      }
      kvs->remove(map_key);
      if (!kvs->is_consistent()) {
        return -134;
      }
    }
  }

  cout << std::endl << "passed random ops test" << std::endl;

  return err;
}

int KvStoreTest::functionality_tests() {
  int err = 0;
  //kvs->remove_all();
  /*if (!kvs->is_consistent()) {
    return -134;
  }*/
  cout << "initial remove all successful" << std::endl;
  err = test_set_get_rm_one_kv();
  if (err < 0) {
    cout << "set/getting one value failed with code " << err;
    cout << std::endl;
    return err;
  }
  //err = test_split_merge();
  if (err < 0) {
    cout << "split/merge test failed with code " << err << std::endl;
    return err;
  }
  return err;
}

int KvStoreTest::stress_tests() {
  int err = 0;
  kvs->remove_all();
  err = test_non_random_insert_gets();
  if (err < 0) {
    cout << "non-random inserts and gets failed with code " << err;
    cout << std::endl;
    return err;
  }
  err = test_random_insertions();
  if (err < 0) {
    cout << "random insertions test failed with code " << err;
    cout << std::endl;
    return err;
  }
  err = test_random_ops();
  if (err < 0) {
    cout << "random ops test failed with code " << err;
    cout << std::endl;
    return err;
  }
  return err;
}

int main(int argc, const char** argv) {
  KvStoreTest kvst;
  int err = kvst.setup(argc, argv);
  if (err == 0) cout << "setup successful" << std::endl;
  else return err;
  err = kvst.functionality_tests();
  //if (err < 0) return err;
  //kvst.stress_tests();
  return 0;
};
