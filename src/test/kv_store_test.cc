/*
 * kv_store_test.cc
 *
 *  Created on: Jun 11, 2012
 *      Author: eleanor
 */

#include "test/kv_store_test.h"
#include "key_value_store/key_value_structure.h"
#include "key_value_store/kv_flat_btree_async.h"
#include "include/rados/librados.hpp"
#include "test/omap_bench.h"
#include "common/ceph_argparse.h"


#include <string>
#include <climits>
#include <iostream>
#include <sstream>
#include <cmath>

void StopWatch::start_time() {
  begin_time = ceph_clock_now(g_ceph_context);
}
void StopWatch::stop_time() {
  end_time = ceph_clock_now(g_ceph_context);
}
double StopWatch::get_time() {
  return (end_time - begin_time) * 1000;
}

void StopWatch::clear() {
  begin_time = end_time = utime_t();
}

KvStoreTest::KvStoreTest()
: k(2),
  wait_time(100000),
  entries(30),
  ops(10),
  clients(5),
  increment(10),
  key_size(5),
  val_size(7),
  inject('n'),
  kvs(NULL),
  client_name("admin")
{
  probs[25] = 'i';
  probs[50] = 'u';
  probs[75] = 'd';
  probs[100] = 'r';
}

int KvStoreTest::setup(int argc, const char** argv) {
  vector<const char*> args;
  argv_to_vec(argc,argv,args);
  for (unsigned i = 0; i < args.size(); i++) {
    if(i < args.size() - 1) {
      if (strcmp(args[i], "--ops") == 0) {
	ops = atoi(args[i+1]);
      } else if (strcmp(args[i], "-n") == 0) {
	entries = atoi(args[i+1]);
      } else if (strcmp(args[i], "--kval") == 0) {
	k = atoi(args[i+1]);
      } else if (strcmp(args[i], "--keysize") == 0) {
	key_size = atoi(args[i+1]);
      } else if (strcmp(args[i], "--valsize") == 0) {
	val_size = atoi(args[i+1]);
      } else if (strcmp(args[i], "-t") == 0) {
	clients = atoi(args[i+1]);
      } else if (strcmp(args[i], "--inc") == 0) {
	increment = atoi(args[i+1]);
      } else if (strcmp(args[i], "--inj") == 0) {
	if(strcmp("d",args[i+1]) == 0) {
	  inject = 's';
	}
	else if (strcmp("w", args[i+1]) == 0) {
	  if (args.size() - 1 > i + 1) {
	    wait_time = atoi(args[i+2]);
	  }
	  inject = 'w';
	}
      } else if (strcmp(args[i], "-d") == 0) {
	if (i + 3 < args.size() - 1) {
	  cout << "Invalid arguments after -d: there must be 4 of them."
	      << std::endl;
	  continue;
	} else {
	  probs.clear();
	  int sum = atoi(args[i + 1]);
	  probs[sum] = 'i';
	  sum += atoi(args[i + 2]);
	  probs[sum] = 'u';
	  sum += atoi(args[i + 3]);
	  probs[sum] = 'd';
	  sum += atoi(args[i + 4]);
	  probs[sum] = 'r';
	  if (sum != 100) {
	    cout << "Invalid arguments after -d: they must add to 100."
		<< std::endl;
	  }
	}
      } else if (strcmp(args[i], "--name") == 0) {
	client_name = args[i+1];
      }
    } else if (strcmp(args[i], "--help") == 0) {
      cout << "\nUsage: kvstoretest [options]\n"
	   << "Generate latency statistics for a configurable number of "
	   << "key value pair operations of\n"
	   << "configurable size.\n\n"
	   << "OPTIONS\n"
	   << " --name                                 client name (default"
	   << " admin\n"
	   << "	--ops                                  number of operations "
	   << "(default "<< ops << ")\n"
	   << "	-n                                     number of initial pairs "
	   <<  "to write (default " << entries << ")\n"
	   << "	--kval                                 each object has k < "
	   << "pairs < 2k (default " << k << ")\n"
	   << "	--keysize                              number of characters "
	   << "per key (default " << key_size << ")\n"
      	   << "	--valsize                              number of characters "
      	   << "per value (default " << val_size << ")\n"
	   << " -t                                     the number of clients to"
	   << " run at once for stress test. (default " << clients << ")\n"
	   << " -i                                     type of interruption "
	   << "(default none, 'd' for death, 'w' <time> for wait time ms \n"
	   << "                                        (default 1000))\n"
	   << " -d <insert> <update> <delete> <read>   distribution to use "
	   << "(default 25-25-25-25), where <insert>, etc. are percents of"
	   << " ops. these must add to 100.\n"
	   << std::endl;
      exit(1);
    }
  }

  KvFlatBtreeAsync * kvba = new KvFlatBtreeAsync(k, client_name);
  kvs = kvba;
  switch (inject) {
  case 'w':
    kvs->set_inject(&KeyValueStructure::wait, wait_time);
    break;
  case 's':
    kvs->set_inject(&KeyValueStructure::suicide, wait_time);
    break;
  default:
    kvs->set_inject(&KeyValueStructure::nothing, wait_time);
    break;
  }


  librados::Rados rados;
  string rados_id("admin");
  string pool_name("data");
  librados::IoCtx io_ctx;
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


  int err = kvs->setup(argc, argv);
  if (err < 0 && err != -17) {
    cout << "error during setup of kvs: " << err << std::endl;
    return err;
  }

  srand(time(NULL));

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

pair<string, bufferlist> KvStoreTest::rand_distr(bool new_elem) {
  pair<string, bufferlist> ret;
  if (new_elem) {
    ret = make_pair(random_string(key_size),
	KvFlatBtreeAsync::to_bl(random_string(val_size)));
  } else {
    ret.first = key_map[rand() % key_map.size()];
    ret.second = KvFlatBtreeAsync::to_bl(random_string(val_size));
  }
  return ret;
}

int KvStoreTest::test_set_get_rm_one_kv() {
  bufferlist val1;
  bufferlist val2;
  val1.append("Value 1");
  val2.append("Value 2");
  int err = kvs->set("Key 1", val1, true);
  if (err < 0){
    cout << "[x] setting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if (!kvs->is_consistent()) {
    return -EINCONSIST;
  }
  cout << "[v] first set successful " << std::endl;

  err = kvs->set("Key 2", val2, true);
  if (err < 0){
    cout << "[x] setting Key 2 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if (!kvs->is_consistent()) {
    return -EINCONSIST;
  }
  cout << "[v] second set successful " << std::endl;

  bufferlist ret1;
  bufferlist ret2;
  err = kvs->get("Key 1", &ret1);
  if (err < 0){
    cout << "[x] getting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }

  err = kvs->get("Key 2", &ret2);
  assert(kvs->is_consistent());
  if (err < 0){
    cout << "[x] getting Key 2 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if ("Value 1" == string(ret1.c_str(), ret1.length())
        && "Value 2" == string(ret2.c_str(), ret2.length())) {
    cout << "[v] gets successful" << std::endl;
  }
  else {
    cout << "[x] gets did not return the same value" << std::endl;
    return -EINCONSIST;
  }

  bufferlist val3;
  bufferlist ret3;
  val3.append("Value 3");
  err = kvs->set("Key 1", val3, false);
  if (err == -17){
    cout << "[v] test of exclusive set succeeded" << std::endl;
  }
  err = kvs->set("Key 1", val3, true);
  if (err < 0){
    cout << "[x] resetting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if (!kvs->is_consistent()) {
    return -EINCONSIST;
  }
  err = kvs->get("Key 1", &ret3);
  if (err < 0){
    cout << "[x] re-getting Key 1 failed with code " << err;
    cout << std::endl;
    return err;
  }
  if ("Value 3" == string(ret3.c_str(), ret3.length())) {
    cout << "[v] Non-exclusive set succeeded" << std::endl;
  } else {
    cout << "[x] Non-exclusive set failed: "
	<< "expected get(\"Key 1\") to return Value 3 but found "
	<< string(ret3.c_str(), ret3.length()) << std::endl;
    cout << kvs->str() << std::endl;
    return -EINCONSIST;
  }

  kvs->remove("Key 1");
  if (!kvs->is_consistent()) {
    return -EINCONSIST;
  }
  cout << "[v] passed first removal" << std::endl;
  kvs->remove("Key 2");
  if (!kvs->is_consistent()) {
    return -EINCONSIST;
  }
  cout << "[v] passed removing test" << std::endl;
  return 0;
}

int KvStoreTest::test_split_merge() {
  int err = 0;
  kvs->remove_all();
  if (!kvs->is_consistent()) {
    err = -EINCONSIST;
    return err;
  }
  map<string, bufferlist> data;
  for (int i = k; i < 3 * k; ++i) {
    stringstream key;
    stringstream valstrm;
    bufferlist val;
    key << "Key " << i;
    valstrm << "Value " << i;
    val.append(valstrm.str().c_str());
    data[key.str()] = val;
    kvs->set(key.str(), val, true);
    if (!kvs->is_consistent()) {
      err = -EINCONSIST;
      return err;
    }
  }

  //we should now have one full node. inserting one more should
  //cause a split.
  stringstream nextkey;
  stringstream nextval;
  bufferlist nextvalbfr;
  nextkey << "Key " << 0;
  nextval << "Value " << 0;
  nextvalbfr.append(nextval.str());
  data[nextkey.str()] = nextvalbfr;
  err = kvs->set(nextkey.str(), nextvalbfr, true);
  if (err < 0) {
    cout << "[x] Split failed with error " << err;
    return err;
  }
  if (!kvs->is_consistent()) {
    cout << "[x] Split failed - not consistent" << std::endl;
    err = -EINCONSIST;
    return err;
  }
  cout << "[v] Passed split test" << std::endl;

  for (int i = 1; i < k; ++i) {
    stringstream key;
    stringstream valstrm;
    bufferlist val;
    key << "Key " << i;
    valstrm << "Value " << i;
    val.append(valstrm.str().c_str());
    data[key.str()] = val;
    kvs->set(key.str(), val, true);
    if (!kvs->is_consistent()) {
      err = -EINCONSIST;
      return err;
    }
  }

  //now removing one key should make it rebalance...
  cout << kvs->str() << std::endl;
  stringstream midkey;
  midkey << "Key " << 4;
  err = kvs->remove(midkey.str());
  if (err < 0) {
    cout << "[x] Rebalance failed with error " << err;
    return err;
  }
  if (!kvs->is_consistent()) {
    err = -EINCONSIST;
    cout << "[x] Rebalance failed - not consistent" << std::endl;
    return err;
  }
  cout << kvs->str() << std::endl;
  cout << "[v] passed rebalance test" << std::endl;

  //keep removing keys until they have to merge...
  for (int i = 3 * k; i > 2 * k; --i) {
    stringstream key;
    key << "Key " << i;
    err = kvs->remove(key.str());
    if (err < 0 && err != -1) {
      cout << "[x] Merge failed with error " << err << std::endl;
      return err;
    }
    if (!kvs->is_consistent()) {
      err = -EINCONSIST;
      cout << "[x] Merge failed with error" << err << std::endl;
      return err;
    }
  }
  cout << kvs->str() << std::endl;
  cout << "[v] passed merge test" << std::endl;
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
      err = -EINCONSIST;
    }
  }
  cout << "insertions complete." << std::endl;
  cout << "Getting them back" << std::endl;

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
      err = -EINCONSIST;
    }
    keysleft--;
  }
  if (keysleft > 0) {
    cout << keysleft << " objects were not inserted." << std::endl;
    passed = false;
    err = -EINCONSIST;
  }
  cout << "getting keys succeeded." << std::endl << "Checking values";

  //get keys and values
  std::map<string,bufferlist> out_kvmap;
  err = kvs->get_all_keys_and_values(&out_kvmap);
  if (!kvs->is_consistent()) {
    cout << "get_all_keys_and_values failed - not consistent."
	<< " this is surprising since that method only reads..." << std::endl;
    passed = false;
    err = -EINCONSIST;
  }
  for (std::map<string,bufferlist>::iterator it = out_kvmap.begin();
      it != out_kvmap.end(); ++it) {
    cout << ".";
    cout.flush();
    if (kvmap.count(it->first) == 0) {
      cout << "get_all_keys_and_values failed - there is an object that"
	  << "doesn't belong: " << it->first << std::endl;
      passed = false;
      err = -EINCONSIST;
    }
    else if (string(kvmap[it->first].c_str(), kvmap[it->first].length())
	!= string(it->second.c_str(), it->second.length())) {
      cout << "get_all_keys_and_values has the wrong value for " << it->first
	  << ": expected "
	  << string(kvmap[it->first].c_str(), kvmap[it->first].length())
	  << " but found " << string(it->second.c_str(), it->second.length())
	  << std::endl;
      passed = false;
      err = -EINCONSIST;
    }
  }
  cout << "checking values successful. Removing..." << std::endl;

  kvs->remove_all();
  if (!kvs->is_consistent()) {
    cout << "remove all failed - not consistent" << std::endl;
    passed = false;
    return -EINCONSIST;
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
      err = -EINCONSIST;
    }
  }
  cout << "insertions complete." << std::endl << "Getting keys";

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
      err = -EINCONSIST;
    }
    keysleft--;
  }
  if (keysleft > 0) {
    cout << keysleft << " objects were not inserted." << std::endl;
    passed = false;
    err = -EINCONSIST;
  }
  cout << "getting keys succeeded." << std::endl << "Checking values";

  //get keys and values
  out_kvmap.clear();
  err = kvs->get_all_keys_and_values(&out_kvmap);
  if (!kvs->is_consistent()) {
    cout << "get_all_keys_and_values failed - not consistent."
	<< " this is surprising since that method only reads..." << std::endl;
    passed = false;
    err = -EINCONSIST;
  }
  for (std::map<string,bufferlist>::iterator it = out_kvmap.begin();
      it != out_kvmap.end(); ++it) {
    cout << ".";
    cout.flush();
    if (kvmap.count(it->first) == 0) {
      cout << "get_all_keys_and_values failed - there is an object that"
	  << "doesn't belong: " << it->first << std::endl;
      passed = false;
      err = -EINCONSIST;
    }
    else if (string(kvmap[it->first].c_str(), kvmap[it->first].length())
	!= string(it->second.c_str(), it->second.length())) {
      cout << "get_all_keys_and_values has the wrong value for " << it->first
	  << ": expected "
	  << string(kvmap[it->first].c_str(), kvmap[it->first].length())
	  << " but found " << string(it->second.c_str(), it->second.length())
	  << std::endl;
      passed = false;
      err = -EINCONSIST;
    }
  }
  cout << std::endl << "getting values complete. Removing them...";

  //remove them in reverse order
  for (map<string,bufferlist>::reverse_iterator it = kvmap.rbegin();
      it != kvmap.rend(); ++it) {
    cout << ".";
    cout.flush();
    err = kvs->remove(it->first);
    if (err < 0) {
      cout << "error removing key: " << err;
      cout << std::endl;
      return err;
    }
    if (!kvs->is_consistent()) {
      cout << "remove failed - not consistent" << std::endl;
      passed = false;
      err = -EINCONSIST;
    }
  }
  cout << std::endl;

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
    cout << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tinitial:" << i + 1
	<< " / " << entries << std::endl;
    cout << ".";
    cout.flush();
    pair<string,bufferlist> this_pair = map_vector[i];
    err = kvs->set(this_pair.first, this_pair.second, true);
    if (err < 0) {
      cout << "setting " << this_pair.first << " failed with " << err
	  << std::endl;
      return err;
    }
/*    if (!kvs->is_consistent()) {
      cout << "Random insertions failed - not consistent" << std::endl;
      return -EINCONSIST;
    }*/
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
        return -EINCONSIST;
      }
      kvs->set(key,bfr, true);
      if (!kvs->is_consistent()) {
        return -EINCONSIST;
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
        return -EINCONSIST;
      }
      kvs->remove(map_key);
      if (!kvs->is_consistent()) {
        return -EINCONSIST;
      }
    }
  }

  cout << std::endl << "passed random ops test" << std::endl;

  return err;
}

void *KvStoreTest::pset(void *ptr){
  struct set_args *args = (struct set_args *)ptr;
  args->sw.start_time();
  args->kvs->set((string)args->key, (bufferlist)args->val, true);
  args->sw.stop_time();
  return NULL;
}

void *KvStoreTest::prm(void *ptr) {
  struct rm_args *args = (struct rm_args *)ptr;
  args->sw.start_time();
  args->kvs->remove((string)args->key);
  args->sw.stop_time();
  return NULL;
}

int KvStoreTest::test_verify_concurrent_sets(int argc, const char** argv) {
  int err = 0;
  int wait_size_0 = 10;
  int wait_size_1 = 10;
  vector<__useconds_t> waits0(wait_size_0,(__useconds_t)0);
  vector<__useconds_t> waits1(wait_size_1,(__useconds_t)0);
  struct set_args set_args0;
  struct set_args set_args1;
  KeyValueStructure * kvs0 = new KvFlatBtreeAsync(k,"client0", waits0);
  KeyValueStructure * kvs1 = new KvFlatBtreeAsync(k,"client1", waits1);
  kvs0->setup(argc, argv);
  kvs1->setup(argc, argv);


  for (int i = 0; i < wait_size_0; i++) {
    if (i > 0) {
      waits0[i-1] = 0;
    }
    waits0[i] = wait_time;
    for (int j = 0; j < wait_size_1; j++) {
      if (j > 0) {
        waits1[j-1] = 0;
      }
      cout << "\t\t\t\t\t\t\t\t\t\t\t\t\t" << j + i*wait_size_1 << " / "
	  << wait_size_0 * wait_size_1 << std::endl;
      waits1[j] = wait_time;
      pthread_t thread0;
      pthread_t thread1;

      //kvs0->set_waits(waits0);
      kvs1->set_waits(waits1);
      set_args0.kvs = kvs0;
      set_args1.kvs = kvs1;
      set_args0.key = random_string(5);
      set_args1.key = random_string(5);
      set_args0.val = KvFlatBtreeAsync::
	  to_bl(random_string(7));
      set_args1.val = KvFlatBtreeAsync::
	  to_bl(random_string(7));

      err = pthread_create(&thread0, NULL,
	  pset, (void*)&set_args0);
      if (err < 0) {
	cout << "error creating first pthread: " << err << std::endl;
	return err;
      }
      err = pthread_create(&thread1, NULL, pset, (void*)&set_args1);
      if (err < 0) {
	cout << "error creating second pthread: " << err << std::endl;
	return err;
      }
      void *status0;
      void *status1;
      cout << "waiting to join writer of Key " << i << std::endl;
      err = pthread_join(thread0, &status0);
      if (err < 0) {
	cout << "error joining first pthread: " << err << std::endl;
	return err;
      }
      cout << "waiting to join writer of Key " << 9 + j << std::endl;
      err = pthread_join(thread1, &status1);
      if (err < 0) {
	cout << "error joining second pthread: " << err << std::endl;
	return err;
      }
      cout << "checking consistency" << std::endl;
      if (!kvs->is_consistent()) {
	return -EINCONSIST;
      }
    }
  }
  cout << kvs0->str();
  delete kvs0;
  delete kvs1;
  return err;
}

int KvStoreTest::test_verify_concurrent_set_rms(int argc, const char** argv){
  int err = 0;
  int wait_size_0 = 10;
  int wait_size_1 = 15;
  vector<__useconds_t> waits0(wait_size_0,(__useconds_t)0);
  vector<__useconds_t> waits1(wait_size_1,(__useconds_t)0);
  struct set_args set_args0;
  struct rm_args set_args1;
  KeyValueStructure * kvs0 = new KvFlatBtreeAsync(k,"client0", waits0);
  KeyValueStructure * kvs1 = new KvFlatBtreeAsync(k,"client1", waits1);
  kvs0->setup(argc, argv);
  kvs1->setup(argc, argv);

  for (int i = 0; i < wait_size_0; i++) {
    if (i > 0) {
      waits0[i-1] = 0;
    }
    waits0[i] = wait_time;
    for (int j = 0; j < wait_size_1; j++) {
      if (j > 0) {
        waits1[j-1] = 0;
      }
      cout << "\t\t\t\t\t\t\t\t\t\t\t\t\t" << j + i*wait_size_1 << " / "
	  << wait_size_0 * wait_size_1 << std::endl;
      waits1[j] = wait_time;
      pthread_t thread0;
      pthread_t thread1;

      //kvs0->set_waits(waits0);
      kvs1->set_waits(waits1);
      set_args0.kvs = kvs0;
      set_args1.kvs = kvs1;
      set_args0.key = KvFlatBtreeAsync::to_string("Key ",i);
      set_args1.key = KvFlatBtreeAsync::to_string("Key ",i - 2);
      set_args0.val = KvFlatBtreeAsync::
	  to_bl(KvFlatBtreeAsync::to_string("Value ",i));

      err = pthread_create(&thread0, NULL,
	  pset, (void*)&set_args0);
      if (err < 0) {
	cout << "error creating first pthread: " << err << std::endl;
	return err;
      }
      err = pthread_create(&thread1, NULL, prm, (void*)&set_args1);
      if (err < 0) {
	cout << "error creating second pthread: " << err << std::endl;
	return err;
      }
      void *status0;
      void *status1;
      cout << "waiting to join writer of Key " << i << std::endl;
      err = pthread_join(thread0, &status0);
      if (err < 0) {
	cout << "error joining first pthread: " << err << std::endl;
	return err;
      }
      cout << "waiting to join writer of Key " << 9 + j << std::endl;
      err = pthread_join(thread1, &status1);
      if (err < 0) {
	cout << "error joining second pthread: " << err << std::endl;
	return err;
      }
      cout << "checking consistency" << std::endl;
      if (!kvs->is_consistent()) {
	return -EINCONSIST;
      }
    }
  }
  cout << kvs0->str();
  delete kvs0;
  delete kvs1;
  return err;
}

int KvStoreTest::test_verify_random_set_rms(int argc, const char** argv) {
  int err = 0;
  int wait_size_0 = 10;
  int wait_size_1 = 15;
  vector<__useconds_t> waits0(wait_size_0,(__useconds_t)0);
  vector<__useconds_t> waits1(wait_size_1,(__useconds_t)0);
  struct set_args set_args0;
  struct rm_args rm_args1;
  KeyValueStructure * kvs0 = new KvFlatBtreeAsync(k,"client0", waits0);
  KeyValueStructure * kvs1 = new KvFlatBtreeAsync(k,"client1", waits1);
  kvs0->setup(argc, argv);
  kvs1->setup(argc, argv);
  std::set<int> keys;
  map<int, pair<string, bufferlist> > bigmap;

  int count = 0;
  pthread_t thread[10];
  KeyValueStructure * kvbas[10];
  struct set_args init_set_args[10];

  for(int i = 0; i < 10; i++) {
    vector<__useconds_t> wait(wait_size_0 * 1000,(__useconds_t)0);
    string name = KvFlatBtreeAsync::to_string("prados",i);
    cout << "name is " << name << std::endl;
    kvbas[i] = new KvFlatBtreeAsync(2, name, wait);
    kvbas[i]->setup(argc, argv);
  }

  for (int i = 0; i < 10; i++) {
    cout << i << std::endl;
    pair<string, bufferlist> this_pair;
    this_pair.first = random_string(5);
    this_pair.second = KvFlatBtreeAsync::to_bl(random_string(7));
    cout << this_pair.first << std::endl;
    bigmap[count] = this_pair;
    cout << bigmap[count].first << std::endl;
    keys.insert(count);
    init_set_args[i].kvs = kvbas[i];
    init_set_args[i].key = bigmap[count].first;
    init_set_args[i].val = bigmap[count].second;
    cout << i << " set to insert " << bigmap[count].first << std::endl;
    err = pthread_create(&thread[i], NULL, pset, (void*)&init_set_args[i]);
    if (err < 0) {
	cout << "error creating first pthread: " << err << std::endl;
	return err;
    }
    count++;
  }
  //return err;

  for (int i = 0; i < 10; i++) {
    void *status;
    int err = pthread_join(thread[i], &status);
    if (err < 0) {
	cout << "error joining first pthread: " << err << std::endl;
	return err;
    }
    if (i == 2) cout << kvbas[i]->str();
    delete kvbas[i];
  }
  cout << "survived" << std::endl;
  //return 0;

  for (int i = 0; i < wait_size_0; i++) {
    if (i > 0) {
      waits0[i-1] = 0;
    }
    waits0[i] = wait_time;
    for (int j = 0; j < wait_size_1; j++) {
      if (i > 0) {
        waits1[j-1] = 0;
      }
      cout << "\t\t\t\t\t\t\t\t\t\t\t\t\t" << j + i*wait_size_1 << " / "
	  << wait_size_0 * wait_size_1 << std::endl;
      waits1[j] = wait_time;
      pthread_t thread0;
      pthread_t thread1;

      kvs0->set_waits(waits0);
      kvs1->set_waits(waits1);
      set_args0.kvs = kvs0;
      rm_args1.kvs = kvs1;

      bigmap[count] = pair<string, bufferlist>(random_string(5),
	  KvFlatBtreeAsync::to_bl(random_string(7)));
      keys.insert(count);
      set_args0.key = bigmap[count].first;
      set_args0.val = bigmap[count].second;
      cout << "kvs0 set to insert " << bigmap[count].first << std::endl;
      int rm_int = -1;
      while(keys.count(rm_int) == 0) {
	rm_int = rand() % count;
      }
      rm_args1.key = bigmap[rm_int].first;
      cout << "kvs1 is set to remove " << rm_args1.key << std::endl;
      assert(keys.count(rm_int) > 0);
      keys.erase(rm_int);
      bigmap.erase(rm_int);


      err = pthread_create(&thread0, NULL,
	  pset, (void*)&set_args0);
      if (err < 0) {
	cout << "error creating first pthread: " << err << std::endl;
	return err;
      }
      err = pthread_create(&thread1, NULL, prm, (void*)&rm_args1);
      if (err < 0) {
	cout << "error creating second pthread: " << err << std::endl;
	return err;
      }
      void *status0;
      void *status1;
      cout << "waiting to join writer of " << bigmap[count].first
	  << std::endl;
      err = pthread_join(thread0, &status0);
      if (err < 0) {
	cout << "error joining first pthread: " << err << std::endl;
	return err;
      }
      cout << "waiting to join remover of " << rm_args1.key << std::endl;
      err = pthread_join(thread1, &status1);
      if (err < 0) {
	cout << "error joining second pthread: " << err << std::endl;
	return err;
      }
      cout << "checking consistency" << std::endl;
      if (!kvs->is_consistent()) {
	return -EINCONSIST;
      }
      count++;
    }
  }
  cout << kvs0->str();
  return err;
}

int KvStoreTest::test_stress_random_set_rms(int argc, const char** argv) {
  int err = 0;
  pthread_t real_threads[clients];
  int set_size = ((int)ceil(clients / 2.0));
  set_args set_args_ar[set_size];
  int rm_size = ((int)floor(clients / 2.0));
  rm_args rm_args_ar[rm_size];
  KeyValueStructure * kvs_arr[clients];
  void *status[clients];
  map<int, pair<string, bufferlist> > bigmap;
  StopWatch * sws[clients];
  StopWatch suicide_watch;

  //setup initial objects
  int count = 0;
  pthread_t thread[entries];
  KeyValueStructure * kvbas[entries];
  struct set_args init_set_args[entries];

  for(int i = 0; i < 10; i++) {
    string name = KvFlatBtreeAsync::to_string("prados",i);
    cout << "name is " << name << std::endl;
    kvbas[i] = new KvFlatBtreeAsync(2, name);
    kvbas[i]->setup(argc, argv);
  }

  srand(time(NULL));

  for (int i = 0; i < 10; i++) {
    cout << i << std::endl;
    pair<string, bufferlist> this_pair;
    this_pair.first = random_string(5);
    this_pair.second = KvFlatBtreeAsync::to_bl(random_string(7));
    cout << this_pair.first << std::endl;
    bigmap[count] = this_pair;
    cout << bigmap[count].first << std::endl;
    init_set_args[i].kvs = kvbas[i];
    init_set_args[i].key = bigmap[count].first;
    init_set_args[i].val = bigmap[count].second;
    cout << i << " set to insert " << bigmap[count].first << std::endl;
    err = pthread_create(&thread[i], NULL, pset, (void*)&init_set_args[i]);
    if (err < 0) {
	cout << "error creating first pthread: " << err << std::endl;
	return err;
    }
    count++;
  }

  for (int i = 0; i < 10; i++) {
    void *status;
    int err = pthread_join(thread[i], &status);
    if (err < 0) {
	cout << "error joining first pthread: " << err << std::endl;
	return err;
    }
    if (i != 9) {
      delete kvbas[i];
    }
  }
  assert(kvbas[9]->is_consistent());
  delete kvbas[9];

  //setup kvs
  for(int i = 0; i < clients; i++) {
    kvs_arr[i] = new KvFlatBtreeAsync(k, KvFlatBtreeAsync::to_string("client",i));
    err = kvs_arr[i]->setup(argc, argv);
    if (err < 0 && err != -17) {
      cout << "error setting up client " << i << ": " << err << std::endl;
      return err;
    }
  }

  //tests
  for (int i = 0; i < ops; i++) {
    cout << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t" << i << " / "
	<< ops << std::endl;
    int set_index = 0;
    int rm_index = 0;

    for (int j = 0; j < clients; j++) {
      if (j % 2 == 0) {
	set_args_ar[set_index].kvs = kvs_arr[j];
	bigmap[count] = pair<string, bufferlist>(random_string(5),
	    KvFlatBtreeAsync::to_bl(random_string(7)));
	set_args_ar[set_index].key = bigmap[count].first;
	set_args_ar[set_index].val = bigmap[count].second;
	cout << "kvs" << j << " set to insert "
	    << bigmap[count].first << std::endl;
	sws[j] = &set_args_ar[set_index].sw;
	err = pthread_create(&real_threads[j], NULL,
	    pset, (void*)&set_args_ar[set_index]);
	if (err < 0) {
	  cout << "error creating first pthread: " << err << std::endl;
	  return err;
	}
	cout << "created " << j << std::endl;
	set_index++;
	count++;
      } else {
	rm_args_ar[rm_index].kvs = kvs_arr[j];
	int rm_int = -1;
	while(bigmap.count(rm_int) == 0) {
	  rm_int = rand() % count;
	}
	assert(bigmap.count(rm_int) > 0);
	cout << j << ": setting rm_args_ar[" << rm_index << "]  to "
	    << bigmap[rm_int].first << std::endl;
	rm_args_ar[rm_index].key = bigmap[rm_int].first;
	cout << "kvs" << j << " is set to remove " << rm_args_ar[rm_index].key
	    << std::endl;
	bigmap.erase(rm_int);
	sws[j] = &rm_args_ar[rm_index].sw;
	err = pthread_create(&real_threads[j], NULL, prm,
	    (void*)&rm_args_ar[rm_index]);
	cout << "created " << j << std::endl;
	if (err < 0) {
	  cout << "error creating remover pthread: " << err << std::endl;
	  return err;
	}
	rm_index++;
      }
    }

    for (int j = 0; j < clients; j++) {
      cout << "waiting to join " << j << std::endl;
      err = pthread_join(real_threads[j], &status[j]);
      if (err < 0) {
	cout << "error joining " << j << ": " << err << std::endl;
	return err;
      }
      cout << "joined " << j << std::endl;
      if (j == 0) {
	suicide_watch.start_time();
      } else if (j == clients - 1) {
	suicide_watch.stop_time();
	sleep(1000);
	cout << "checking consistency" << std::endl;
	if (suicide_watch.get_time() < utime_t(0,1000)
	    && !kvs->is_consistent()) {
	  return -EINCONSIST;
	}
      }
      double time = sws[j]->get_time();
      sws[j]->clear();
      data.avg_latency = (data.avg_latency * data.completed_ops + time)
          / (data.completed_ops + 1);
      data.completed_ops++;
      if (time < data.min_latency) {
        data.min_latency = time;
      }
      if (time > data.max_latency) {
        data.max_latency = time;
      }
      data.total_latency += time;
      ++(data.freq_map[time / increment]);
      if (data.freq_map[time / increment] > data.mode.second) {
	data.mode.first = time / increment;
        data.mode.second = data.freq_map[time / increment];
      }
    }
    if (!kvs->is_consistent()) {
      return -EINCONSIST;
    }
  }
  for (int j = 0; j < clients; j++) {
/*    if (j == clients - 1) {
      cout << kvs->str();
    }*/
    delete kvs_arr[j];
  }

  print_time_data();
  return err;
}

int KvStoreTest::test_teuthology(next_gen_t distr, const map<int, char> &probs)
{
  int err = 0;
  test_random_insertions();
  vector<kv_bench_datum> datums;
  for (int i = 0; i < ops; i++) {
    StopWatch sw;
    kv_bench_datum d;
    cout << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t" << i + 1 << " / "
	<< ops << std::endl;
    pair<string, bufferlist> kv;
    int random = (rand() % 100);
    cout << random << std::endl;
    d.op = probs.lower_bound(random)->second;
    switch (d.op) {
    case 'i':
      kv = (((KvStoreTest *)this)->*distr)(true);
      sw.start_time();
      err = kvs->set(kv.first, kv.second, false);
      sw.stop_time();
      if (err < 0 && err != -17) {
	cout << "Error setting " << kv << ": " << err << std::endl;
	return err;
      } /*else if (!kvs->is_consistent()) {
	cout << "Error setting " << kv << ": not consistent!" << std::endl;
	return -EINCONSIST;
      }*/
      break;
    case 'u':
      kv = (((KvStoreTest *)this)->*distr)(true);
      sw.start_time();
      err = kvs->set(kv.first, kv.second, true);
      sw.stop_time();
      if (err < 0) {
	cout << "Error updating " << kv << ": " << err << std::endl;
	return err;
      } /*else if (!kvs->is_consistent()) {
	cout << "Error updating " << kv << ": not consistent!" << std::endl;
	return -EINCONSIST;
      }*/
      break;
    case 'd':
      kv = (((KvStoreTest *)this)->*distr)(true);
      sw.start_time();
      err = kvs->remove(kv.first);
      sw.stop_time();
      if (err < 0) {
	cout << "Error removing " << kv << ": " << err << std::endl;
	return err;
      } /*else if (!kvs->is_consistent()) {
	cout << "Error removing " << kv << ": not consistent!" << std::endl;
	return -EINCONSIST;
      }*/
      break;
    case 'r':
      kv = (((KvStoreTest *)this)->*distr)(true);
      bufferlist val;
      sw.start_time();
      err = kvs->get(kv.first, &kv.second);
      sw.stop_time();
      if (err < 0) {
	cout << "Error getting " << kv << ": " << err << std::endl;
	return err;
      } /*else if (!kvs->is_consistent()) {
	cout << "Error getting " << kv << ": not consistent!" << std::endl;
	return -EINCONSIST;
      }*/
      break;
    }

    double time = sw.get_time();
    sw.clear();
    d.latency = time;
    datums.push_back(d);
    data.avg_latency = (data.avg_latency * data.completed_ops + time)
        / (data.completed_ops + 1);
    data.completed_ops++;
    if (time < data.min_latency) {
      data.min_latency = time;
    }
    if (time > data.max_latency) {
      data.max_latency = time;
    }
    data.total_latency += time;
    ++(data.freq_map[time / increment]);
    if (data.freq_map[time / increment] > data.mode.second) {
	data.mode.first = time / increment;
      data.mode.second = data.freq_map[time / increment];
    }
  }

  for(vector<kv_bench_datum>::iterator it = datums.begin(); it != datums.end();
      ++it) {
    print_time_datum(&cout, *it);
  }

  print_time_data();
  return err;
}

int KvStoreTest::functionality_tests() {
  int err = 0;
  cout << "initial remove all successful" << std::endl;
  //err = test_set_get_rm_one_kv();
  cout << std::endl;
  if (err < 0) {
    cout << "set/getting one value failed with code " << err;
    cout << std::endl;
    return err;
  }
  err = test_split_merge();
  if (err < 0) {
    cout << "split/merge test failed with code " << err << std::endl;
    return err;
  }
  return err;
}

int KvStoreTest::stress_tests() {
  int err = 0;
  kvs->remove_all();
  //err = test_non_random_insert_gets();
  if (err < 0) {
    cout << "non-random inserts and gets failed with code " << err;
    cout << std::endl;
    return err;
  }
  //err = test_random_insertions();
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

int KvStoreTest::verification_tests(int argc, const char** argv) {
  int err = 0;
  //err = test_verify_concurrent_sets(argc, argv);
  if (err < 0) {
    cout << "concurrent sets failed: " << err << std::endl;
    return err;
  }
  //err = test_verify_concurrent_set_rms(argc, argv);
  if (err < 0) {
    cout << "concurrent set/rms failed: " << err << std::endl;
    return err;
  }
  //err = test_verify_random_set_rms(argc, argv);
  if (err < 0) {
    cout << "concurrent random set/rms failed: " << err << std::endl;
    return err;
  }
  err = test_stress_random_set_rms(argc, argv);
  if (err < 0) {
    cout << "concurrent random stress test failed: " << err << std::endl;
    return err;
  }
  return err;
}

int KvStoreTest::teuthology_tests() {
  int err = 0;
  test_teuthology(&KvStoreTest::rand_distr, probs);
  return err;
}

void KvStoreTest::print_time_datum(ostream * s, kv_bench_datum d) {
  (*s) << d.op;
  if (d.op == 'i') cout << "\t";
  if (d.op == 'd') cout << "\t\t";
  if (d.op == 'r') cout << "\t\t\t";
  if (d.op == 'u') cout << "\t\t\t\t";
  cout << "\t" << d.latency << std::endl;
}

void KvStoreTest::print_time_data() {
  map<int, char>::iterator it = probs.begin();
  cout << "========================================================";
  cout << "\nNumber of initial entries:\t" << entries;
  cout << "\nNumber of sets of operations:\t" << ops;
  cout << "\nNumber of threads per op:\t" << clients;
  cout << "\nk:\t\t\t\t" << k;
  cout << "\nprobability insertions: ." << (it++)->first;
  cout << "\nprobability updates: ." << (it--)->first - ((it++)++)->first;
  cout << "\nprobability deletes: ." << (it--)->first - ((it++)++)->first;
  cout << "\nprobability reads: ." << (it--)->first - ((it++)++)->first;
  cout << "\n";
  cout << "\n";
  cout << "Average latency:\t" << data.avg_latency;
  cout << "ms\nMinimum latency:\t" << data.min_latency;
  cout << "ms\nMaximum latency:\t" << data.max_latency;
  cout << "ms\nMode latency:\t\t" << "between " << data.mode.first * increment;
  cout << " and " << data.mode.first * increment + increment;
  cout << "ms\nTotal latency:\t\t" << data.total_latency;
  cout << "ms\n";
  //return;
/*  cout << "\nHistogram: " << std::endl;
  for(int i = floor(data.min_latency / increment); i <
      ceil(data.max_latency / increment); i++) {
    cout << ">= "<< i * increment << "ms";
    int spaces;
    if (i == 0) spaces = 5;
    else spaces = 3 - floor(log10(i));
    for (int j = 0; j < spaces; j++) {
      cout << " ";
    }
    cout << "[";
    for(int j = 0; j < ((data.freq_map)[i])*45/(data.mode.second); j++) {
      cout << "*";
    }
    cout << std::endl;
  }*/
  cout << "========================================================"
       << std::endl;
}

int main(int argc, const char** argv) {
  KvStoreTest kvst;
  int err = kvst.setup(argc, argv);
  if (err == 0) cout << "setup successful" << std::endl;
  else{
    cout << "error " << err << std::endl;
    return err;
  }
  //err = kvst.teuthology_tests();
  //err = kvst.verification_tests(argc, argv);
  err = kvst.functionality_tests();
  if (err < 0) return err;
  //kvst.stress_tests();
  return 0;
};
