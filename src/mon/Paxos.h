// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/*
time---->

cccccccccccccccccca????????????????????????????????????????
cccccccccccccccccca????????????????????????????????????????
cccccccccccccccccca???????????????????????????????????????? leader
cccccccccccccccccc????????????????????????????????????????? 
ccccc?????????????????????????????????????????????????????? 

last_committed

pn_from
pn

a 12v 
b 12v
c 14v
d
e 12v


*/


/*
 * NOTE: This libary is based on the Paxos algorithm, but varies in a few key ways:
 *  1- Only a single new value is generated at a time, simplifying the recovery logic.
 *  2- Nodes track "committed" values, and share them generously (and trustingly)
 *  3- A 'leasing' mechism is built-in, allowing nodes to determine when it is safe to 
 *     "read" their copy of the last committed value.
 *
 * This provides a simple replication substrate that services can be built on top of.
 * See PaxosService.h
 */

#ifndef CEPH_MON_PAXOS_H
#define CEPH_MON_PAXOS_H

#include "include/types.h"
#include "mon_types.h"
#include "include/buffer.h"
#include "messages/PaxosServiceMessage.h"
#include "msg/msg_types.h"

#include "include/Context.h"

#include "common/Timer.h"

class Monitor;
class MMonPaxos;
class Paxos;


// i am one state machine.
/**
 *
 */
class Paxos {
  /**
   * The Monitor to which this Paxos class is associated with.
   */
  Monitor *mon;

  // my state machine info
  int machine_id;
  const char *machine_name;

  friend class Monitor;
  friend class PaxosService;

  list<std::string> extra_state_dirs;

  // LEADER+PEON

  // -- generic state --
public:
  /**
   * @defgroup Paxos_h_states States on which the leader/peon may be.
   * @{
   */
  /**
   * Leader/Peon is in Paxos' Recovery state
   */
  const static int STATE_RECOVERING = 1;
  /**
   * Leader/Peon is idle, and the Peon may or may not have a valid lease.
   */
  const static int STATE_ACTIVE     = 2;
  /**
   * Leader/Peon is updating to a new value.
   */
  const static int STATE_UPDATING   = 3;
 
  /**
   * Obtain state name from constant value.
   *
   * @note This function will raise a fatal error if @p s is not
   *	   a valid state value.
   *
   * @param s State value.
   * @return The state's name.
   */
  static const char *get_statename(int s) {
    switch (s) {
    case STATE_RECOVERING: return "recovering";
    case STATE_ACTIVE: return "active";
    case STATE_UPDATING: return "updating";
    default: assert(0); return 0;
    }
  }

private:
  /**
   * The state we are in.
   */
  int state;
  /**
   * @} Paxos_h_states 
   */

public:
  /**
   * Check if we are recovering.
   *
   * @return 'true' if we are on the Recovering state; 'false' otherwise.
   */
  bool is_recovering() const { return state == STATE_RECOVERING; }
  /**
   * Check if we are active.
   *
   * @return 'true' if we are on the Active state; 'false' otherwise.
   */
  bool is_active() const { return state == STATE_ACTIVE; }
  /**
   * Check if we are updating.
   *
   * @return 'true' if we are on the Updating state; 'false' otherwise.
   */
  bool is_updating() const { return state == STATE_UPDATING; }

private:
  /**
   * @defgroup Paxos_h_recovery_vars Common recovery-related member variables
   * @note These variables are common to both the Leader and the Peons.
   * @note We also call the Recovery Phase as Phase 1.
   * @{
   */
  /**
   *
   */
  version_t first_committed;
  /**
   * Last Proposal Number
   *
   * @todo Expand description
   */
  version_t last_pn;
  /**
   * Last committed value's version.
   *
   * On both the Leader and the Peons, this is the last value's version that 
   * was accepted by a given quorum and thus committed, that this instance 
   * knows about.
   *
   * @note It may not be the last committed value's version throughout the
   *	   system. If we are a Peon, we may have not been part of the quorum
   *	   that accepted the value, and for this very same reason we may still
   *	   be a (couple of) version(s) behind, until we learn about the most
   *	   recent version.
   */
  version_t last_committed;
  /**
   * Last committed value's time.
   *
   * When the commit happened.
   */
  utime_t last_commit_time;
  /**
   * The last Proposal Number we have accepted.
   *
   * On the Leader, it will be the Proposal Number picked by the Leader 
   * itself. On the Peon, however, it will be the proposal sent by the Leader
   * and it will only be updated iif its value is higher than the one
   * already known by the Peon.
   */
  version_t accepted_pn;
  /**
   * @todo This has something to do with the last_committed version. Not sure
   *	   about what it entails, tbh.
   */
  version_t accepted_pn_from;
  /**
   * @todo Check out if this map has any purpose at all. So far, we have only
   *	   seen it being read, although it is never affected.
   */
  map<int,version_t> peer_first_committed;
  /**
   * Map holding the last committed version by each quorum member.
   *
   * The versions kept in this map are updated during the collect phase.
   * When the Leader starts the collect phase, each Peon will reply with its
   * last committed version, which will then be kept in this map.
   */
  map<int,version_t> peer_last_committed;
  /**
   * @todo Check out what 'slurping' is.
   */
  int slurping;

  // active (phase 2)
  /**
   * When does our read lease expires.
   *
   * Instead of performing a full commit each time a read is requested, we
   * keep leases. Each lease will have an expiration date, which may or may
   * not be extended. This member variable will keep when is the lease 
   * expiring.
   */
  utime_t lease_expire;
  list<Context*> waiting_for_active;
  list<Context*> waiting_for_readable;

  version_t latest_stashed;

  // -- leader --
  // recovery (paxos phase 1)
  unsigned   num_last;
  version_t  uncommitted_v;
  version_t  uncommitted_pn;
  bufferlist uncommitted_value;

  Context    *collect_timeout_event;

  // active
  set<int>   acked_lease;
  Context    *lease_renew_event;
  Context    *lease_ack_timeout_event;
  Context    *lease_timeout_event;

  // updating (paxos phase 2)
  bufferlist new_value;
  set<int>   accepted;

  Context    *accept_timeout_event;

  list<Context*> waiting_for_writeable;
  list<Context*> waiting_for_commit;

  //synchronization warnings
  utime_t last_clock_drift_warn;
  int clock_drift_warned;


  class C_CollectTimeout : public Context {
    Paxos *paxos;
  public:
    C_CollectTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->collect_timeout();
    }
  };

  class C_AcceptTimeout : public Context {
    Paxos *paxos;
  public:
    C_AcceptTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->accept_timeout();
    }
  };

  class C_LeaseAckTimeout : public Context {
    Paxos *paxos;
  public:
    C_LeaseAckTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->lease_ack_timeout();
    }
  };

  class C_LeaseTimeout : public Context {
    Paxos *paxos;
  public:
    C_LeaseTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->lease_timeout();
    }
  };

  class C_LeaseRenew : public Context {
    Paxos *paxos;
  public:
    C_LeaseRenew(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->lease_renew_timeout();
    }
  };


  void collect(version_t oldpn);
  void handle_collect(MMonPaxos*);
  void handle_last(MMonPaxos*);
  void collect_timeout();

  void begin(bufferlist& value);
  void handle_begin(MMonPaxos*);
  void handle_accept(MMonPaxos*);
  void accept_timeout();

  void commit();
  void handle_commit(MMonPaxos*);
  void extend_lease();
  void handle_lease(MMonPaxos*);
  void handle_lease_ack(MMonPaxos*);

  void lease_ack_timeout();    // on leader, if lease isn't acked by all peons
  void lease_renew_timeout();  // on leader, to renew the lease
  void lease_timeout();        // on peon, if lease isn't extended

  void cancel_events();

  version_t get_new_proposal_number(version_t gt=0);
  
  void warn_on_future_time(utime_t t, entity_name_t from);

public:
  Paxos(Monitor *m,
	int mid) : mon(m),
		   machine_id(mid), 
		   machine_name(get_paxos_name(mid)),
		   state(STATE_RECOVERING),
		   first_committed(0),
		   last_pn(0),
		   last_committed(0),
		   accepted_pn(0),
		   accepted_pn_from(0),
		   slurping(0),
		   collect_timeout_event(0),
		   lease_renew_event(0),
		   lease_ack_timeout_event(0),
		   lease_timeout_event(0),
		   accept_timeout_event(0),
		   clock_drift_warned(0) { }

  const char *get_machine_name() const {
    return machine_name;
  }

  void dispatch(PaxosServiceMessage *m);

  void init();
  /**
   * This function runs basic consistency checks. Importantly, if
   * it is inconsistent and shouldn't be, it asserts out.
   *
   * @return True if consistent, false if not.
   */
  bool is_consistent();

  void restart();
  void leader_init();
  void peon_init();

  void share_state(MMonPaxos *m, version_t first_committed, version_t last_committed);
  void store_state(MMonPaxos *m);

  void add_extra_state_dir(string s) {
    extra_state_dirs.push_back(s);
  }

  // -- service interface --
  void wait_for_active(Context *c) {
    waiting_for_active.push_back(c);
  }

  void trim_to(version_t first, bool force=false);
  
  void start_slurping();
  void end_slurping();
  bool is_slurping() { return slurping == 1; }

  // read
  version_t get_version() { return last_committed; }
  bool is_readable(version_t seen=0);
  bool read(version_t v, bufferlist &bl);
  version_t read_current(bufferlist &bl);
  void wait_for_readable(Context *onreadable) {
    //assert(!is_readable());
    waiting_for_readable.push_back(onreadable);
  }

  // write
  bool is_leader();
  bool is_writeable();
  void wait_for_writeable(Context *c) {
    assert(!is_writeable());
    waiting_for_writeable.push_back(c);
  }

  bool propose_new_value(bufferlist& bl, Context *oncommit=0);
  void wait_for_commit(Context *oncommit) {
    waiting_for_commit.push_back(oncommit);
  }
  void wait_for_commit_front(Context *oncommit) {
    waiting_for_commit.push_front(oncommit);
  }

  // if state values are incrementals, it is usefult to keep
  // the latest copy of the complete structure.
  void stash_latest(version_t v, bufferlist& bl);
  version_t get_stashed(bufferlist& bl);
  version_t get_stashed_version() { return latest_stashed; }

  version_t get_first_committed() { return first_committed; }
};



#endif

