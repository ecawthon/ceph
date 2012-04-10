/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef STATETRACKER_HPP_
#define STATETRACKER_HPP_

#include <vector>
#include <map>
#include <string>
#include <list>
#include <tr1/memory>

#include <common/Mutex.h>

/**
 * This struct represents a single state. It has a name and id,
 * along with an optional superstate and an optional list of substates.
 * If there is no superstate, it's left NULL.
 * Users only get const pointers to the State.
 *
 * Anybody accessing the substates member must take the substate_lock first.
 * All other data members are immutable once created, so you can use
 * lock-free accesses to them.
 */
class StateMakerImpl;
struct State {
  /// The system ID this State is part of
  StateMakerImpl *system_pointer;
  /// The ID for this state
  int state_id;
  /// The name of this state
  std::string state_name;
  /// The superstate, or NULL if this has no superstates.
  const State *superstate;
  /// Any substates of this state.
  std::list<int> substates;
  /// Lock to protect substate access
  mutable Mutex substate_lock;

private:
  friend class StateMakerImpl;
  State(StateMakerImpl *sys, int id, std::string &name) :
    system_pointer(sys), state_id(id), state_name(name),
    superstate(NULL), substate_lock(name.c_str()) {}
};

/**
 * The StateMakerImpl allows you to dynamically allocate, name, and reference
 * state pointers in a system. Each state pointer consists of:
 * 1) A system id, (generated by the StateMakerImpl)
 * 2) a state id, (which you can provide or have generated)
 * 3) a state name, (which you must provide)
 * You can reference states with the system id and either one of state id
 * and state name.
 *
 * It is intended to support two different modes of use.
 * One: You have a known list of states in a system (perhaps as an enum,
 * perhaps as a list of strings). On startup you dynamically allocate a
 * system id, and then insert each state as a string/int
 * pair. You or others can refer to them with the system id and either
 * state id int or string.
 * Two: On startup, allocate a system id. Then dynamically allocate new states
 * as you move through the system, in order to do things like count how
 * often you are in a particular state in a bootstrapped fashion that doesn't
 * require central organization.
 */
class StateMakerImpl {
public:
  /**
   * @defgroup StateMakerImpl object information
   * @{
   */
private:
  /// my system name
  std::string system_name;
  /// The allocator for new state IDs
  int state_id_alloc;
  /// vector of state names, indexed by state ID
  std::vector<std::string> state_names;
  /// vector of States, indexed by state ID
  std::vector<State *> states;
  /// map from state names to state IDs
  std::map<std::string, int> state_name_map;
  /// protect access to the object's members
  mutable Mutex lock;
  /**
   * Construct a new StateMakerImpl.
   *
   * @param name The name of the StateMakerImpl.
   */
  StateMakerImpl(const char *name) :
    system_name(name),
    state_id_alloc(0),
    state_names(),
    states(),
    state_name_map(),
    lock(name)
  {}
public:
  ~StateMakerImpl() {
    for (std::vector<State *>::iterator i = states.begin();
        i != states.end();
        ++i) {
      delete *i;
    }
  }
  /**
   * Create a new StateMaker.
   *
   * @param name The name of the StateMaker.
   * @return A StateMaker.
   */
  static std::tr1::shared_ptr<StateMakerImpl>
    create_state_maker(const char *name) {
    std::tr1::shared_ptr<StateMakerImpl> tracker(new StateMakerImpl(name));
    return tracker;
  }

  /**
   * Retrieve the system name for this StateMakerImpl.
   * @return the System name, as a string.
   */
  const std::string get_system_name() const { return system_name; }
  /// @} StateMakerImpl object information

  /**
   *  @defgroup States Functions to handle state tracking
   *  @{
   */
private:
  /**
   * Allocate a new state and insert it into the system. This doesn't
   * do any error checking; do that yourself!
   * The lock should be held before calling this function.
   *
   * @param state_name The name of the state
   * @param id The ID of the state to insert
   * @param superstate The ID of the superstate, if there is one, otherwise -1
   */
  void _allocate_state(const char *state_name, int id, int superstate);
public:
  /**
   * Register a new state in this system, with a unique name.
   *
   * @param state_name The name of the state
   * @param superstate The state ID of the super state, if it exists; -1 else.
   * @return The state ID of the new state, or -EEXIST if the state name
   * already exists, or -ENOENT if the super state does not exist.
   */
  int create_new_state(const char *state_name, int superstate);
  /**
   * Register a new state in this system with the given state ID. The
   * given ID must be greater than any currently existing state ID and
   * the name must not already be registered.
   *
   * @param state_name The name of the state
   * @param state_id The ID of the state
   * @param superstate The state ID of the super state, if it exists, -1 else.
   * @return The state ID of the new state, or -EEXIST if the state name
   * already exists, or -ENOENT if the super state does not exist, or
   * -EINVAL if the state ID is invalid.
   */
  int create_new_state_with_id(const char *state_name, int id, int superstate);
  /**
   * Retrieve a state object reference given its state id.
   *
   * @param id The ID of the state.
   * @return The associated state object, as a const pointer, or NULL if it
   * does not exist.
   */
  const State *retrieve_state (int id) const;
  /**
   * Retrieve a state ID from its name.
   *
   * @param state_name The name of the state
   * @return The ID of the state, as an int, or -ENOENT if it does not exist
   */
  int retrieve_state_id(const char *name) const;
  /**
   * Check if a state belongs to this StateMaker.
   *
   * @param state The State to check
   * @return true if it is my state, false otherwise
   */
  bool is_my_state(const State *state) const { return state->system_pointer == this; }
  ///@} // States
};
typedef std::tr1::shared_ptr<StateMakerImpl> StateMaker;

/**
 * This class holds a set of collected StateMakers which have some
 * logical relation, like for each thread that lives within a class.
 * Each module (including the parent) must have a consistently-used name.
 */
class ModularStateMakerImpl {
  std::map<const char *, StateMaker> modules;
  std::string name;
  Mutex lock;
public:
  /**
   * Create the StateMaker for a given module, or retrieve it if one
   * already exists.
   */
  StateMaker create_maker(const char *module);
  /**
   * Get the StateMaker for a given module.
   *
   * @param module The name of the module that goes with the StateMaker.
   */
  StateMaker get_maker(const char *module);
  /**
   * Create a new ModularStateMaker.
   */
  static std::tr1::shared_ptr<ModularStateMakerImpl>
  create_modular_state_maker(const char *name) {
    std::tr1::shared_ptr<ModularStateMakerImpl>
      modular_maker(new ModularStateMakerImpl(name));
    return modular_maker;
  }
private:
  ModularStateMakerImpl(const char *n) : name(n), lock(n) {}
};
typedef std::tr1::shared_ptr<ModularStateMakerImpl> ModularStateMaker;

/**
 * StateTracker
 */
class StateTracker {
public:
  /**
   * Retrieve the StateMaker associated with a subsystem. If one
   * doesn't exist, this creates it.
   *
   * @return the requested StateMaker
   */
  virtual StateMaker get_subsystem_maker(const char *system) = 0;
  /**
   * Report that you've entered a new state or substate.
   *
   * @param system The subsystem reporting in.
   * @param id The id for this instance of the subsystem.
   * @param state The name of the new state as a const char*.
   *
   * @return The id of the state which it's switched into.
   */
  virtual int report_state_changed(const char *system, long id, const char *state) = 0;
  /**
   * Report that you've entered a new state or substate.
   *
   * @param system The subsystem reporting in.
   * @param id The id for this instance of the subsystem.
   * @param state The ID of the new state, as an int.
   */
  virtual void report_state_changed(const char *system, long id, int state) = 0;
protected:
  ~StateTracker() {}
};

#endif /* STATETRACKER_HPP_ */