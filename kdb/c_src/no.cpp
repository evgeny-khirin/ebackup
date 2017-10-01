///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : no.cpp
/// Author  : Evgeny Khirin <>
/// Description : Named object. If object has unique name, than it is
/// registered and can be found by name in registry.
///-------------------------------------------------------------------
#include "no.hpp"
#include "utils.hpp"
#include "mutex.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
std::string no_stats::to_string() {
	std::stringstream ss;
	ss << m_reg_name << ": ";
	return ss.str();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
no_stats::~no_stats() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
named_object::~named_object() {
	unregister(m_name);
}

//--------------------------------------------------------------------
// public function.
// Common options:
//   {name, Name} - registered object name. Named objects significantly
//      reduce complexity of program.
//--------------------------------------------------------------------
void named_object::start(const opt_map & options) {
	opt_map::const_iterator it;
	if ((it = options.find("name")) == options.end()) {
		throw missed_param();
	}
	m_name = it->second;
	register_(m_name, this);
}

//--------------------------------------------------------------------
// global variables
//--------------------------------------------------------------------
#ifdef _WIN32
typedef std::map<std::string, const named_object *> registry;
#else
typedef std::tr1::unordered_map<std::string, const named_object *> registry;
#endif
registry	gr;
rmutex *	gr_mutex = new rmutex;

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void register_(const std::string & key, const named_object * obj) {
	scoped_lock<rmutex> lock(gr_mutex);
	if (gr.find(key) != gr.end()) {
		throw already_registered();
	}
	gr[key] = obj;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void unregister(const std::string & key) {
	scoped_lock<rmutex> lock(gr_mutex);
	gr.erase(key);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
named_object * whereis(const std::string & key) {
	scoped_lock<rmutex> lock(gr_mutex);
	registry::const_iterator it = gr.find(key);
	if (it == gr.end()) {
		return NULL;
	}
	return (named_object *)it->second;
}


