///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : no.hpp
/// Author  : Evgeny Khirin <>
/// Description : Named object. If object has unique name, than it is
/// registered and can be found by name in registry.
///-------------------------------------------------------------------
#ifndef __no_hpp__
#define __no_hpp__

#include <string>
#include <list>
#ifdef _WIN32
#include <map>
#else
#include <tr1/unordered_map>
#endif

#include "status.hpp"

//--------------------------------------------------------------------
// Misc definitions and forward declarations.
//--------------------------------------------------------------------
#ifdef _WIN32
typedef std::map<std::string, std::string> opt_map;
#else
typedef std::tr1::unordered_map<std::string, std::string> opt_map;
#endif

//--------------------------------------------------------------------
// Statistics class for named objects.
//--------------------------------------------------------------------
struct no_stats {
	std::string		m_reg_name;			// registered name of object;

	//--------------------------------------------------------------------
	// Function: ~no_stats().
	// Description: Destructor.
	//--------------------------------------------------------------------
	virtual ~no_stats();

	//--------------------------------------------------------------------
	// Function: soft_reset().
	// Description: Does soft reset of statistics preparing it for new accumulation.
	// It does not reset non accumulating counters, like number of dirty items
	// in cache. But resets number of hits and lookups in cache.
	//--------------------------------------------------------------------
	virtual void soft_reset() = 0;

	//--------------------------------------------------------------------
	// Function: to_string() -> string
	// Description: Converts statistics to string.
	//--------------------------------------------------------------------
	virtual std::string to_string();
};
typedef std::list<no_stats *> no_stats_list;

//--------------------------------------------------------------------
// Named object
//--------------------------------------------------------------------
class named_object {
private:
	std::string m_name;

public:
	//--------------------------------------------------------------------
	// Function: ~named_object().
	// Description: Destructor.
	//--------------------------------------------------------------------
	virtual ~named_object();

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts named object. Options are object specific.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: stop().
	// Description: Stops named object. After stopping object is not functional
	// and must be deleted. By default, does nothing. If object has underlaying
	// objects, than it must call thier stop functions after it stopped itself.
	// That allows effective stopping of complex tree in single operation on its
	// root. And then deleting of complex objects trees in any order after stopping.
	//--------------------------------------------------------------------
	virtual void stop() = 0;

	//--------------------------------------------------------------------
	// Function: del_underlaying().
	// Description: Allows deleting of complex dynamic trees of objects in single
	// operation on its root. By default, does nothing. If object has pointer on
	// underlaying objects, than it must call thier delete functions before deleting
	// pointer.
	//--------------------------------------------------------------------
	virtual void del_underlaying() = 0;

	//--------------------------------------------------------------------
	// Function: stats(stats_list_t & list).
	// Description: Collects statistics of objects tree. If object has pointer
	// on underlaying objects, than it must call thier stats functions as well.
	//--------------------------------------------------------------------
	virtual void stats(no_stats_list & list) = 0;

	//--------------------------------------------------------------------
	// Function: get_name() -> string
	// Description: Obtains name of object.
	//--------------------------------------------------------------------
	const std::string & get_name() {return m_name;}
};

//--------------------------------------------------------------------
// Function: register_(const std::string & key, const named_object * obj).
// Description: Registers object in global registry.
//--------------------------------------------------------------------
void register_(const std::string & key, const named_object * obj);

//--------------------------------------------------------------------
// Function: unregister(const std::string & key).
// Description: Removes object registration from global registry.
//--------------------------------------------------------------------
void unregister(const std::string & key);

//--------------------------------------------------------------------
// Function: whereis(const std::string & key) -> named_object * | NULL
// Description: Looks up object in global registry.
// Returns pointer to object or NULL if object is not registered.
//--------------------------------------------------------------------
named_object * whereis(const std::string & key);

#endif // __no_hpp__

