///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_cache.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device with cache.
///-------------------------------------------------------------------
#ifndef __bd_cache_hpp__
#define __bd_cache_hpp__

#include "bd.hpp"
#include "lru_cache.hpp"

//--------------------------------------------------------------------
// stats
//--------------------------------------------------------------------
struct bd_cache_stats: public bd_stats {
	lru_stats *	m_cache_stats;

	bd_cache_stats() {m_cache_stats = NULL;}

	// see base class.
	virtual void soft_reset();

	// see base class.
	virtual std::string to_string();
};

//--------------------------------------------------------------------
// Block device with cache for increasing read performance. To increase
// both read and write performance create bd_cache on top of bd_dwrite
// device.
//--------------------------------------------------------------------
class bd_cache: public intermediate_block_device {
private:
	lru_cache<uint64_t, block_ptr>	m_cache;	// cache
	bd_cache_stats									m_stats;	// stats

	//--------------------------------------------------------------------
	// Function: save_dirty(void * param, uint64_t & n, block_ptr & pb).
	// Description: Callback for saving cache dirty items.
	//--------------------------------------------------------------------
	static void save_dirty(void * param, uint64_t & n, block_ptr & pb);

public:
	//--------------------------------------------------------------------
	// Function: bd_crash().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_cache() {m_stats.m_cache_stats = m_cache.stats();}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the device. Options are device specific.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {block_device, Name} - name of underlaying device.
	//   {cache_capacity, Capacity} - cache capacity in blocks.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & block_device,
											uint32_t cache_capacity);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual void stats(no_stats_list & list);

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual void write(block_list & blocks);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual void sync();
};
#endif // __bd_cache_hpp__

