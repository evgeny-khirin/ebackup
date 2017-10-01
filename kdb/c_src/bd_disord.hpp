///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_disord.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device wich simulates real devices.
///               Main feature - blocks are flushed and written to underlaying
///               device in disorder.
///-------------------------------------------------------------------
#ifndef __bd_disord_hpp__
#define __bd_disord_hpp__

#ifdef _WIN32
#include <map>
#else
#include <tr1/unordered_map>
#endif
#include <vector>
#include <boost/random/linear_congruential.hpp>

#include "bd.hpp"

//--------------------------------------------------------------------
// Intermediate block device wich flushes and writes blocks to underlaying
// device in disorder.
// Used in unit test.
//--------------------------------------------------------------------
class bd_disord: public intermediate_block_device {
private:
	typedef boost::minstd_rand rand_t;
#ifdef _WIN32
	typedef std::map<uint64_t, block_ptr> buffer_t;
#else
	typedef std::tr1::unordered_map<uint64_t, block_ptr> buffer_t;
#endif

private:
	buffer_t								m_buffer;
	uint64_t *							m_buffer_index;
	uint32_t								m_buffer_capacity;
	rand_t									m_rand;							// random number generator.

	//--------------------------------------------------------------------
	// Function: flush().
	// Description: Forces of buffer flush.
	//--------------------------------------------------------------------
	void flush();

	//--------------------------------------------------------------------
	// Function: push(uint64_t n, block_ptr& pb, buffer_t & acc).
	// Description: Pushes block into buffer. If buffer is full than random item is
	// popped into accumulating list.
	//--------------------------------------------------------------------
	void push(uint64_t n, block_ptr & pb, buffer_t & acc);

public:
	//--------------------------------------------------------------------
	// Function: bd_disord().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_disord() {m_buffer_capacity = 0; m_buffer_index = NULL;}

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
	//   {buffer_capacity, Capacity} - capacity of buffer in blocks.
	//   {seed, Seed} - random number generator's seed. Parameter is optional.
	//      If missed, than random number generator initialized with some predefined
	//      value.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & block_device,
										uint32_t buffer_capacity, int32_t seed);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual void write(block_list & blocks);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual void sync();
};
#endif // __bd_disord_hpp__

