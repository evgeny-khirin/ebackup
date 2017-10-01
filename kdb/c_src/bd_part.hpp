///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_part.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device - presents partition on top of
/// underlaying device.
///-------------------------------------------------------------------
#ifndef __bd_part_hpp__
#define __bd_part_hpp__

#include "bd.hpp"

//--------------------------------------------------------------------
// Implements partition on top of underlaying device.
//--------------------------------------------------------------------
class bd_part: public intermediate_block_device {
private:
	uint64_t 	m_start;
	uint64_t 	m_capacity;
	bd_stats	m_stats;

public:
	//--------------------------------------------------------------------
	// Function: bd_part().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_part() {m_start = m_capacity = 0;}

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
	//   {start, Start} - first block of partion.
	//   {capacity, Capacity} - capacity of partition in blocks.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & block_device,
										uint64_t start, uint64_t capacity);


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
	virtual uint64_t capacity();
};
#endif // __bd_part_hpp__

