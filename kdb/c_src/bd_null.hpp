///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_null.hpp
/// Author  : Evgeny Khirin <>
/// Description : Null block device.
///-------------------------------------------------------------------
#ifndef __bd_null_hpp__
#define __bd_null_hpp__

#include "bd.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// null block device.
//--------------------------------------------------------------------
class bd_null: public block_device {
private:
	uint64_t 		m_capacity;					// device capacity in blocks
	uint32_t 		m_block_size;				// device block size
	bd_stats		m_stats;
	block_ptr		m_pb0;
	block_ptr		m_pb1;

public:
	//--------------------------------------------------------------------
	// Function: bd_null().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_null() {m_capacity = 0; m_block_size = 0;}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the device. Options are device specific.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {block_size, BlockSize} - size of block in bytes. Default is 4K.
	//   {capacity, Capacity} - device capacity in blocks. Default is 2^64.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, uint32_t block_size,
										uint64_t capacity);

	// see base class.
	virtual void stats(no_stats_list & list);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	//see base class
	virtual void write(uint64_t n, block_ptr & pb);

	//see base class
	virtual void write(block_list & blocks);

	//see base class
	virtual block_ptr read(uint64_t n);

	//see base class
	virtual void sync();

	//see base class
	virtual uint32_t block_size();

	//see base class
	virtual uint64_t capacity();

	//see base class
	virtual block_ptr alloc();
};

#endif /* __bd__null_hpp__ */

