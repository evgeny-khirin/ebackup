///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_factor.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device - block size is  multiplied up
/// by constant factor.
///-------------------------------------------------------------------
#ifndef __bd_factor_hpp__
#define __bd_factor_hpp__

#include "bd.hpp"

//--------------------------------------------------------------------
// Factor block device.
//--------------------------------------------------------------------
class bd_factor: public intermediate_block_device {
private:
	uint64_t		m_capacity;
	uint32_t 		m_block_size;	// effective device block size
	bool        m_stop_underlaying;

public:
	//--------------------------------------------------------------------
	// Function: bd_factor().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_factor() {m_capacity = 0; m_block_size = 0; m_stop_underlaying = true;}

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
	//   {factor, Factor} - multiplying factor. Factor must be positive integer.
	//   {stop_underlaying, Bool} - should behaive as intermediate_block_device
	//      in stop and del_underlaying functions. Default, is "true".
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & block_device,
										uint32_t factor, bool stop_underlaying);

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
	virtual uint32_t block_size();

	// see base class.
	virtual uint64_t capacity();

	// see base class.
	virtual block_ptr alloc();
};

#endif // __bd_factor_hpp__

