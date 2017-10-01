///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_pool.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device. Implements pool of devices.
/// Each underlaying device must have same block size. But capacity of each
/// device may be different.
///-------------------------------------------------------------------
#ifndef __bd_pool_hpp__
#define __bd_pool_hpp__

#include <vector>

#include "bd.hpp"

//--------------------------------------------------------------------
// Implements pool of block devices. Each underlaying device must have same
// block size. But capacity of each device may be different. Capacity of
// device can not changed after it added to pool.
//--------------------------------------------------------------------
class bd_pool: public block_device {
private:
	typedef std::pair<block_device *, uint64_t> dev_descr_t;	// device consist of pair value:
																														// 1. Pointer to device.
																														// 2. First block of device.
	typedef std::vector<dev_descr_t> devices_t;

private:
	uint64_t	m_capacity;
	uint32_t	m_block_size;
	devices_t m_devices;
	bd_stats	m_stats;

	//--------------------------------------------------------------------
	// Function: uint32_t device_for_block(uint64_t n).
	// Description: Returns index of device descriptor for block number.
	//--------------------------------------------------------------------
	uint32_t device_for_block(uint64_t n);

public:
	//--------------------------------------------------------------------
	// Function: bd_pool().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_pool() {m_capacity = 0; m_block_size = 0;}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the device. Options are device specific.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {devices, [Name]} - list of underlying block devices names. List of
	//      devices may be empty.
	//   {block_size, BlockSize} - size of block in bytes. Parameter is obligatory
	//      when device list is empty. If list is not empty, block size is
	//      obtained from first device.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & devices,
										uint32_t block_size);

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

	// see base class.
	virtual uint32_t block_size();

	// see base class.
	virtual uint64_t capacity();

	// see base class.
	virtual block_ptr alloc();

	//--------------------------------------------------------------------
	// Function: add_device(const std::string& name).
	// Description: Adds device to pool dynamically.
	//--------------------------------------------------------------------
	void add_device(const std::string & name);

	//--------------------------------------------------------------------
	// Function: add_device(block_device * pdev).
	// Description: Adds device to pool dynamically.
	//--------------------------------------------------------------------
	void add_device(block_device * pdev);
};
#endif // __bd_pool_hpp__

