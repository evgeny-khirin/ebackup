///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_raid0.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device. Implements software RAID 0 (striped
/// disks). Each underlaying device must have same capacity and block size.
///-------------------------------------------------------------------
#ifndef __bd_raid0_hpp__
#define __bd_raid0_hpp__

#include <vector>

#include "bd.hpp"

//--------------------------------------------------------------------
// Implements raid0 (striping) of same block devices. Devices must
// have same capacity and block size.
//--------------------------------------------------------------------
class bd_raid0: public block_device {
private:
	typedef std::vector<block_device *> devices_t;

private:
	uint64_t	m_capacity;
	uint32_t	m_block_size;
	devices_t m_devices;
	bd_stats	m_stats;

public:
	//--------------------------------------------------------------------
	// Function: bd_raid0().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_raid0() {m_capacity = 0; m_block_size = 0;}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the device. Options are device specific.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {devices, [Name]} - list of underlying block devices names. List must
	//      be non-empty.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & devices);

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
};
#endif // __bd_raid0_hpp__

