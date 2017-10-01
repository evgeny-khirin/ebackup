///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_file.hpp
/// Author  : Evgeny Khirin <>
/// Description : File block device.
///-------------------------------------------------------------------
#ifndef __bd_file_hpp__
#define __bd_file_hpp__

#include "bd.hpp"

//--------------------------------------------------------------------
// File block device
//--------------------------------------------------------------------
class bd_file: public block_device {
private:
	uint64_t 		m_capacity;					// device capacity in blocks
	uint32_t 		m_block_size;				// device block size
	int 				m_fd;								// file descriptor
	bd_stats		m_stats;

public:
	//--------------------------------------------------------------------
	// Function: bd_file().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_file() {m_capacity = 0; m_block_size = 0; m_fd = -1;}

	//--------------------------------------------------------------------
	// Function: ~bd_file().
	// Description: Destructor.
	//--------------------------------------------------------------------
	virtual ~bd_file();

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
	//   {file, FileName} - file name.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, uint32_t block_size,
										uint64_t capacity, const std::string & file);

	// see base class.
	virtual void stats(no_stats_list & list);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual void write(uint64_t n, uint32_t block_size, block_ptr & pb);

	// see base class.
	virtual void write(block_list & blocks);

	// see base class.
	virtual void write(uint32_t block_size, block_list & blocks);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual block_ptr read(uint64_t n, uint32_t block_size);

	// see base class.
	virtual void sync();

	// see base class.
	virtual uint32_t block_size();

	// see base class.
	virtual uint64_t capacity();

	// see base class.
	virtual block_ptr alloc();
};

#endif /* __bd_file_hpp__ */

