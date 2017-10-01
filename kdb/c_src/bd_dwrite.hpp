///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_dwrite.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device, implementing delayed write algorithm.
///-------------------------------------------------------------------
#ifndef __bd_dwrite_hpp__
#define __bd_dwrite_hpp__

#ifdef _WIN32
#include <map>
#else
#include <tr1/unordered_map>
#endif

#include "bd.hpp"

//--------------------------------------------------------------------
// Block device with delayed write for increasing write performance.
// It accumalates blocks in buffer untill it will reach its capacity.
// Than all blocks are flushed in single operation.
//--------------------------------------------------------------------
class bd_dwrite: public intermediate_block_device {
private:
#ifdef _WIN32
	typedef std::map<uint64_t, block_ptr> buffer_t;
#else
	typedef std::tr1::unordered_map<uint64_t, block_ptr> buffer_t;
#endif

private:
	buffer_t	m_buffer;
	uint32_t	m_buffer_capacity;

	//--------------------------------------------------------------------
	// Function: flush().
	// Description: Forces of buffer flush.
	//--------------------------------------------------------------------
	void flush();

public:
	//--------------------------------------------------------------------
	// Function: bd_dwrite().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_dwrite() {m_buffer_capacity = 0;}

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
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & block_device,
										uint32_t buffer_capacity);

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
#endif // __bd_dwrite_hpp__

