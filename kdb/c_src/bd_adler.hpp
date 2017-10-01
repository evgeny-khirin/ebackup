///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_adler.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device - data integrity controlled
/// by ADLER-32 check sum.
///-------------------------------------------------------------------
#ifndef __bd_adler_hpp__
#define __bd_adler_hpp__

#include "bd.hpp"

//--------------------------------------------------------------------
// Adler block device. ADLER-32 checksum is stored in end of each block.
//--------------------------------------------------------------------
class bd_adler: public intermediate_block_device {
private:
	uint32_t 		m_block_size;	// effective device block size

public:
	//--------------------------------------------------------------------
	// Function: bd_adler().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_adler() {m_block_size = 0;}

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
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & block_device);

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual void write(block_list & blocks);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual uint32_t block_size();
};
#endif // __bd_adler_hpp__

