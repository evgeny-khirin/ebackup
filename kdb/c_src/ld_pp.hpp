///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : ld_pp.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate log device with ping-pong write algorithm.
/// Prevents log corruption when tail block is overwritten multiple times
/// by logger.
///-------------------------------------------------------------------
#ifndef __ld_pp_hpp__
#define __ld_pp_hpp__

#include "ld.hpp"

//--------------------------------------------------------------------
// Intermediate ping-pong log device.
//--------------------------------------------------------------------
class ld_pp: public intermediate_log_device {
private:
	uint32_t		m_block_size;			// effective block size. Each
																// block reserves 12 bytes for
																// logical block number and
																// generation.
	uint64_t		m_tail_block;			// tail's block number
	uint32_t		m_tail_gen;				// tail generation
	uint64_t		m_tail_lsn;				// tail lsn, saved for m_pdevice->sync.

	//--------------------------------------------------------------------
	// Function: block_ptr read_block(uint64_t n, uint32_t & generation).
	// Description: Reads block and its generation from device.
	//--------------------------------------------------------------------
	block_ptr read_block(uint64_t n, uint32_t & generation);

public:
	//--------------------------------------------------------------------
	// Function: ld_pp().
	// Description: Constructor.
	//--------------------------------------------------------------------
	ld_pp() {
		m_block_size = 0;
		m_tail_block = 0;
		m_tail_gen = 0;
		m_tail_lsn = 0;
	}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the device. Options are device specific.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {log_device, Name} - name of underlaying device.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & log_device);

	// see base class.
	virtual void set_tail(uint64_t lsn, uint64_t tail_block);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual uint32_t block_size();

	// see base class.
	virtual void sync(uint64_t tail_lsn);
};
#endif // __ld_pp_hpp__

