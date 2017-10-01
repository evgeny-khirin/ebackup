///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_crash.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device wich simulates power failure by
/// random corruption of last written block.
///-------------------------------------------------------------------
#ifndef __bd_crash_hpp__
#define __bd_crash_hpp__

#include <boost/random/linear_congruential.hpp>

#include "bd.hpp"
#include "mutex.hpp"

//--------------------------------------------------------------------
// Intermediate block device wich simulates power failure by random
// corruption of last written block.
// Used in unit test.
//--------------------------------------------------------------------
class bd_crash: public intermediate_block_device {
private:
	typedef boost::minstd_rand rand_t;

private:
	uint64_t 	m_last_n;						// number of last written block or UINT64_MAX
	block_ptr	m_last_pb;					// last written block's data
	rand_t		m_rand;							// random number generator.
	rmutex		m_mutex;
	bool			m_power_failure;		// power_failure?

	//--------------------------------------------------------------------
	// Function: reproducible_ops().
	// Description: Simulates power failure synchronously for reproducible
	// test results.
	//--------------------------------------------------------------------
	void reproducible_ops();

public:
	//--------------------------------------------------------------------
	// Function: bd_crash().
	// Description: Constructor.
	//--------------------------------------------------------------------
	bd_crash() {m_power_failure = false; m_last_n = UINT64_MAX;}

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
										int32_t seed);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual void write(block_list & blocks);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual void sync();

	//--------------------------------------------------------------------
	// Function: ops(bool corrupt).
	// Description: Simulates power failure.
	//--------------------------------------------------------------------
	void ops(bool corrupt);
};
#endif // __bd_crash_hpp__

