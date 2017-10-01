///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : nlogger.hpp
/// Author  : Evgeny Khirin <>
/// Description : null logger.
///-------------------------------------------------------------------
#ifndef __nlogger_hpp__
#define __nlogger_hpp__

#include "logger.hpp"

//--------------------------------------------------------------------
// null logger
//--------------------------------------------------------------------
class nlogger: public logger {
	uint64_t	m_head_lsn;
	uint64_t	m_tail_lsn;
	uint64_t	m_checkpoint_lsn;

public:

	nlogger() {
		m_head_lsn = m_tail_lsn = m_checkpoint_lsn = 0;
	}

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name);

	// see base class
	virtual void stop() {}

	// see base class
	virtual void del_underlaying() {}

	// see base class
	virtual void stats(no_stats_list & list) {}

	// see base class
	virtual uint64_t get_head() {return m_head_lsn;}

	// see base class
	virtual uint64_t get_checkpoint() {return m_checkpoint_lsn;}

	// see base class
	virtual void set_head(uint64_t head_lsn, uint64_t checkpoint_lsn) {
		m_head_lsn = head_lsn;
		m_checkpoint_lsn = checkpoint_lsn;
	}

	// see base class
	virtual uint64_t get_tail() {return m_tail_lsn;}

	// see base class
	virtual uint64_t write(const void * pdata, uint32_t size, uint64_t & redo_lsn) {
		redo_lsn = m_tail_lsn;
		m_tail_lsn += size;
		return m_tail_lsn;
	}

	// see base class
	virtual result read_forward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) {return my_eof;}

	// see base class
	virtual result read_backward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) {return my_eof;}

	// see base class
	virtual void sync(uint64_t lsn) {}
};

#endif // __nlogger_hpp__

