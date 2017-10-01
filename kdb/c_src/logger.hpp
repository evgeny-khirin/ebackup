///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : logger.hpp
/// Author  : Evgeny Khirin <>
/// Description : Logger for transaction systems.
///-------------------------------------------------------------------
#ifndef __logger_hpp__
#define __logger_hpp__

#include "ld.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// logger interface
//--------------------------------------------------------------------
class logger: public named_object {
public:
	//--------------------------------------------------------------------
	// Function: get_head() -> uint64_t
	// Description: Returns head LSN of log.
	//--------------------------------------------------------------------
	virtual uint64_t get_head() = 0;

	//--------------------------------------------------------------------
	// Function: get_checkpoint() -> uint64_t
	// Description: Returns transaction manager checkpoint LSN.
	//--------------------------------------------------------------------
	virtual uint64_t get_checkpoint() = 0;

//--------------------------------------------------------------------
	// Function: set_head(uint64_t head_lsn, uint64_t low_watter_mark).
	// Description: Sets new head LSN of log. New LSN must be greater
	// than current. Old entries, previos to new head are discarded.
	// Low watter mark LSN must be between log head and log tail.
	//--------------------------------------------------------------------
	virtual void set_head(uint64_t head_lsn, uint64_t low_watter_mark) = 0;

	//--------------------------------------------------------------------
	// Function: get_tail() -> uint64_t
	// Description: Returns tail LSN of log.
	//--------------------------------------------------------------------
	virtual uint64_t get_tail() = 0;

	//--------------------------------------------------------------------
	// Function: write(const void * pdata, uint32_t size, uint64_t & redo_lsn) -> uint64_t
	// Description: Writes binary data to log. Sets redo_lsn to tail LSN before
	// write operation.
	// Returns undo LSN for the data (LSN past the data).
	//--------------------------------------------------------------------
	virtual uint64_t write(const void * pdata, uint32_t size, uint64_t & redo_lsn) = 0;

	//--------------------------------------------------------------------
	// Function: read_forward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) -> ok | eof
	// Description: Reads binary data from log in forward direction. Buffer is
	// overwritten from its beginning.
	//--------------------------------------------------------------------
	result read_forward(serial_buffer & buffer, uint64_t lsn) {
		uint64_t next_lsn;
		return read_forward(buffer, lsn, next_lsn);
	}
	virtual result read_forward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) = 0;

	//--------------------------------------------------------------------
	// Function: read_backward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) -> ok | eof.
	// Description: Reads binary data from log in backward direction. Buffer is
	// overwritten from its beginning.
	//--------------------------------------------------------------------
	result read_backward(serial_buffer & buffer, uint64_t lsn) {
		uint64_t next_lsn;
		return read_backward(buffer, lsn, next_lsn);
	}
	virtual result read_backward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) = 0;

	//--------------------------------------------------------------------
	// Function: sync(uint64_t lsn).
	// Description: Ensures that all data upto given LSN are flushed and stored
	// in stable storage.
	//--------------------------------------------------------------------
	virtual void sync(uint64_t lsn) = 0;

	//--------------------------------------------------------------------
	// Function: log_size() -> uint64_t
	// Description: Returns size of log in bytes.
	//--------------------------------------------------------------------
	uint64_t log_size() {return get_tail() - get_head();}
};

//--------------------------------------------------------------------
// Basic logger implementation.
//--------------------------------------------------------------------
class blogger: public logger {
private:
	log_device * 	m_pdevice;			// Underlaying log device
	uint32_t			m_block_size;		// Effective block size
	uint64_t			m_head_lsn;			// Head of log
	uint64_t			m_checkpoint_lsn; // Transaction manager checkpoint LSN
	uint64_t			m_tail_lsn;			// Tail of log
	block_ptr			m_tail_pb;			// Current tail block
	uint64_t			m_tail_block;		// Tail block number
	uint32_t			m_tail_offset;	// Where to put next data into tail block.
	uint64_t			m_last_sync;		// Last synchronized LSN
	bool					m_tail_valid;		// is logger is valid? It is valid only
																// after find tail is finished.

	//--------------------------------------------------------------------
	// Function: find_tail().
	// Description: Finds tail of log on log initialization.
	//--------------------------------------------------------------------
	void find_tail();

	//--------------------------------------------------------------------
	// Function: result read_exact(serial_buffer & buffer, uint32_t size, uint64_t & lsn) -> ok | eof.
	// Description: Reads exact number of bytes from log device at given LSN and
	// updates lsn to point on next data.
	//--------------------------------------------------------------------
	result read_exact(serial_buffer & buffer, uint32_t size, uint64_t & lsn);

	//--------------------------------------------------------------------
	// Function: write_exact(const void * pbuffer, uint32_t size).
	// Description: Writes exact number of bytes to log device.
	//--------------------------------------------------------------------
	void write_exact(const void * pdata, uint32_t size);

	//--------------------------------------------------------------------
	// Function: result read_forward_internal(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn).
	// Description: Same as read_forward, but does not tests eof. Used by find tail.
	//--------------------------------------------------------------------
	result read_forward_internal(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn);

public:
	//--------------------------------------------------------------------
	// Function: blogger().
	// Description: Constructor.
	//--------------------------------------------------------------------
	blogger() {
		m_pdevice = NULL;
		m_block_size = 0;
		m_tail_block = 0;
		m_tail_offset = 0;
		m_tail_valid = false;
		m_head_lsn = m_tail_lsn = m_last_sync = 0;
		m_checkpoint_lsn = 0;
	}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the logger.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {log_device, Name} - underlaying log device.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & log_device);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual void stats(no_stats_list & list);

	// see base class.
	virtual uint64_t get_head();

	// see base class
	virtual uint64_t get_checkpoint();

	// see base class.
	virtual void set_head(uint64_t head_lsn, uint64_t checkpoint_lsn);

	// see base class.
	virtual uint64_t get_tail();

	// see base class.
	virtual uint64_t write(const void * pdata, uint32_t size, uint64_t & redo_lsn);

	// see base class.
	virtual result read_forward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn);

	// see base class.
	virtual result read_backward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn);

	// see base class.
	virtual void sync(uint64_t lsn);
};

#endif // __logger_hpp__

