///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bm.hpp
/// Author  : Evgeny Khirin <>
/// Description : Buffer manager interface.
///-------------------------------------------------------------------
#ifndef __bm_hpp__
#define __bm_hpp__

#include <map>

#include "tm.hpp"
#include "lru_cache.hpp"

//--------------------------------------------------------------------
// stats
//--------------------------------------------------------------------
struct buffer_mgr_stats: public bd_stats {
	lru_stats *	m_cache_stats;

	buffer_mgr_stats() {m_cache_stats = NULL;}

	// see base class.
	virtual void soft_reset();

	// see base class.
	virtual std::string to_string();
};

//--------------------------------------------------------------------
// block decoded by resource manager
//--------------------------------------------------------------------
class decoded_block {
	friend class buffer_mgr;

private:
	uint64_t	m_redo_lsn;					// Block's low watter mark - first LSN
																// of block's update since it is
																// loaded in cache (redo LSN of first
																// write operation).
	uint64_t	m_lsn;							// Block's current LSN
	bool			m_decoded;					// used to distinguish decoded and encoded block
	bool			m_dirty;						// write operation is performed on block and
																// block is in dirty table.

public:
	decoded_block(bool decoded = true) {
		m_redo_lsn = UINT64_MAX;
		m_lsn = 0;
		m_decoded = decoded;
		m_dirty = false;
	}

	virtual ~decoded_block() {}

	virtual void serialize(serial_buffer & buf) = 0;

	bool is_decoded() {return m_decoded;}

	virtual void dump() {}

	void reset() {
		m_redo_lsn = UINT64_MAX;
		m_lsn = 0;
		m_dirty = false;
	}
};

//--------------------------------------------------------------------
// block, which is not decoded still
//--------------------------------------------------------------------
class encoded_block: public decoded_block {
private:
	block_ptr		m_pb;

	encoded_block();

public:
	encoded_block(block_ptr & pb): decoded_block(false) {m_pb = pb;}

	virtual void serialize(serial_buffer & buf);

	block_ptr & get() {return m_pb;}
};

//--------------------------------------------------------------------
// decoded block pointer
//--------------------------------------------------------------------
typedef linked_ptr<decoded_block>		decoded_block_ptr;

//--------------------------------------------------------------------
// Buffer manager.
//--------------------------------------------------------------------
class buffer_mgr: public named_object {
public:
	typedef decoded_block_ptr (*decode_fun)(uint64_t n, serial_buffer & buf);

private:
	typedef lru_cache<uint64_t, decoded_block_ptr> cache_t;
	// Maps block's redo LSN to block number
	typedef std::map<uint64_t, uint64_t>	dirty_map_t;

private:
	cache_t						m_cache;
	block_device *		m_bd;
	trans_mgr *				m_tm;
	buffer_mgr_stats	m_stats;
	uint32_t					m_block_size; 	// effective block size
	dirty_map_t				m_dirty_map;		// used for calculation of "low watter mark".

	//--------------------------------------------------------------------
	// Function: save_dirty(void * buffer_mgr, uint64_t & n, decoded_block_ptr & db).
	// Description: Callback for saving cache dirty items.
	//--------------------------------------------------------------------
	static void save_dirty(void * buffer_mgr, uint64_t & n, decoded_block_ptr & db);

public:
	//--------------------------------------------------------------------
	// Function: buffer_mgr().
	// Description: Constructor.
	//--------------------------------------------------------------------
	buffer_mgr() {
		m_bd = NULL;
		m_tm = NULL;
		m_block_size = 0;
		m_stats.m_cache_stats = m_cache.stats();
	}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the transaction manager.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {block_device, Name} - underlying block device.
	//   {transaction_manager, Name} - transaction manager.
	//   {cache_capacity, CacheCapacity} - cache capacity.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & block_device,
										const std::string & trans_mgr, uint32_t cache_capacity);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual void stats(no_stats_list & list);

	//--------------------------------------------------------------------
	// Function: checkpoint(uint64_t log_head) -> uint64_t
	// Description: Notifies buffer manager that checkpoint is taken by transaction
	// manager. Buffer manager must flash dirty blocks if they prevent moving
	// log head forward.
	// Returns buffer manager's low watter mark - recovery LSN of oldest dirty block.
	//--------------------------------------------------------------------
	uint64_t checkpoint(uint64_t log_head);

	//--------------------------------------------------------------------
	// Function: recover_dirty(uint64_t n, block_ptr & pb, uint64_t block_lsn, uint64_t redo_lsn)
	// Description: Recovers dirty block from log, previosly saved with
	// tm::log_dirty function.
	//--------------------------------------------------------------------
	void recover_dirty(uint64_t n, block_ptr & pb, uint64_t block_lsn, uint64_t redo_lsn);

	//--------------------------------------------------------------------
	// Function: write(uint64_t n, decoded_block_ptr & pb, uint64_t redo_lsn).
	// Description: Writes block to device. Block numbers start from 0.
	// Buffer manager assumes that resource manager already logged all necessary
	// information for rollback and recovery assotiated with that block and
	// passed last Lsn, returned by log_update or log_compensate functions
	// or received in redo callback.  So no log entry added by buffer manager.
	//--------------------------------------------------------------------
	void write(uint64_t n, decoded_block_ptr & pb, uint64_t redo_lsn);

	//--------------------------------------------------------------------
	// Function: read(uint64_t n, decode_fun decoder) -> decoded_block_ptr
	// Description: Reads block from device. Block numbers start from 0.
	//--------------------------------------------------------------------
	decoded_block_ptr read(uint64_t n, decode_fun decoder);

	//--------------------------------------------------------------------
	// Function: read_lsn(uint64_t n) -> uint64_t
	// Description: Reads block's redo LSN. Block numbers start from 0.
	// Returns recover LSN of block. Returns 0 if LSN can not be read.
	//--------------------------------------------------------------------
	uint64_t read_lsn(uint64_t n);

	//--------------------------------------------------------------------
	// Function: lock(uint64_t n)
	// Description: Lock block in cache. Locked blocks are not flushed.
	// Write operation unlocks block.
	// Block numbers start from 0.
	//--------------------------------------------------------------------
	void lock(uint64_t n) {m_cache.lock(n);}

	//--------------------------------------------------------------------
	// Function: block_size() -> uint32_t
	// Description: Returns block size of device.
	//--------------------------------------------------------------------
	uint32_t block_size() {return m_block_size;}

	//--------------------------------------------------------------------
	// Function: capacity() -> uint64_t
	// Description: Returns capacity of device.
	//--------------------------------------------------------------------
	uint64_t capacity() {return m_bd->capacity();}

	//--------------------------------------------------------------------
	// Function: alloc() -> block_ptr
	// Description: Allocates block.
	//--------------------------------------------------------------------
	block_ptr alloc() {return m_bd->alloc();}

	//--------------------------------------------------------------------
	// Function: sync()
	// Description: Flushes cache to device. Called by transaction manager
	// as part of stop process.
	//--------------------------------------------------------------------
	void sync() {m_cache.sync(); m_bd->sync();}
};

#endif // __bm_hpp__

