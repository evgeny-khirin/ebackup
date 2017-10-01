///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : tm.hpp
/// Author  : Evgeny Khirin <>
/// Description : Transaction manager interface.
///-------------------------------------------------------------------
#ifndef __tm_hpp__
#define __tm_hpp__

#include <stdint.h>
#include <list>
#ifdef _WIN32
#include <map>
#else
#include <tr1/unordered_map>
#endif

#include "logger.hpp"
#include "rm.hpp"
#include "mutex.hpp"

//--------------------------------------------------------------------
// Forward declaration
//--------------------------------------------------------------------
class buffer_mgr;

//--------------------------------------------------------------------
// Transactions types.
//--------------------------------------------------------------------
enum trans_type_t {
	hard,				// usual transaction with ACID properties.
	soft,				// transaction, wich does not guarantees durability property of ACID.
							// those transactions are much faster than hard. Note, that
							// nested hard transaction behaves like soft.
	read_only		// read-only transaction: no logging is allowed for that and
							// nested transactions.
};

//--------------------------------------------------------------------
// Transaction manager.
//--------------------------------------------------------------------
class trans_mgr: public named_object {
private:
	struct txn_t {
		uint64_t			m_last_lsn;				// last undo LSN.
		uint64_t			m_start_lsn;			// LSN of transaction start. Valid
																		// in top level transactions only.
		trans_type_t	m_type;						// transaction type
		uint32_t			m_id;							// transaction id (nesting level)

		txn_t() {
			m_type = read_only;
			m_id = UINT32_MAX;
			m_last_lsn = 0;
			m_start_lsn = UINT64_MAX;
		}

		txn_t(trans_type_t	type) {
			m_last_lsn = 0;
			m_start_lsn = 0;
			m_type = type;
			m_id = 0;
			m_start_lsn = UINT64_MAX;
		}
	};

	typedef std::list<resource_mgr *>			rm_list;			// resource managers list
	struct dirty_descr_t {
		uint64_t	m_lsn;
		uint64_t	m_load_lsn;
	};
	// Improves performance of recovery process
#ifdef _WIN32
	typedef std::map<uint64_t, dirty_descr_t>	dirty_map_t;	
#else
	typedef std::tr1::unordered_map<uint64_t, dirty_descr_t>	dirty_map_t;	
#endif

private:
	uint64_t					m_cp_log_size;				// size of log for checkpoint storing
	logger *					m_logger;							// attached logger
	buffer_mgr *			m_bm;									// attached buffer manager
	std::string				m_bm_name;
	std::list<txn_t>	m_txn_stack;					// transactions stack
	rm_list						m_rm_list;						// set of known resource managers
	rmutex						m_mutex;							// transactions serialization mutex.
	bool							m_recovered;
	bool							m_checkpoint_disabled;

	//--------------------------------------------------------------------
	// Function: write(const void * pdata, uint32_t size, uint64_t & redo_lsn) -> uint64_t
	// Description: Writes binary data to log. If size of log exceeds
	// m_cp_log_size, than checkpoint is stored and log head is ajusted.
	// Returns sync LSN for the data (LSN past the data).
	//--------------------------------------------------------------------
	uint64_t write_log(const void * pdata, uint32_t size, uint64_t & redo_lsn);

	//--------------------------------------------------------------------
	// Function: checkpoint()
	// Description: Saves checkpoint.
	//--------------------------------------------------------------------
	void checkpoint();

	//--------------------------------------------------------------------
	// Function: build_dirty_map(dirty_map_t & dirty_map)
	// Description: Builds map of dirty blocks in log.
	//--------------------------------------------------------------------
	void build_dirty_map(dirty_map_t & dirty_map);

	//--------------------------------------------------------------------
	// Function: recover_checkpoint() -> uint64_t
	// Description: Recovers checkpoint.
	// Returns low water mark.
	//--------------------------------------------------------------------
	uint64_t recover_checkpoint();

	//--------------------------------------------------------------------
	// Function: redo_log(uint64_t low_water_mark, const dirty_map_t & dirty_map)
	// Description: Redoes log and creates stack of incomplete transactions.
	//--------------------------------------------------------------------
	void redo_log(uint64_t low_water_mark, const dirty_map_t & dirty_map);

	//--------------------------------------------------------------------
	// Function: recover_block(uint64_t n, const dirty_map_t & dirty_map) -> uint64_t
	// Description: Tries to recover dirty block saved with log_dirty function.
	// Returns recovered block LSN or 0 if block can not be recovered.
	//--------------------------------------------------------------------
	uint64_t recover_block(uint64_t n, const dirty_map_t & dirty_map);

	//--------------------------------------------------------------------
	// Function: rollback(txn_t & txn).
	// Description: Rolls back single transaction.
	//--------------------------------------------------------------------
	void rollback(txn_t & txn);

	//--------------------------------------------------------------------
	// Function: rollback_loosers()
	// Description: Rolls back all incompleted transactions.
	//--------------------------------------------------------------------
	void rollback_loosers();

public:
	//--------------------------------------------------------------------
	// Function: trans_mgr().
	// Description: Constructor.
	//--------------------------------------------------------------------
	trans_mgr() {
		m_cp_log_size = 0;
		m_logger = NULL;
		m_bm = NULL;
		m_recovered = false;
		m_checkpoint_disabled = false;
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
	//   {logger, Name} - attached logger.
	//   {buffer_manager, Name} - attached buffer manager.
	//   {cp_log_size, Bytes} - size of log when checkpoint is stored.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & logger,
										const std::string & bm, uint64_t cp_log_size);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual void stats(no_stats_list & list);

	//--------------------------------------------------------------------
	// Function: register_rm(resource_mgr * prm)
	// Description: Registers resource manager in transaction manager. Resource
	// manager must do that on in its start function. So that transaction manager
	// can save checkpoints correctly.
	//--------------------------------------------------------------------
	void register_rm(resource_mgr * rm);

	//--------------------------------------------------------------------
	// Function: recover()
	// Description: Executes recover process. Invoke this function immediately after
	// starting all resource managers and before any other actions.
	//--------------------------------------------------------------------
	void recover();

	//--------------------------------------------------------------------
	// Function: log_update(resource_mgr * rm, uint64_t n, const void * data, uint32_t size) -> uint64_t
	// Description: Writes log update record. Resource manager must be capabale to
	// perform undo and redo operations using this record.
	//    rm = resource manager, writing the record. Name of resource manager is
	//       stored by TM in log and than used in recovery and rollback processes
	//       for resolving of reource manager.
	//    n = block number
	//    data = update data.
	// 		size = size of update data.
	// Returns redo LSN associated with the record.
	//--------------------------------------------------------------------
	uint64_t log_update(resource_mgr * rm, uint64_t n, const void * data, uint32_t size);
	uint64_t log_update(resource_mgr * rm, const void * data, uint32_t size) {
		// Logs update operation not related to any block
		return log_update(rm, UINT64_MAX, data, size);
	}

	//--------------------------------------------------------------------
	// Function: log_compensate(resource_mgr * rm, const void * data, uint32_t size,
	//                          void * opaque) -> uint64_t
	// Description: Writes log compensate record. Resource manager must be capabale to
	// perform redo operation using this record. Called by resource manager from
	// its undo method, in order to write compensation record.
	//    rm = resource manager, writing the record. Name of resource manager is
	//       stored by TM in log and than used in recovery and rollback processes
	//       for resolving of reource manager.
	//    n = block number
	//    data = compensation data.
	//    size = size of compensation data.
	//    opaque = opaque parameter of rm:undo function.
	// Returns redo LSN associated with the record.
	//--------------------------------------------------------------------
	uint64_t log_compensate(resource_mgr * rm, uint64_t n, const void * data, uint32_t size, void * opaque);
	uint64_t log_compensate(resource_mgr * rm, const void * data, uint32_t size, void * opaque) {
		// Logs compensate operation not related to any block
		return log_compensate(rm, UINT64_MAX, data, size, opaque);
	}

	//--------------------------------------------------------------------
	// Function: log_rm_state(resource_mgr * rm, const void * data, uint32_t size)
	// Description: Saves in log resource manager's state.
	//    rm = resource manager, writing the record. Name of resource manager is
	//       stored by TM in log and than used in recovery and rollback processes
	//       for resolving of reource manager.
	//    data = state data.
	//    size = size of state data.
	//--------------------------------------------------------------------
	void log_rm_state(resource_mgr * rm, const void * data, uint32_t size);

	//--------------------------------------------------------------------
	// Function: log_dirty(uint64_t n, block_ptr & pb, uint32_t block_size, uint64_t lsn, uint64_t redo_lsn)
	// Description: Writes dirty block to log as redo only record and syncs
	// logger. Called by buffer manager as part of save dirty process.
	//    n = block number
	//    pb = block pointer
	//    block_size = effective block size.
	//    lsn = block's LSN
	//    redo_lsn = block's redo LSN
	//--------------------------------------------------------------------
	void log_dirty(uint64_t n, block_ptr & pb, uint32_t block_size, uint64_t lsn, uint64_t redo_lsn);

	//--------------------------------------------------------------------
	// Function: begin_transaction(transaction_type type) -> uint32_t
	// Description: Starts new transaction in current thread. Returns
	// transaction ID.
	//--------------------------------------------------------------------
	uint32_t begin_transaction(trans_type_t type);

	//--------------------------------------------------------------------
	// Function: commit(uint32_t id).
	// Description: Commits thread transaction. Effectively commits all nested
	// transactions as well.
	//--------------------------------------------------------------------
	void commit(uint32_t id);

	//--------------------------------------------------------------------
	// Function: rollback(uint32_t id).
	// Description: Rolls current thread transaction. Effectively rolls back
	// all nested transactions as well.
	//--------------------------------------------------------------------
	void rollback(uint32_t id);

	//--------------------------------------------------------------------
	// Function: log_size() -> uint64_t
	// Description: Returns log size.
	//--------------------------------------------------------------------
	uint64_t log_size() {return m_logger->log_size();}
};

#endif // __tm_hpp__

