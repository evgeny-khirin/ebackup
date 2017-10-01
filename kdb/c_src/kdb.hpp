///-------------------------------------------------------------------
/// Copyright (c) 2002-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : kdb.hpp
/// Author  : Evgeny Khirin <>
/// Description : Database interface.
///-------------------------------------------------------------------
#ifndef __kdb_hpp__
#define __kdb_hpp__

#include "btree.hpp"
#include "thread_pool.hpp"

//--------------------------------------------------------------------
// Database interface.
//--------------------------------------------------------------------
class kdb: public resource_mgr {
private:
	struct state_t {
		uint64_t						m_root;			// root block of root directory
		uint64_t						m_open;			// root block of opened tables
		uint64_t						m_dropped;	// root block of dropped tables

		state_t() {
			m_root = m_open = m_dropped = UINT64_MAX;
		}
	};

private:
	trans_mgr *						m_tm;
	buffer_mgr *					m_bm;
	stm *									m_stm;
	btree_rm *						m_btree_rm;
	state_t								m_state;
	std::auto_ptr<btree>	m_root;
	std::auto_ptr<btree>	m_open;
	std::auto_ptr<btree>	m_dropped;
	thread_pool						m_thread_pool;
	bool									m_stopped;

	//--------------------------------------------------------------------
	// Function: reclaimer(kdb * db).
	// Description: Reclaims in background space occupied by dropped tables.
	//--------------------------------------------------------------------
	static void reclaimer(kdb * db);
	void reclaimer();

public:
	//--------------------------------------------------------------------
	// Function: kdb().
	// Description: Constructor.
	//--------------------------------------------------------------------
	kdb() {
		m_tm = NULL;
		m_bm = NULL;
		m_stm = NULL;
		m_btree_rm = NULL;
		m_stopped = false;
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
	//   {transaction_manager, Name} - transaction manager.
	//   {buffer_manager, Name} - buffer manager.
	//   {storage_manager, Name} - storage manager.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & trans_mgr,
										const std::string & buffer_mgr, const std::string & stm);

	// see base class
	virtual void stop();

	// see base class
	virtual void del_underlaying();

	// see base class
	virtual void stats(no_stats_list & list);

	// see base class
	virtual void undo(uint64_t n, const void * data, uint32_t size, void * opaque);

	// see base class
	virtual void redo(uint64_t n, bool state_only, const void * data, uint32_t size, uint64_t redo_lsn);

	// see base class
	virtual void checkpoint();

	// see base class
	virtual void recover_state(const void * data, uint32_t size, uint64_t redo_lsn);

	// see base class
	virtual void recover_finished();

	//--------------------------------------------------------------------
	// Function: open(const term_ptr & name) -> btree *
	// Description: Opens table by name. If table does not exist, it is
	// created.
	//--------------------------------------------------------------------
	btree * open(const term_ptr & name);

	//--------------------------------------------------------------------
	// Function: close(btree * table)
	// Description: Closes opened table.
	//--------------------------------------------------------------------
	void close(btree * table);

	//--------------------------------------------------------------------
	// Function: drop(const term_ptr & name)
	// Description: Deletes table.
	//--------------------------------------------------------------------
	void drop(const term_ptr & name);

	//--------------------------------------------------------------------
	// Function: recover()
	// Description: Transaction manager API.
	//--------------------------------------------------------------------
	void recover() {m_tm->recover();}

	//--------------------------------------------------------------------
	// Function: begin_transaction(trans_type_t type) -> uint32_t
	// Description: Transaction manager API.
	//--------------------------------------------------------------------
	uint32_t begin_transaction(trans_type_t type) {return m_tm->begin_transaction(type);}

	//--------------------------------------------------------------------
	// Function: commit(uint32_t id)
	// Description: Transaction manager API.
	//--------------------------------------------------------------------
	void commit(uint32_t id) {m_tm->commit(id);}

	//--------------------------------------------------------------------
	// Function: rollback(uint32_t id).
	// Description: Transaction manager API.
	//--------------------------------------------------------------------
	void rollback(uint32_t id) {m_tm->rollback(id);}

	//--------------------------------------------------------------------
	// Function: used().
	// Description: Starage manager API.
	//--------------------------------------------------------------------
	uint64_t used() {return m_stm->used();}

	//--------------------------------------------------------------------
	// Function: root_begin().
	// Description: Returns begin iterator for root table. Must be called within
	// transaction.
	//--------------------------------------------------------------------
	btree::iterator root_begin() {return m_root->begin();}
};

#endif // __kdb_hpp__

