///-------------------------------------------------------------------
/// Copyright (c) 2002-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : kdb.cpp
/// Author  : Evgeny Khirin <>
/// Description : Database interface.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#define __STDC_CONSTANT_MACROS
#include <stdint.h>
#include <assert.h>

#include "kdb.hpp"

//--------------------------------------------------------------------
// Global variables
//--------------------------------------------------------------------
static char g_nil_buf[1] = {106};
static term_ptr g_nil_term(new term(g_nil_buf, sizeof(g_nil_buf)));

//--------------------------------------------------------------------
// public function
// Supported options:
//   {transaction_manager, Name} - transaction manager.
//   {buffer_manager, Name} - buffer manager.
//   {storage_manager, Name} - storage manager.
//--------------------------------------------------------------------
void kdb::start(const opt_map & options) {
	resource_mgr::start(options);

	opt_map::const_iterator it;
	if ((it = options.find("transaction_manager")) == options.end()) {
		throw missed_param();
	}
	m_tm = (trans_mgr *)whereis(it->second);
	if (m_tm == NULL) {
		throw not_registered();
	}
	const std::string & tm_name = it->second;
	m_tm->register_rm(this);

	if ((it = options.find("buffer_manager")) == options.end()) {
		throw missed_param();
	}
	m_bm = (buffer_mgr *)whereis(it->second);
	if (m_bm == NULL) {
		throw not_registered();
	}
	const std::string & bm_name = it->second;

	if ((it = options.find("storage_manager")) == options.end()) {
		throw missed_param();
	}
	m_stm = (stm *)whereis(it->second);
	if (m_stm == NULL) {
		throw not_registered();
	}
	// Start B-tree RM
	std::string btree_rm_name(get_name());
	btree_rm_name += "_rm_";
	opt_map btree_opts;
	btree_opts["name"] = btree_rm_name;
	btree_opts["transaction_manager"] = tm_name;
	btree_opts["buffer_manager"] = bm_name;
	m_btree_rm = new btree_rm;
	m_btree_rm->start(btree_opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::start(const std::string & name, const std::string & trans_mgr,
										 const std::string & buffer_mgr, const std::string & stm) {
	printf("kdb: name %s, trans_mgr %s, buffer_mgr %s, stm %s\n",
				 name.c_str(), trans_mgr.c_str(), buffer_mgr.c_str(), stm.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["transaction_manager"] = trans_mgr;
	opts["buffer_manager"] = buffer_mgr;
	opts["storage_manager"] = stm;
	kdb * pkdb = new kdb;
	pkdb->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::stop() {
	m_stopped = true;
	try {
		m_tm->stop();
	} catch (kdb_exception &) {
	}
	try {
		m_bm->stop();
	} catch (kdb_exception &) {
	}
	try {
		m_stm->stop();
	} catch (kdb_exception &) {
	}
	try {
		m_btree_rm->stop();
	} catch (kdb_exception &) {
	}
	m_thread_pool.stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::del_underlaying() {
	if (!m_stopped) {
		m_stopped = true;
		m_thread_pool.stop();
	}
	m_tm->del_underlaying();
	m_bm->del_underlaying();
	m_stm->del_underlaying();
	m_btree_rm->del_underlaying();
	delete m_tm;
	delete m_bm;
	delete m_stm;
	delete m_btree_rm;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::stats(no_stats_list & list) {
	m_tm->stats(list);
	m_bm->stats(list);
	m_stm->stats(list);
	m_btree_rm->stats(list);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::checkpoint() {
	m_tm->log_rm_state(this, &m_state, sizeof(m_state));
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::recover_state(const void * data, uint32_t size, uint64_t redo_lsn) {
	assert(size == sizeof(m_state));
	m_state = *((state_t *)data);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::recover_finished() {
	if (m_state.m_root == UINT64_MAX) {
		// main tables are not created
		uint32_t id = m_tm->begin_transaction(soft);
		m_root.reset(btree::create(m_tm, m_bm, m_stm, m_btree_rm));
		m_open.reset(btree::create(m_tm, m_bm, m_stm, m_btree_rm));
		m_dropped.reset(btree::create(m_tm, m_bm, m_stm, m_btree_rm));
		m_state.m_root = m_root->root();
		m_state.m_open = m_open->root();
		m_state.m_dropped = m_dropped->root();
		m_tm->log_update(this, &m_state, sizeof(m_state));
		m_tm->commit(id);
	} else {
		// open main tables
		m_root.reset(btree::open(m_tm, m_bm, m_stm, m_btree_rm, m_state.m_root));
		m_open.reset(btree::open(m_tm, m_bm, m_stm, m_btree_rm, m_state.m_open));
		m_dropped.reset(btree::open(m_tm, m_bm, m_stm, m_btree_rm, m_state.m_dropped));
	}
	// clean open table
	m_open->clear();
	// start thread pool with background tasks
	m_thread_pool.start(1);
	m_thread_pool.spawn(boost::bind(boost::mem_fn(&kdb::reclaimer), this));
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::undo(uint64_t n, const void * data, uint32_t size, void * opaque) {
	assert(n == UINT64_MAX);
	assert(size == sizeof(m_state));
	m_state = state_t();
	m_tm->log_compensate(this, &m_state, sizeof(m_state), opaque);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::redo(uint64_t n, bool state_only, const void * data, uint32_t size, uint64_t redo_lsn) {
	assert(n == UINT64_MAX);
	assert(size == sizeof(m_state));
	m_state = *((state_t *)data);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
static term_ptr encode_term_64(uint64_t n) {
	unsigned char buf[12];
	buf[0] = 131;
	buf[1] = 110;
	buf[2] = 8;
	buf[3] = 0;
	*((uint64_t *)&buf[4]) = n;
	return term_ptr(new term(buf, sizeof(buf)));
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
static uint64_t decode_term_64(const term_ptr & t) {
	assert(t->size() == 12);
	const char * p = t->data();
	assert((uint8_t)*p == 131);
	p++;
	assert(*p == 110);
	p++;
	assert(*p == 8);
	p++;
	assert(*p == 0);
	p++;
	return *((uint64_t *)p);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
static term_ptr encode_term_32(uint32_t n) {
	unsigned char buf[8];
	buf[0] = 131;
	buf[1] = 110;
	buf[2] = 4;
	buf[3] = 0;
	*((uint32_t *)&buf[4]) = n;
	return term_ptr(new term(buf, sizeof(buf)));
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
static uint32_t decode_term_32(const term_ptr & t) {
	assert(t->size() == 8);
	const char * p = t->data();
	assert((uint8_t)*p == 131);
	p++;
	assert(*p == 110);
	p++;
	assert(*p == 4);
	p++;
	assert(*p == 0);
	p++;
	return *((uint32_t *)p);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
btree * kdb::open(const term_ptr & name) {
	std::auto_ptr<btree> table;
	uint32_t id = m_tm->begin_transaction(soft);
	term_ptr root_term;
	// lookup table by name
	switch (m_root->lookup(name, root_term)) {
	case ok:
		// table exist
		table.reset(btree::open(m_tm, m_bm, m_stm, m_btree_rm, decode_term_64(root_term)));
		break;
	case not_found:
		// table does not exist, create it and store in root
		table.reset(btree::create(m_tm, m_bm, m_stm, m_btree_rm));
		root_term = encode_term_64(table->root());
		m_root->insert(name, root_term);
		break;
	default:
		throw internal_error();
	}
	uint32_t ref_count;
	term_ptr ref_term;
	// get open reference count
	switch (m_open->lookup(root_term, ref_term)) {
	case ok:
		// table already opened
		ref_count = decode_term_32(ref_term) + 1;
		break;
	case not_found:
		// table opened first time
		ref_count = 1;
		break;
	default:
		throw internal_error();
	}
	// set open reference count
	m_open->insert(root_term, encode_term_32(ref_count));
	m_tm->commit(id);
	return table.release();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::close(btree * table) {
	uint32_t id = m_tm->begin_transaction(soft);
	uint32_t ref_count;
	term_ptr root_term = encode_term_64(table->root());
	term_ptr ref_term;
	// get open reference count
	switch (m_open->lookup(root_term, ref_term)) {
	case ok:
		ref_count = decode_term_32(ref_term) - 1;
		break;
	default:
		throw internal_error();
	}
	if (ref_count > 0) {
		// update reference count
		m_open->insert(root_term, encode_term_32(ref_count));
	} else {
		// remove table from opened table
		m_open->remove(root_term);
	}
	m_tm->commit(id);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void kdb::drop(const term_ptr & name) {
	uint32_t id = m_tm->begin_transaction(soft);
	bool exists;
	term_ptr root_term;
	// lookup table by name
	switch (m_root->lookup(name, root_term)) {
	case ok:
		exists = true;
		break;
	case not_found:
		exists = false;
		break;
	default:
		throw internal_error();
	}
	if (exists) {
		// just move table to dropped, real delete will be performed
		// by reclaimer in background
		m_dropped->insert(root_term, g_nil_term);
		m_root->remove(name);
	}
	m_tm->commit(id);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void kdb::reclaimer(kdb * db) {
	db->reclaimer();
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void kdb::reclaimer() {
	while (!m_stopped) {
		try {
			std::auto_ptr<btree> tree;
			term_ptr root_term;
			// Find tree for reclaiming
			uint32_t id = m_tm->begin_transaction(read_only);
			for (btree::iterator i = m_dropped->begin(); i.has_more(); i++) {
				root_term = i->first;
				term_ptr v;
				if (m_open->lookup(root_term, v) == not_found) {
					tree.reset(btree::open(m_tm, m_bm, m_stm, m_btree_rm, decode_term_64(root_term)));
					break;
				}
			}
			m_tm->commit(id);
			if (tree.get() == NULL) {
				// nothing to reclaim
				msleep(100);
				continue;
			}
			// clean tree first, NO TRANSACTIONS, see btree::clear.
			tree->clear();
			// finish reclaiming
			id = m_tm->begin_transaction(soft);
			tree->drop();
			m_dropped->remove(root_term);
			m_tm->commit(id);
		} catch (std::exception &) {
			if (!m_stopped) {
				msleep(100);
			}
		}
	}
}

