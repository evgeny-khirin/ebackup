///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : stm.cpp
/// Author  : Evgeny Khirin <>
/// Description : Storage manager based on intervals list.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <assert.h>
#include "stm.hpp"

namespace {
	//--------------------------------------------------------------------
	// undo/redo signatures
	//--------------------------------------------------------------------
	#define REMOVE					1
	#define INSERT					2
	#define NEW_NODE				3
	#define CHANGE_HEAD			4
	#define INCR_CAPACITY		5
	#define DECR_CAPACITY		6

	//--------------------------------------------------------------------
	// log record
	//--------------------------------------------------------------------
	struct log_rec {
		char	m_sign;
	};

	//--------------------------------------------------------------------
	// remove record - block removed from node
	//--------------------------------------------------------------------
	struct remove_rec: public log_rec {
		uint64_t	m_block;

		remove_rec() {m_sign = REMOVE;}
	};

	//--------------------------------------------------------------------
	// insert record - block inserted into node
	//--------------------------------------------------------------------
	struct insert_rec: public log_rec {
		uint64_t	m_block;

		insert_rec() {m_sign = INSERT;}
	};

	//--------------------------------------------------------------------
	// new node record
	//--------------------------------------------------------------------
	struct new_node_rec: public log_rec {
		uint64_t m_next;

		new_node_rec() {m_sign = NEW_NODE;}
	};

	//--------------------------------------------------------------------
	// change head record
	//--------------------------------------------------------------------
	struct change_head_rec: public log_rec {
		uint64_t	m_old_head;
		uint64_t	m_new_head;
		uint64_t	m_old_used;
		uint64_t	m_new_used;
		bool			m_save_on_undo;

		change_head_rec() {m_sign = CHANGE_HEAD;}
	};

	//--------------------------------------------------------------------
	// increase capacity record
	//--------------------------------------------------------------------
	struct incr_capacity_rec: public log_rec {
		uint64_t	m_old_capacity;
		uint64_t	m_new_capacity;

		incr_capacity_rec() {m_sign = INCR_CAPACITY;}
	};

	//--------------------------------------------------------------------
	// decrease capacity record
	//--------------------------------------------------------------------
	struct decr_capacity_rec: public log_rec {
		uint64_t	m_capacity;

		decr_capacity_rec() {m_sign = DECR_CAPACITY;}
	};
} // namespace

//--------------------------------------------------------------------
// public function
// Supported options:
//   {transaction_manager, Name} - transaction manager.
//   {buffer_manager, Name} - buffer manager.
//--------------------------------------------------------------------
void stm::start(const opt_map & options) {
	resource_mgr::start(options);

	opt_map::const_iterator it;
	if ((it = options.find("transaction_manager")) == options.end()) {
		throw missed_param();
	}
	m_tm = (trans_mgr *)whereis(it->second);
	if (m_tm == NULL) {
		throw not_registered();
	}
	m_tm->register_rm(this);

	if ((it = options.find("buffer_manager")) == options.end()) {
		throw missed_param();
	}
	m_bm = (buffer_mgr *)whereis(it->second);
	if (m_bm == NULL) {
		throw not_registered();
	}

	// calculate max number of intervals in node
	uint32_t block_size = m_bm->block_size();
	// Node reserved space:
	//    8 - pointer to next node
	//    4 - number of intervals in node
	uint32_t reserved = 8 + 4;
	uint32_t interval_size = 8 + 8;	// {start, len} both 64-bits.
	m_intervals_in_node = (block_size - reserved) / interval_size;
	if (m_intervals_in_node == 0) {
		throw block_too_small();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::start(const std::string & name, const std::string & trans_mgr,
								const std::string & buffer_mgr) {
	printf("stm: name %s, trans_mgr %s, buffer_mgr %s\n",
				 name.c_str(), trans_mgr.c_str(), buffer_mgr.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["transaction_manager"] = trans_mgr;
	opts["buffer_manager"] = buffer_mgr;
	stm * pstm = new stm;
	pstm->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::stop() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::del_underlaying() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::stats(no_stats_list & list) {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t stm::alloc() {
//	printf("stm::alloc: started\n");
	uint32_t id = m_tm->begin_transaction(soft);
	if (m_used == m_capacity) {
		check_capacity();
		if (m_used == m_capacity) {
			throw no_free_space();
		}
	}
	uint64_t n = alloc_internal();
	m_tm->commit(id);
//	printf("stm::alloc: finished, block %llu\n", n);
	return n;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t stm::alloc_internal() {
	decoded_block_ptr pb = m_bm->read(m_head, node_decoder);
	node_t & head = (node_t &)*pb;
	interval_map & m = head.m_map;
	if (m.empty()) {
		// allocate head itself
		uint64_t n = m_head;
		// log update BEFORE real node change, because if node is dirty, it can be
		// flushed by buffer manager on log_update, if transaction manager decide
		// to take checkpoint.
		change_head_rec rec;
		rec.m_old_head = m_head;
		rec.m_new_head = head.m_next;
		rec.m_old_used = m_used;
		rec.m_new_used = m_used + 1;
		rec.m_save_on_undo = true;
		m_tm->log_update(this, &rec, sizeof(rec));
//		printf("--- stm::alloc_internal: change head, old: used %llu, head %llu, new: used %llu, head %llu, redo_lsn %llu\n",
//					 rec.m_old_used, rec.m_old_head, rec.m_new_used, rec.m_new_head, redo_lsn);
		// update header
		m_head = rec.m_new_head;
		m_used = rec.m_new_used;
		return n;
	}
	// allocate first block from first interval
	interval_map::iterator i = m.begin();
	uint64_t n = i->first;
	uint64_t len = i->second;
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	remove_rec rec;
	rec.m_block = n;
	uint64_t redo_lsn = m_tm->log_update(this, m_head, &rec, sizeof(rec));
	m_used++;
//	printf("--- stm::alloc_internal: remove node %llu, block %llu, redo_lsn %llu\n",
//				 m_head, n, redo_lsn);
	// erase interval
	m.erase(i);
	// if interval has more blocks, insert new interval
	if (len > 1) {
		m[n + 1] = len - 1;
	}
	// save node
	m_bm->write(m_head, pb, redo_lsn);
	return n;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void stm::free_internal(uint64_t n) {
	if (m_head == UINT64_MAX) {
		// there is no head - device completely allocated. Create new head.
		new_head(n);
		return;
	}
	decoded_block_ptr pb = m_bm->read(m_head, node_decoder);
	node_t & head = (node_t &)*pb;
	uint64_t redo_lsn = insert(head, m_head, n, true);
	if (redo_lsn > 0) {
		// block inserted into current head
		m_used--;
		// save head
		m_bm->write(m_head, pb, redo_lsn);
		return;
	}
	// block can not be inserted into current head, create new
	new_head(n);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
uint64_t stm::insert(node_t & node, uint64_t node_n, uint64_t n, bool do_log) {
	uint64_t redo_lsn = 1;
	interval_map & m = node.m_map;
	interval_map::iterator i = m.lower_bound(n);
	// try to merge n in upper interval.
	if (i != m.end() && i->first == n + 1) {
		// n can be mereged in upper interval.
		if (i != m.begin()) {
			// check, may be n connects two intervals
			interval_map::iterator j = i;
			j--;
			if (j->first + j->second == n) {
				// yes n connects two intervals
				if (do_log) {
					// log update BEFORE real node change, because if node is dirty, it can be
					// flushed by buffer manager on log_update, if transaction manager decide
					// to take checkpoint.
					insert_rec rec;
					rec.m_block = n;
					redo_lsn = m_tm->log_update(this, node_n, &rec, sizeof(rec));
//					printf("--- stm::insert: node %llu, block %llu, redo_lsn %llu\n", node_n, n, redo_lsn);
				}
				m[j->first] = j->second + i->second + 1;
				m.erase(i);
				return redo_lsn;
			}
		}
		// n does not connect interval, update upper interval.
		if (do_log) {
			// log update BEFORE real node change, because if node is dirty, it can be
			// flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			insert_rec rec;
			rec.m_block = n;
			redo_lsn = m_tm->log_update(this, node_n, &rec, sizeof(rec));
//			printf("--- stm::insert: node %llu, block %llu, redo_lsn %llu\n", node_n, n, redo_lsn);
		}
		m[n] = i->second + 1;
		m.erase(i);
		return redo_lsn;
	}
	// is there lower interval?
	if (i == m.begin()) {
		// there is no lower interval
		if (m.size() == m_intervals_in_node) {
			// head is full, can not insert
			return 0;
		}
		// put new interval in node
		if (do_log) {
			// log update BEFORE real node change, because if node is dirty, it can be
			// flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			insert_rec rec;
			rec.m_block = n;
			redo_lsn = m_tm->log_update(this, node_n, &rec, sizeof(rec));
//			printf("--- stm::insert: node %llu, block %llu, redo_lsn %llu\n", node_n, n, redo_lsn);
		}
		m[n] = 1;
		return redo_lsn;
	}
	// try to merge with lower interval
	i--;
	if (i->first + i->second == n) {
		// n can be merged into lower interval
		if (do_log) {
			// log update BEFORE real node change, because if node is dirty, it can be
			// flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			insert_rec rec;
			rec.m_block = n;
			redo_lsn = m_tm->log_update(this, node_n, &rec, sizeof(rec));
//			printf("--- stm::insert: node %llu, block %llu, redo_lsn %llu\n", node_n, n, redo_lsn);
		}
		m[i->first] = i->second + 1;
		return redo_lsn;
	}
	// n can not be merged
	if (m.size() == m_intervals_in_node) {
		// head is full, can not insert
		return 0;
	}
	// put new interval in node
	if (do_log) {
		// log update BEFORE real node change, because if node is dirty, it can be
		// flushed by buffer manager on log_update, if transaction manager decide
		// to take checkpoint.
		insert_rec rec;
		rec.m_block = n;
		redo_lsn = m_tm->log_update(this, node_n, &rec, sizeof(rec));
//		printf("--- stm::insert: node %llu, block %llu, redo_lsn %llu\n", node_n, n, redo_lsn);
	}
	m[n] = 1;
	return redo_lsn;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void stm::remove(node_t & node, uint64_t n) {
	// check may be n is start of interval.
	interval_map & m = node.m_map;
	interval_map::iterator i = m.lower_bound(n);
	if (i != m.end() && i->first == n) {
		uint64_t len = i->second;
		m.erase(i);
		if (len > 1) {
			m[n + 1] = len - 1;
		}
		return;
	}
	// n must be in middle or in end of lower interval
	assert(
		// there is no lower interval
		i != m.begin());
	i--;
	uint64_t start = i->first;
	uint64_t len = i->second;
	assert(
		// lower interval does not contain n
		n < start + len);
	// split interval
	m[start] = n - start;
	// calc len of second interval without n
	len = start + len - n - 1;
	if (len > 0) {
		m[n + 1] = len;
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void stm::new_head(uint64_t n) {
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	change_head_rec hrec;
	hrec.m_old_head = m_head;
	hrec.m_new_head = n;
	hrec.m_old_used = m_used;
	hrec.m_new_used = m_used - 1;
	hrec.m_save_on_undo = false;
	m_tm->log_update(this, &hrec, sizeof(hrec));
//	printf("--- stm::new_head: change head, old: used %llu, head %llu, new: used %llu, head %llu, redo_lsn %llu\n",
//				 hrec.m_old_used, hrec.m_old_head, hrec.m_new_used, hrec.m_new_head, redo_lsn_1);
	// update header
	m_used = hrec.m_new_used;
	m_head = hrec.m_new_head;
	// create new head
	decoded_block_ptr pb(new node_t(hrec.m_old_head));
	// log operation
	new_node_rec rec;
	rec.m_next = hrec.m_old_head;
	uint64_t redo_lsn = m_tm->log_update(this, n, &rec, sizeof(rec));
//	printf("--- stm::new_head: new node %llu, next %llu, redo_lsn %llu\n",
//				 n, rec.m_next, redo_lsn);
	// save head
	m_bm->write(m_head, pb, redo_lsn);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::free(uint64_t n) {
//	printf("stm::free: started, block %llu\n", n);
	uint32_t id = m_tm->begin_transaction(soft);
//	block_ptr pb = m_bm->alloc();
//	decoded_block_ptr db = decoded_block_ptr(new encoded_block(pb));
//	char * b = pb.prepare_write();
//	memset(b, 0xcc, m_bm->block_size());
//	m_bm->write(n, db);
	free_internal(n);
	m_tm->commit(id);
//	printf("stm::free: finished\n");
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void stm::check_capacity() {
	uint64_t new_capacity = m_bm->capacity();
	if (new_capacity <= m_capacity) {
		// capacity did not change
		return;
	}
	// check_capacity function called only when threre is no free space
	// left. So first block of new storage become new head of free blocks
	// list.
	//
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	// log header update
	incr_capacity_rec rec;
	rec.m_old_capacity = m_capacity;
	rec.m_new_capacity = new_capacity;
	uint64_t redo_lsn = m_tm->log_update(this, m_capacity, &rec, sizeof(rec));
	// create new head
	m_head = m_capacity;
	m_capacity = new_capacity;
	node_t * head = new node_t;
	decoded_block_ptr pb(head);
	uint64_t start = m_head + 1;
	uint64_t len = new_capacity - start;
	if (len > 0) {
		head->m_map[start] = len;
	}
	// save head
	m_bm->write(m_head, pb, redo_lsn);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
decoded_block_ptr stm::node_decoder(uint64_t n, serial_buffer & buf) {
	node_t * head = new node_t();
	decoded_block_ptr pb(head);
	head->deserialize(buf);
	return pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::undo(uint64_t n, const void * data, uint32_t size, void * opaque) {
	switch (*((char *)data)) {
	case REMOVE:
		{
			remove_rec * rec = (remove_rec *)data;
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			insert_rec crec;
			crec.m_block = rec->m_block;
			uint64_t redo_lsn = m_tm->log_compensate(this, n, &crec, sizeof(crec), opaque);
//			printf("--- stm::undo: REMOVE node %llu, block %llu, redo_lsn %llu\n",
//						 n, rec->m_block, redo_lsn);
			// update metadata
			m_used--;
			// undo operation
			decoded_block_ptr pb = m_bm->read(n, node_decoder);
			node_t & node = (node_t &)*pb;
			uint64_t updated_lsn = insert(node, n, rec->m_block, false);
			assert(updated_lsn > 0);
			(void)updated_lsn;
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case INSERT:
		{
			insert_rec * rec = (insert_rec *)data;
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			remove_rec crec;
			crec.m_block = rec->m_block;
			uint64_t redo_lsn = m_tm->log_compensate(this, n, &crec, sizeof(crec), opaque);
//			printf("--- stm::undo: INSERT node %llu, block %llu, redo_lsn %llu\n",
//						 n, rec->m_block, redo_lsn);
			// update metadata
			m_used++;
			// undo operation
			decoded_block_ptr pb = m_bm->read(n, node_decoder);
			node_t & node = (node_t &)*pb;
			remove(node, rec->m_block);
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case NEW_NODE:
		// nothing to undo
		return;
	case CHANGE_HEAD:
		{
			change_head_rec * rec = (change_head_rec *)data;
//			printf("--- stm::undo: CHANGE_HEAD old: used %llu, head %llu, new: used %llu, head %llu, save on undo %d\n",
//						 rec->m_old_used, rec->m_old_head, rec->m_new_used, rec->m_new_head, (int)rec->m_save_on_undo);
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			change_head_rec crec;
			crec.m_old_used = UINT64_MAX;
			crec.m_new_used = rec->m_old_used;
			crec.m_old_head = UINT64_MAX;
			crec.m_new_head = rec->m_old_head;
			crec.m_save_on_undo = false;
			m_tm->log_compensate(this, &crec, sizeof(crec), opaque);
			// update metadata
			m_used = rec->m_old_used;
			m_head = rec->m_old_head;
			// Save new head
			if (rec->m_save_on_undo) {
				// new head was next pointer in old head.
				node_t * head = new node_t(rec->m_new_head);
				decoded_block_ptr pb(head);
				// write compensation record BEFORE real node change, because if node is dirty,
				// it can be flushed by buffer manager on log_update, if transaction manager decide
				// to take checkpoint.
				new_node_rec new_rec;
				new_rec.m_next = rec->m_new_head;
				uint64_t redo_lsn = m_tm->log_compensate(this, rec->m_old_head, &new_rec, sizeof(new_rec), opaque);
				m_bm->write(rec->m_old_head, pb, redo_lsn);
			}
		}
		return;
	case INCR_CAPACITY:
		{
			incr_capacity_rec * rec = (incr_capacity_rec *)data;
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			decr_capacity_rec crec;
			crec.m_capacity = rec->m_old_capacity;
			m_tm->log_compensate(this, &crec, sizeof(crec), opaque);
			// update metadata
			m_capacity = rec->m_old_capacity;
			m_head = UINT64_MAX;
		}
		return;
	default:
		assert(false);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::redo(uint64_t n, bool state_only, const void * data, uint32_t size, uint64_t redo_lsn) {
	switch (*((char *)data)) {
	case REMOVE:
		{
			remove_rec * rec = (remove_rec *)data;
//			printf("--- stm::redo: REMOVE node %llu, state_only %d, block %llu, redo_lsn %llu\n",
//						 n, (int)state_only, rec->m_block, redo_lsn);
			if (!state_only) {
				decoded_block_ptr pb = m_bm->read(n, node_decoder);
				node_t & node = (node_t &)*pb;
				remove(node, rec->m_block);
				// save node
				m_bm->write(n, pb, redo_lsn);
			}
			// update metadata
			if (redo_lsn > m_checkpoint_lsn) {
				m_used++;
			}
		}
		return;
	case INSERT:
		{
			insert_rec * rec = (insert_rec *)data;
//			printf("--- stm::redo: INSERT node %llu, state_only %d, block %llu, redo_lsn %llu\n",
//						 n, (int)state_only, rec->m_block, redo_lsn);
			if (!state_only) {
				decoded_block_ptr pb = m_bm->read(n, node_decoder);
				node_t & node = (node_t &)*pb;
				uint64_t updated_lsn = insert(node, n, rec->m_block, false);
				assert(updated_lsn > 0);
				(void)updated_lsn;
				// save node
				m_bm->write(n, pb, redo_lsn);
			}
			// update metadata
			if (redo_lsn > m_checkpoint_lsn) {
				m_used--;
			}
		}
		return;
	case NEW_NODE:
		if (!state_only) {
			new_node_rec * rec = (new_node_rec *)data;
//			printf("--- stm::redo: NEW_NODE node %llu, state_only %d, next %llu, redo_lsn %llu\n",
//						 n, (int)state_only, rec->m_next, redo_lsn);
			decoded_block_ptr pb(new node_t(rec->m_next));
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case CHANGE_HEAD:
		if (redo_lsn > m_checkpoint_lsn) {
			change_head_rec * rec = (change_head_rec *)data;
//			printf("--- stm::redo: CHANGE_HEAD new used %llu, new head %llu, redo_lsn %llu\n",
//						 rec->m_new_used, rec->m_new_head, redo_lsn);
			m_used = rec->m_new_used;
			m_head = rec->m_new_head;
		}
		return;
	case INCR_CAPACITY:
		{
			incr_capacity_rec * rec = (incr_capacity_rec *)data;
			if (!state_only) {
				// create new head
				node_t * head = new node_t;
				decoded_block_ptr pb(head);
				uint64_t start = n + 1;
				uint64_t len = rec->m_new_capacity - start;
				if (len > 0) {
					head->m_map[start] = len;
				}
				// save head
				m_bm->write(n, pb, redo_lsn);
			}
			if (redo_lsn > m_checkpoint_lsn) {
				m_head = n;
				m_capacity = rec->m_new_capacity;
			}
		}
		return;
	case DECR_CAPACITY:
		if (redo_lsn > m_checkpoint_lsn) {
			decr_capacity_rec * rec = (decr_capacity_rec *)data;
			m_head = UINT64_MAX;
			m_capacity = rec->m_capacity;
		}
		return;
	default:
		assert(false);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::checkpoint() {
	serial_buffer buf;
	buf.put(m_used);
	buf.put(m_head);
	buf.put(m_capacity);
	m_tm->log_rm_state(this, buf.data(), buf.size());
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void stm::recover_state(const void * data, uint32_t size, uint64_t redo_lsn) {
	serial_buffer buf(data, size, true);
	buf.get(m_used);
	buf.get(m_head);
	buf.get(m_capacity);
	m_checkpoint_lsn = redo_lsn;
//	printf("--- stm::recover_state: checkpoint_lsn %llu\n", m_checkpoint_lsn);
}

