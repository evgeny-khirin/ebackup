///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : tm.cpp
/// Author  : Evgeny Khirin <>
/// Description : Transaction manager. It allows single top level
/// transaction.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <assert.h>
#include <stdio.h>

#include "tm.hpp"
#include "bm.hpp"

//--------------------------------------------------------------------
// Signatures
//--------------------------------------------------------------------
#define BEGIN_TXN					1
#define END_TXN						2
#define UPDATE						3
#define CLEAR							4
#define DIRTY_BLOCK				5
#define RM_STATE					6
#define BEGIN_CHECKPOINT	7
#define END_CHECKPOINT		8

namespace {
	//--------------------------------------------------------------------
	// transaction log entry
	//--------------------------------------------------------------------
	struct log_entry {
		uint8_t	m_sign;				// Record signature, used to distinguish
													// records in log.

		void serialize(serial_buffer & buf) {
			buf.put(m_sign);
		}

		void deserialize(serial_buffer & buf) {
			buf.get(m_sign);
		}
	};

	//--------------------------------------------------------------------
	// transactions log entry
	// all transactions log entries have prev lsn field.
	//--------------------------------------------------------------------
	struct txn_entry: public log_entry {
		uint64_t	m_prev_lsn;		// Valid in nested transactions only and is 0
														// in top level transactions, which always
														// returns eof in read_backward.
														// Contains LSN of previous log entry in
														// parent transaction. Use logger:read_backward
														// to access previous entry.

		txn_entry() {
			m_prev_lsn = 0;
		}

		void serialize(serial_buffer & buf) {
			log_entry::serialize(buf);
			buf.put(m_prev_lsn);
		}

		void deserialize(serial_buffer & buf) {
			log_entry::deserialize(buf);
			buf.get(m_prev_lsn);
		}
	};

	//--------------------------------------------------------------------
	// begin transaction log entry
	//--------------------------------------------------------------------
	struct begin_txn_entry: public txn_entry {
		uint32_t		m_id;		// transaction id

		begin_txn_entry() {m_sign = BEGIN_TXN; m_id = 0;}

		void serialize(serial_buffer & buf) {
			txn_entry::serialize(buf);
			buf.put(m_id);
		}

		void deserialize(serial_buffer & buf) {
			txn_entry::deserialize(buf);
			buf.get(m_id);
		}
	};

	//--------------------------------------------------------------------
	// end transaction log entry
	//--------------------------------------------------------------------
	struct end_txn_entry: public txn_entry {
		uint32_t		m_id;		// transaction id

		end_txn_entry() {m_sign = END_TXN; m_id = UINT32_MAX;};

		end_txn_entry(uint32_t id) {m_sign = END_TXN; m_id = id;}

		void serialize(serial_buffer & buf) {
			txn_entry::serialize(buf);
			buf.put(m_id);
		}

		void deserialize(serial_buffer & buf) {
			txn_entry::deserialize(buf);
			buf.get(m_id);
		}
	};

	//--------------------------------------------------------------------
	// log entry with resource_mgr data
	//--------------------------------------------------------------------
	struct rm_entry: public txn_entry {
		uint64_t					m_block;			// block number
		resource_mgr *		m_rm;					// pointer to resource manager
		const void *			m_data;				// udate data
		uint32_t					m_size;				// size of update data.

		void serialize(serial_buffer & buf) {
			//store parent
			txn_entry::serialize(buf);
			// store block number
			buf.put(m_block);
			// store resource manager
			const std::string & name = m_rm->get_name();
			uint32_t name_len = name.length();
			buf.put(name_len);
			buf.put(name.data(), name_len);
			// store resource manager specific information
			buf.put(m_size);
			buf.put(m_data, m_size);
		}

		void deserialize(serial_buffer & buf) {
			// restore parent
			txn_entry::deserialize(buf);
			// restore block number
			buf.get(m_block);
			// restore resource manager
			uint32_t name_len;
			buf.get(name_len);
			const char * cname = (const char *)buf.get_ptr(name_len);
			std::string name(cname, name_len);
			m_rm = (resource_mgr *)whereis(name);
			// restore resource manager specific information
			buf.get(m_size);
			m_data = buf.get_ptr(m_size);
		}
	};

	//--------------------------------------------------------------------
	// update log entry
	//--------------------------------------------------------------------
	struct update_entry: public rm_entry {
		// Fields below are not serializable
		// and used internally by transaction
		// manager in rollback process.
		uint64_t				m_last_lsn;			// updated on each log_compensate

		update_entry() {m_sign = UPDATE; m_last_lsn = 0;}
	};

	//--------------------------------------------------------------------
	// clear log entry
	//--------------------------------------------------------------------
	struct clear_entry: public rm_entry {
		clear_entry() {m_sign = CLEAR;}
	};

	//--------------------------------------------------------------------
	// dirty block log entry
	//--------------------------------------------------------------------
	struct dirty_block_entry: public log_entry {
		// data fields are valid after deserialization only
		uint64_t	m_block;				// block number
		uint64_t	m_lsn;					// block LSN
		uint64_t	m_redo_lsn;			// redo block LSN
		block_ptr m_pb;						// block ptr

		dirty_block_entry() {m_sign = DIRTY_BLOCK;}

		void serialize(serial_buffer & buf, uint64_t n, block_ptr & pb, uint32_t block_size,
									 uint64_t lsn, uint64_t redo_lsn) {
			//store parent
			log_entry::serialize(buf);
			buf.put(n);
			buf.put(lsn);
			buf.put(redo_lsn);
			buf.put(block_size);
			buf.put(pb.prepare_read(), block_size);
		}

		void deserialize(serial_buffer & buf, buffer_mgr * bm) {
			// restore parent
			log_entry::deserialize(buf);
			buf.get(m_block);
			buf.get(m_lsn);
			buf.get(m_redo_lsn);
			uint32_t block_size;
			buf.get(block_size);
			const void * ptr = buf.get_ptr(block_size);
			m_pb = bm->alloc();
			char * b = m_pb.prepare_write();
			memcpy(b, ptr, block_size);
		}

		void deserialize_part(serial_buffer & buf) {
			// restore parent
			log_entry::deserialize(buf);
			// get partial data
			buf.get(m_block);
			buf.get(m_lsn);
		}
	};

	//--------------------------------------------------------------------
	// resource_mgr state entry
	//--------------------------------------------------------------------
	struct rm_state_entry: public log_entry {
		resource_mgr *		m_rm;					// pointer to resource manager
		const void *			m_data;				// udate data
		uint32_t					m_size;				// size of update data.

		rm_state_entry() {m_sign = RM_STATE;}

		void serialize(serial_buffer & buf) {
			//store parent
			log_entry::serialize(buf);
			// store resource manager
			const std::string & name = m_rm->get_name();
			uint32_t name_len = name.length();
			buf.put(name_len);
			buf.put(name.data(), name_len);
			// store resource manager specific information
			buf.put(m_size);
			buf.put(m_data, m_size);
		}

		void deserialize(serial_buffer & buf) {
			// restore parent
			log_entry::deserialize(buf);
			// restore resource manager
			uint32_t name_len;
			buf.get(name_len);
			const char * cname = (const char *)buf.get_ptr(name_len);
			std::string name(cname, name_len);
			m_rm = (resource_mgr *)whereis(name);
			// restore resource manager specific information
			buf.get(m_size);
			m_data = buf.get_ptr(m_size);
		}
	};

	//--------------------------------------------------------------------
	// begin checkpoint log entry
	//--------------------------------------------------------------------
	struct begin_checkpoint_entry: public log_entry {
		// FIELDS are valid after deserialization only
		uint64_t	m_low_watter_mark;

		begin_checkpoint_entry() {m_sign = BEGIN_CHECKPOINT;}

		void serialize(serial_buffer & buf, uint64_t low_watter_mark) {
			// store parent
			log_entry::serialize(buf);
			// store low watter mark
			buf.put(low_watter_mark);
		}

		void deserialize(serial_buffer & buf) {
			// restore parent
			log_entry::deserialize(buf);
			// store low watter mark
			buf.get(m_low_watter_mark);
		}
	};

	//--------------------------------------------------------------------
	// end checkpoint log entry
	//--------------------------------------------------------------------
	struct end_checkpoint_entry: public log_entry {
		end_checkpoint_entry() {m_sign = END_CHECKPOINT;}
	};
} // namespace

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::stop() {
	m_mutex.lock(5000);
	try {
		// rollback non completed transactions
		rollback_loosers();
		// start checkpoint
		if (m_recovered) {
			m_bm->sync();
			checkpoint();
			m_recovered = false;
		}
		m_rm_list.clear();
		m_logger->stop();
	} catch (std::exception &) {
		m_mutex.unlock_completely();
		throw;
	}
	m_mutex.unlock_completely();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::del_underlaying() {
	m_mutex.lock();
	try {
		m_rm_list.clear();
		m_logger->del_underlaying();
		delete m_logger;
		m_logger = NULL;
	} catch (std::exception &) {
		m_mutex.unlock_completely();
		throw;
	}
	m_mutex.unlock_completely();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::stats(no_stats_list & list) {
	m_logger->stats(list);
}

//--------------------------------------------------------------------
// public function
// Supported options:
//   {logger, Name} - attached logger.
//   {buffer_manager, Name} - attached buffer manager.
//   {cp_log_size, Bytes} - size of log when checkpoint is stored.
//--------------------------------------------------------------------
void trans_mgr::start(const opt_map & options) {
	named_object::start(options);

	opt_map::const_iterator it;
	if ((it = options.find("logger")) == options.end()) {
		throw missed_param();
	}
	m_logger = (logger *)whereis(it->second);
	if (m_logger == NULL) {
		throw not_registered();
	}

	if ((it = options.find("buffer_manager")) == options.end()) {
		throw missed_param();
	}
	m_bm_name = it->second;

	if ((it = options.find("cp_log_size")) != options.end()) {
#ifdef _WIN32
		m_cp_log_size = _strtoui64(it->second.c_str(), NULL, 10);
#else
		m_cp_log_size = strtoull(it->second.c_str(), NULL, 10);
#endif
		if (m_cp_log_size == 0) {
			throw inv_param();
		}
	} else {
		throw missed_param();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::start(const std::string & name, const std::string & logger,
											const std::string & bm, uint64_t cp_log_size) {
	printf("trans_mgr: name %s, logger %s, buffer manager %s, log size before checkpoint %f MB\n",
				 name.c_str(), logger.c_str(), bm.c_str(), cp_log_size / 1024.0 / 1024.0);
	opt_map	opts;
	opts["name"] = name;
	opts["logger"] = logger;
	opts["buffer_manager"] = bm;
	opts["cp_log_size"] = to_string(cp_log_size);
	trans_mgr * ptrans_mgr = new trans_mgr;
	ptrans_mgr->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t trans_mgr::begin_transaction(trans_type_t type) {
	m_mutex.lock();
	if (!m_recovered) {
		m_mutex.unlock();
		throw not_recovered();
	}
	txn_t	txn(type);
	begin_txn_entry rec;
	if (!m_txn_stack.empty()) {
		txn_t & parent = m_txn_stack.front();
		assert(parent.m_type != read_only || type == read_only);
		rec.m_prev_lsn = parent.m_last_lsn;
		rec.m_id = txn.m_id = parent.m_id + 1;
	}
	if (type != read_only) {
		serial_buffer buf;
		rec.serialize(buf);
		uint64_t redo_lsn;
		try {
			txn.m_last_lsn = write_log(buf.data(), buf.size(), redo_lsn);
		} catch (std::exception &) {
			// unlock mutex and rethrow
			m_mutex.unlock();
			throw;
		}
		if (m_txn_stack.empty()) {
			txn.m_start_lsn = redo_lsn;
		}
	}
	// push transaction on stack.
	m_txn_stack.push_front(txn);
	return txn.m_id;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::commit(uint32_t id) {
	txn_t txn;
	do {
		txn = m_txn_stack.front();
		if (txn.m_type == read_only) {
			// pop last transaction from stack
			m_txn_stack.pop_front();
		} else {
			// write end transaction record.
			end_txn_entry rec(txn.m_id);
			rec.m_prev_lsn = txn.m_last_lsn;
			serial_buffer buf;
			rec.serialize(buf);
			uint64_t redo_lsn;
			txn.m_last_lsn = write_log(buf.data(), buf.size(), redo_lsn);
			// pop last transaction from stack
			m_txn_stack.pop_front();
			// update m_last_lsn field of parent transaction.
			if (!m_txn_stack.empty()) {
				txn_t & parent = m_txn_stack.front();
				parent.m_last_lsn = txn.m_last_lsn;
			}
		}
		if (txn.m_id != id) {
			m_mutex.unlock();
		}
	} while (txn.m_id != id);
	// post commit actions
	if (m_txn_stack.empty()) {
		// sync hard top-level transaction
		if (txn.m_type == hard) {
			m_logger->sync(txn.m_last_lsn);
		}
	}
	m_mutex.unlock();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::rollback(uint32_t id) {
	txn_t txn;
	do {
		txn = m_txn_stack.front();
		if (txn.m_type == read_only) {
			// pop last transaction from stack
			m_txn_stack.pop_front();
		} else {
			// rollback it
			rollback(txn);
			// pop last transaction from stack
			m_txn_stack.pop_front();
			// update m_last_lsn field of parent transaction.
			if (!m_txn_stack.empty()) {
				txn_t & parent = m_txn_stack.front();
				parent.m_last_lsn = txn.m_last_lsn;
			}
		}
		m_mutex.unlock();
	} while (txn.m_id != id);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void trans_mgr::rollback(txn_t & txn) {
	serial_buffer buf;
	uint64_t lsn = txn.m_last_lsn;
	// undo log entries
	while (m_logger->read_backward(buf, lsn) == ok) {
		switch (buf.data()[0]) {
		case BEGIN_TXN:
			// write end transaction record and exit
			{
				end_txn_entry end;
				{
					begin_txn_entry begin;
					begin.deserialize(buf);
					end.m_id = begin.m_id;
					end.m_prev_lsn = begin.m_prev_lsn;
				}
				buf.rewind();
				end.serialize(buf);
				uint64_t redo_lsn;
				txn.m_last_lsn = write_log(buf.data(), buf.size(), redo_lsn);
				if (end.m_id == txn.m_id) {
					return;
				}
				lsn = end.m_prev_lsn;
			}
			break;
		case END_TXN:
		case CLEAR:
			{
				txn_entry entry;
				entry.deserialize(buf);
				lsn = entry.m_prev_lsn;
			}
			break;
		case UPDATE:
			{
				update_entry entry;
				entry.deserialize(buf);
				lsn = entry.m_prev_lsn;
				entry.m_last_lsn = txn.m_last_lsn;
//				printf("------------ trans_mgr::rollback: block %llu\n", entry.m_block);
				entry.m_rm->undo(entry.m_block, entry.m_data, entry.m_size, &entry);
				txn.m_last_lsn = entry.m_last_lsn;
			}
			break;
		default:
			assert(false);
		}
	}
	assert(false);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
uint64_t trans_mgr::write_log(const void * pdata, uint32_t size, uint64_t & redo_lsn) {
	if (m_logger->log_size() >= m_cp_log_size) {
		checkpoint();
	}
	return m_logger->write(pdata, size, redo_lsn);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void trans_mgr::checkpoint() {
	if (m_checkpoint_disabled) {
		return;
	}
	const uint64_t head = m_logger->get_head();
	if (!m_txn_stack.empty() && m_txn_stack.back().m_start_lsn == head) {
		// can not move head because of active transactions
//		printf("------------ trans_mgr::checkpoint: can not move head because of active transactions\n");
		return;
	}
	assert(m_txn_stack.empty() || m_txn_stack.back().m_start_lsn > head);
	m_checkpoint_disabled = true;
//	printf("------------ trans_mgr::checkpoint: started\n");
	// notify buffer manager
	const uint64_t bm_low_watter_mark = m_bm->checkpoint(head + 1024 * 1024);
	if (bm_low_watter_mark == head) {
		// can not move head because of buffer manager
		m_checkpoint_disabled = false;
		return;
	}
	assert(bm_low_watter_mark > head);
	// get checkpoint LSN
	const uint64_t checkpoint_lsn = m_logger->get_tail();
	if (checkpoint_lsn == head) {
		// log is empty
		m_checkpoint_disabled = false;
		return;
	}
	assert(checkpoint_lsn > head);
	// calculate TM low_watter_mark
	uint64_t low_watter_mark;
	if (m_txn_stack.empty()) {
		low_watter_mark = checkpoint_lsn;
	} else {
		low_watter_mark = m_txn_stack.back().m_start_lsn;
	}
	assert(low_watter_mark > head);
	// write begin_checkpoint_entry
	begin_checkpoint_entry begin;
	serial_buffer buf;
	begin.serialize(buf, low_watter_mark);
	uint64_t redo_lsn;
	m_logger->write(buf.data(), buf.size(), redo_lsn);
	assert(redo_lsn == checkpoint_lsn);
	// save resource managers states
	for (rm_list::iterator i = m_rm_list.begin(); i != m_rm_list.end(); i++) {
		resource_mgr * rm = *i;
		rm->checkpoint();
	}
	// write end_checkpoint_entry
	end_checkpoint_entry end;
	buf.rewind();
	end.serialize(buf);
	m_logger->write(buf.data(), buf.size(), redo_lsn);
	// set new head
	uint64_t new_head;
	if (low_watter_mark < bm_low_watter_mark) {
		new_head = low_watter_mark;
	} else {
		new_head = bm_low_watter_mark;
	}
	m_logger->sync(m_logger->get_tail());
	assert(new_head > head);
	m_logger->set_head(new_head, checkpoint_lsn);
//	printf("------------ trans_mgr::checkpoint: set head %llu, checkpoint %llu, low_watter_mark %llu\n",
//				 head, checkpoint_lsn, low_watter_mark);
	m_checkpoint_disabled = false;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::register_rm(resource_mgr * rm) {
	m_rm_list.push_back(rm);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t trans_mgr::log_update(resource_mgr * rm, uint64_t n,
															 const void * data, uint32_t size) {
	assert(m_recovered && !m_txn_stack.empty());
	txn_t & txn = m_txn_stack.front();
	assert(txn.m_type != read_only);
	update_entry e;
	e.m_block = n;
	e.m_rm = rm;
	e.m_data = data;
	e.m_size = size;
	e.m_prev_lsn = txn.m_last_lsn;
	serial_buffer buf;
	e.serialize(buf);
	uint64_t redo_lsn;
	txn.m_last_lsn = write_log(buf.data(), buf.size(), redo_lsn);
	return redo_lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t trans_mgr::log_compensate(resource_mgr * rm, uint64_t n,
																	 const void * data, uint32_t size,
																	 void * opaque) {
	update_entry * up_entry = (update_entry *)opaque;
	clear_entry e;
	e.m_block = n;
	e.m_rm = rm;
	e.m_data = data;
	e.m_size = size;
	e.m_prev_lsn = up_entry->m_prev_lsn;
	serial_buffer buf;
	e.serialize(buf);
	uint64_t redo_lsn;
	up_entry->m_last_lsn = write_log(buf.data(), buf.size(), redo_lsn);
	return redo_lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::log_rm_state(resource_mgr * rm, const void * data, uint32_t size) {
	rm_state_entry e;
	e.m_rm = rm;
	e.m_data = data;
	e.m_size = size;
	serial_buffer buf;
	e.serialize(buf);
	uint64_t redo_lsn;
	m_logger->write(buf.data(), buf.size(), redo_lsn);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::log_dirty(uint64_t n, block_ptr & pb, uint32_t block_size,
													uint64_t lsn, uint64_t block_redo_lsn) {
	dirty_block_entry e;
	serial_buffer buf;
	e.serialize(buf, n, pb, block_size, lsn, block_redo_lsn);
	uint64_t redo_lsn;
	uint64_t undo_lsn = m_logger->write(buf.data(), buf.size(), redo_lsn);
	m_logger->sync(undo_lsn);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void trans_mgr::recover() {
//	printf("------------ trans_mgr::recover: started: head %llu, tail %llu, log size %f MB\n",
//				 m_logger->get_head(), m_logger->get_tail(), m_logger->log_size() / 1024.0 / 1024.0);
	assert(!m_recovered);
	// Resolve buffer manager
	m_bm = (buffer_mgr *)whereis(m_bm_name);
	if (m_bm == NULL) {
		throw not_registered();
	}
	dirty_map_t dirty_map;
	build_dirty_map(dirty_map);
	uint64_t low_watter_mark = recover_checkpoint();
	redo_log(low_watter_mark, dirty_map);
	rollback_loosers();
	m_recovered = true;
	// notify resource managers
	for (rm_list::iterator i = m_rm_list.begin(); i != m_rm_list.end(); i++) {
		resource_mgr * rm = *i;
		rm->recover_finished();
	}
//	printf("------------ trans_mgr::recover: finished\n");
}

//--------------------------------------------------------------------
// internal function
// Since there are no parallel transactions, than function must only check
// that last top level transaction is terminated.
//--------------------------------------------------------------------
void trans_mgr::rollback_loosers() {
	// rollback non completed transactions
	while (!m_txn_stack.empty()) {
		txn_t txn = m_txn_stack.front();
		if (txn.m_type == read_only) {
			// pop last transaction from stack
			m_txn_stack.pop_front();
		} else {
			rollback(txn);
			// pop last transaction from stack
			m_txn_stack.pop_front();
			if (!m_txn_stack.empty()) {
				txn_t & parent = m_txn_stack.front();
				parent.m_last_lsn = txn.m_last_lsn;
			}
		}
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
uint64_t trans_mgr::recover_checkpoint() {
	uint64_t redo_lsn = m_logger->get_checkpoint();
	if (redo_lsn == 0) {
		// no checkpoint saved
		return 0;
	}
	uint64_t low_watter_mark = 0;
	uint64_t undo_lsn;
	serial_buffer buf;
	while (true) {
		result res = m_logger->read_forward(buf, redo_lsn, undo_lsn);
		assert(res == ok);
		(void)res;
		switch (buf.data()[0]) {
		case BEGIN_CHECKPOINT:
			{
				begin_checkpoint_entry e;
				e.deserialize(buf);
				low_watter_mark = e.m_low_watter_mark;
			}
			break;
		case RM_STATE:
			{
				rm_state_entry e;
				e.deserialize(buf);
				e.m_rm->recover_state(e.m_data, e.m_size, redo_lsn);
			}
			break;
		case END_CHECKPOINT:
			return low_watter_mark;
		}
		redo_lsn = undo_lsn;
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void trans_mgr::redo_log(uint64_t low_watter_mark, const dirty_map_t & dirty_map) {
	// disable checkpoints
	m_checkpoint_disabled = true;
	uint64_t redo_lsn = m_logger->get_head();
	uint64_t undo_lsn;
	serial_buffer buf;
	while (m_logger->read_forward(buf, redo_lsn, undo_lsn) == ok) {
		if (redo_lsn >= low_watter_mark) {
			// enable checkpoints
			m_checkpoint_disabled = false;
		}
		switch (buf.data()[0]) {
		case BEGIN_TXN:
			if (redo_lsn >= low_watter_mark) {
				// Push transaction on stack. If transaction is not completed than
				// it will be rolled back latter.
				txn_t txn(soft);
				if (m_txn_stack.empty()) {
					txn.m_start_lsn = redo_lsn;
				} else {
					txn_t & parent = m_txn_stack.front();
					txn.m_id = parent.m_id + 1;
				}
				begin_txn_entry entry;
				entry.deserialize(buf);
				assert(entry.m_id == txn.m_id);
				txn.m_last_lsn = undo_lsn;
				m_txn_stack.push_front(txn);
			}
			break;
		case END_TXN:
			if (redo_lsn >= low_watter_mark) {
				// Pop transaction from stack.
				end_txn_entry entry;
				entry.deserialize(buf);
				txn_t & txn = m_txn_stack.front();
				if (entry.m_id == txn.m_id) {
					m_txn_stack.pop_front();
					if (!m_txn_stack.empty()) {
						txn_t & parent = m_txn_stack.front();
						parent.m_last_lsn = undo_lsn;
					}
				} else {
					// make sure that transaction is nested
					assert(entry.m_id > txn.m_id);
					// update last lsn of current transaction
					txn.m_last_lsn = undo_lsn;
				}
			}
			break;
		case UPDATE:
			{
				update_entry entry;
				entry.deserialize(buf);
				uint64_t block_lsn;
				if (entry.m_block == UINT64_MAX) {
					block_lsn = 0;
				} else {
					block_lsn = m_bm->read_lsn(entry.m_block);
					if (block_lsn == 0) {
						block_lsn = recover_block(entry.m_block, dirty_map);
					}
				}
				bool state_only = (block_lsn >= redo_lsn);
				if (state_only || redo_lsn > block_lsn) {
//					printf("------------ trans_mgr::redo_update: block %llu, state_only %d, lsn %llu, redo lsn %llu\n",
//								 entry.m_block, (int)state_only, block_lsn, redo_lsn);
					entry.m_rm->redo(entry.m_block, state_only, entry.m_data, entry.m_size, redo_lsn);
				}
				if (redo_lsn > low_watter_mark) {
					txn_t & txn = m_txn_stack.front();
					txn.m_last_lsn = undo_lsn;
				}
			}
			break;
		case CLEAR:
			{
				clear_entry entry;
				entry.deserialize(buf);
				uint64_t block_lsn;
				if (entry.m_block == UINT64_MAX) {
					block_lsn = 0;
				} else {
					block_lsn = m_bm->read_lsn(entry.m_block);
					if (block_lsn == 0) {
						block_lsn = recover_block(entry.m_block, dirty_map);
					}
				}
				bool state_only = (block_lsn >= redo_lsn);
				if (state_only || redo_lsn > block_lsn) {
//					printf("------------ trans_mgr::redo_clear: block %llu, state_only %d, lsn %llu, redo lsn %llu\n",
//								 entry.m_block, (int)state_only, block_lsn, redo_lsn);
					entry.m_rm->redo(entry.m_block, state_only, entry.m_data, entry.m_size, redo_lsn);
				}
				if (redo_lsn > low_watter_mark) {
					txn_t & txn = m_txn_stack.front();
					txn.m_last_lsn = undo_lsn;
				}
			}
			break;
		case DIRTY_BLOCK:
			{
				dirty_block_entry entry;
				entry.deserialize(buf, m_bm);
				uint64_t block_lsn = m_bm->read_lsn(entry.m_block);
				if (entry.m_lsn > block_lsn) {
//					printf("------------ trans_mgr::recover_dirty: block %llu, block lsn %llu, new block lsn %llu, redo lsn %llu\n",
//								 entry.m_block, block_lsn, entry.m_lsn, entry.m_redo_lsn);
					m_bm->recover_dirty(entry.m_block, entry.m_pb, entry.m_lsn, entry.m_redo_lsn);
				}
			}
			break;
		}
		redo_lsn = undo_lsn;
	}
	m_checkpoint_disabled = false;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
uint64_t trans_mgr::recover_block(uint64_t n, const dirty_map_t & dirty_map) {
	dirty_map_t::const_iterator i = dirty_map.find(n);
	if (i == dirty_map.end()) {
		return 0;
	}
	serial_buffer buf;
	uint64_t load_lsn = i->second.m_load_lsn;
	result res = m_logger->read_forward(buf, load_lsn, load_lsn);
	assert(res == ok);
	(void)res;
	assert(buf.data()[0] == DIRTY_BLOCK);
	dirty_block_entry entry;
	entry.deserialize(buf, m_bm);
	assert(entry.m_block == n);
//	printf("------------ trans_mgr::recover_block: block %llu, lsn %llu, redo lsn %llu\n",
//				 entry.m_block, entry.m_lsn, entry.m_redo_lsn);
	m_bm->recover_dirty(entry.m_block, entry.m_pb, entry.m_lsn, entry.m_redo_lsn);
	return entry.m_lsn;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void trans_mgr::build_dirty_map(dirty_map_t & dirty_map) {
	uint64_t redo_lsn = m_logger->get_head();
	uint64_t undo_lsn;
	serial_buffer buf;
	while (m_logger->read_forward(buf, redo_lsn, undo_lsn) == ok) {
		if (buf.data()[0] == DIRTY_BLOCK) {
			dirty_block_entry entry;
			entry.deserialize_part(buf);
			dirty_descr_t descr;
			descr.m_lsn = entry.m_lsn;
			descr.m_load_lsn = redo_lsn;
			dirty_map[entry.m_block] = descr;
		}
		redo_lsn = undo_lsn;
	}
}


