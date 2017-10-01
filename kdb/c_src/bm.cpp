///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bm.cpp
/// Author  : Evgeny Khirin <>
/// Description : Buffer manager interface.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <assert.h>
#include <stdio.h>

#include "bm.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void encoded_block::serialize(serial_buffer & buf) {
	throw not_supported();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void buffer_mgr_stats::soft_reset() {
	bd_stats::soft_reset();
	m_cache_stats->soft_reset();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
std::string buffer_mgr_stats::to_string() {
	std::stringstream ss;

	ss << bd_stats::to_string() << ". Cache stats: lookups " <<
		m_cache_stats->m_lookups << ", hits " << m_cache_stats->m_hits << " (" <<
		(double)m_cache_stats->m_hits / m_cache_stats->m_lookups * 100 <<
		"%) capacity " << m_cache_stats->m_capacity << ", dirty " <<
		m_cache_stats->m_dirty << " (" <<
		(double)m_cache_stats->m_dirty / m_cache_stats->m_capacity * 100 <<
		"%) inserts " << m_cache_stats->m_inserts << ", updates " <<
		m_cache_stats->m_updates << " (" <<
		(double)m_cache_stats->m_updates / m_cache_stats->m_inserts * 100 <<
		"%).";

	return ss.str();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void buffer_mgr::stats(no_stats_list & list) {
	list.push_back(&m_stats);
	m_bd->stats(list);
}

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - underlying block device.
//   {logger, Name} - logger
//   {transaction_manager, Name} - transaction manager.
//   {cache_capacity, CacheCapacity} - cache capacity.
//--------------------------------------------------------------------
void buffer_mgr::start(const opt_map & options) {
	named_object::start(options);

	m_stats.m_reg_name = get_name();
	opt_map::const_iterator it;

	if ((it = options.find("block_device")) == options.end()) {
		throw missed_param();
	}
	m_bd = (block_device *)whereis(it->second);
	if (m_bd == NULL) {
		throw not_registered();
	}
	m_block_size = m_bd->block_size() - 8; // reserve space for block LSN

	if ((it = options.find("transaction_manager")) == options.end()) {
		throw missed_param();
	}
	m_tm = (trans_mgr *)whereis(it->second);
	if (m_tm == NULL) {
		throw not_registered();
	}

	if ((it = options.find("cache_capacity")) == options.end()) {
		throw missed_param();
	};
	uint32_t cache_capacity = strtoul(it->second.c_str(), NULL, 10);

	m_cache.start(cache_capacity, save_dirty, this);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void buffer_mgr::start(const std::string & name, const std::string & block_device,
											 const std::string & trans_mgr, uint32_t cache_capacity) {
	printf("buffer_mgr: name %s, block_device %s, trans_mgr %s, cache_capacity %u\n",
				 name.c_str(), block_device.c_str(), trans_mgr.c_str(), cache_capacity);
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	opts["transaction_manager"] = trans_mgr;
	opts["cache_capacity"] = to_string(cache_capacity);
	buffer_mgr * bm = new buffer_mgr;
	bm->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void buffer_mgr::stop() {
	// buffer manager stops underlaying block device only.
	// Logger must be stopped by transaction manager and
	// transaction manager must be stopped by DB manager.
	m_bd->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void buffer_mgr::del_underlaying() {
	// buffer manager deletes underlaying block device only.
	// Logger must be deleted by transaction manager and
	// transaction manager must be deleted by DB manager.
	m_bd->del_underlaying();
	delete m_bd;
	m_bd = NULL;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void buffer_mgr::save_dirty(void * param, uint64_t & n, decoded_block_ptr & db) {
	buffer_mgr * bm = (buffer_mgr *) param;
	// encode block
	block_ptr pb;
	if (db->is_decoded()) {
		// block is decoded, encode it
		pb = bm->alloc();
		char * b = pb.prepare_write();
		serial_buffer buf(b, bm->m_block_size, false);
		db->serialize(buf);
		// store block lsn
		*((uint64_t *)(b + bm->m_block_size)) = db->m_lsn;
	} else {
		// block already encoded
		encoded_block & eb = (encoded_block &)*db;
		pb = eb.get();
	}
	// write block to log before writing it on device
	bm->m_tm->log_dirty(n, pb, bm->m_block_size + 8, db->m_lsn, db->m_redo_lsn);
	// Write block on device. No necessary to sync log because it is already
	// synced by tm::log_dirty.
	bm->m_bd->write(n, pb);
	// erase block from dirty map
	assert(bm->m_dirty_map.find(db->m_redo_lsn) != bm->m_dirty_map.end());
//	printf("------------------------ buffer_mgr::save_dirty: block %llu, lsn %llu, redo lsn %llu, dirty map block %llu\n",
//				 n, db->m_lsn, db->m_redo_lsn, bm->m_dirty_map.find(db->m_redo_lsn)->second);
	assert(bm->m_dirty_map.find(db->m_redo_lsn)->second == n);
	bm->m_dirty_map.erase(db->m_redo_lsn);
	db->m_dirty = false;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
decoded_block_ptr buffer_mgr::read(uint64_t n, decode_fun decoder) {
	m_stats.m_reads++;
	decoded_block_ptr db;
	switch (m_cache.lookup(n, db)) {
	case ok:
		{
			if (db->is_decoded()) {
				// block already decoded
//				printf("------------------------ buffer_mgr::read: block %llu, lsn %llu, redo lsn %llu\n",
//							 n, db->m_lsn, db->m_redo_lsn);
				return db;
			}
			// decode block
			encoded_block & eb = (encoded_block &)*db;
			block_ptr & pb = eb.get();
			const char * b = pb.prepare_read();
			serial_buffer buf(b, m_block_size, true);
			decoded_block_ptr new_db = decoder(n, buf);
			// copy block LSN
			new_db->m_lsn = eb.m_lsn;
			new_db->m_redo_lsn = eb.m_redo_lsn;
			new_db->m_dirty = eb.m_dirty;
			// insert block into cache
			m_cache.insert(n, new_db, new_db->m_dirty);
//			printf("------------------------ buffer_mgr::read: block %llu, lsn %llu, redo lsn %llu\n",
//						 n, new_db->m_lsn, new_db->m_redo_lsn);
			return new_db;
		}
	case not_found:
		{
			block_ptr pb = m_bd->read(n);
			// decode block
			const char * b = pb.prepare_read();
			serial_buffer buf(b, m_block_size, true);
			db = decoder(n, buf);
			// Decode block's LSN
			db->m_lsn = *((uint64_t *)(b + m_block_size));
			// insert block into cache
			m_cache.insert(n, db, false);
//			printf("------------------------ buffer_mgr::read: block %llu, lsn %llu, redo lsn %llu\n",
//						 n, db->m_lsn, db->m_redo_lsn);
			return db;
		}
	default:
		throw internal_error();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t buffer_mgr::read_lsn(uint64_t n) {
	m_stats.m_reads++;
	decoded_block_ptr db;
	switch (m_cache.lookup(n, db)) {
	case ok:
			return db->m_lsn;
	case not_found:
		{
			block_ptr pb;
			try {
				pb = m_bd->read(n);
			} catch (power_failure &) {
				throw;
			} catch (std::exception &) {
				// return min possible LSN, so redo will apply to block
				return 0;
			}
			// decode LSN
			encoded_block * eb = new encoded_block(pb);
			db = decoded_block_ptr(eb);
			const char * b = pb.prepare_read();
			eb->m_lsn = *((uint64_t *)(b + m_block_size));
			// insert block into cache
			m_cache.insert(n, db, false);
			return eb->m_lsn;
		}
	default:
		throw internal_error();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void buffer_mgr::write(uint64_t n, decoded_block_ptr & db, uint64_t redo_lsn) {
	m_stats.m_writes++;
	// update dirty map and block's redo LSN
	if (!db->m_dirty) {
		decoded_block_ptr old_db;
		// check if block already in cache and written. Block may be ovewritten with
		// newly allocated pointer. In that case redo LSN must remain old.
		if (m_cache.lookup(n, old_db) == ok && old_db->m_dirty) {
			db->m_redo_lsn = old_db->m_redo_lsn;
		} else {
			db->m_redo_lsn = redo_lsn;
			m_dirty_map[redo_lsn] = n;
		}
		db->m_dirty = true;
	}
	// set block's LSN past of redo LSN
	db->m_lsn = redo_lsn + 1;
//	printf("------------------------ buffer_mgr::write: block %llu, lsn %llu, redo lsn %llu\n",
//				 n, db->m_lsn, db->m_redo_lsn);
	// insert data into cache
	m_cache.insert(n, db, true);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t buffer_mgr::checkpoint(uint64_t log_head) {
	// Underlaying block device must be synced on checkpoint.
	// Consider following:
	// 1. Block X0 loaded from device into cache.
	// 2. Block X0 modified to X1 and stored in cache with recovery LSN1.
	//    LSN1 placed in dirty map.
	// 3. Block X1 flushed on disk by cache with recovery LSN2 (block
	//    logged before write and saved in OS cache or device buffers).
	//    LSN1 removed from dirty map.
	// 4. Now transaction manager initiates checkpoint. Since dirty table
	//    is empty, transaction manager sets head to LSN3 of begin checkpoint
	//    record, which is after LSN2.
	// 5. Now crash occured. X1 did not reached device and LSN2 is removed
	//    from log on step 4. So block X remains in state X0 (or corrupted)
	//    instead X1 and can not be recovered (redone) to state X1.
	dirty_map_t::iterator i = m_dirty_map.begin();
	if (i == m_dirty_map.end()) {
		// no dirty items. return max LSN.
		m_bd->sync();
		return UINT64_MAX;
	}
	uint64_t prev_first = i->first;
	while (i->first <= log_head && !m_cache.is_locked(i->second)) {
		// we prevent log head from moving forward - flush oldest dirty item
		// and try again.
		m_cache.save(i->second);
		i = m_dirty_map.begin();
		if (i == m_dirty_map.end()) {
			// no dirty items. return max LSN.
			m_bd->sync();
			return UINT64_MAX;
		}
		// make sure that save_dirty removed first entry, i.e. dirty map is correct.
		assert(prev_first != i->first);
		prev_first = i->first;
	}
	m_bd->sync();
	return i->first;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void buffer_mgr::recover_dirty(uint64_t n, block_ptr & pb, uint64_t block_lsn, uint64_t redo_lsn) {
	decoded_block_ptr old_db;
	if (m_cache.lookup(n, old_db) == ok && old_db->m_dirty) {
		// block already in cache
		m_dirty_map.erase(old_db->m_redo_lsn);
	}
	// create new block
	encoded_block * eb = new encoded_block(pb);
	decoded_block_ptr db(eb);
	// insert block in dirty map
	eb->m_lsn = block_lsn;
	eb->m_redo_lsn = redo_lsn;
	eb->m_dirty = true;
	m_dirty_map[redo_lsn] = n;
	// insert block into cache
	m_cache.insert(n, db, true);
}

