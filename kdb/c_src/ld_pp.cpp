///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : ld_pp.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate log device with ping-pong write algorithm.
/// Prevents log corruption when tail block is overwritten multiple times
/// by logger.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>

#include "ld_pp.hpp"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {log_device, Name} - name of underlaying log device.
//--------------------------------------------------------------------
void ld_pp::start(const opt_map & options) {
	intermediate_log_device::start(options);
	opt_map::const_iterator it;
	if ((it = options.find("log_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (log_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	m_block_size = m_pdevice->block_size() - 12;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_pp::start(const std::string & name, const std::string & log_device) {
	printf("ld_pp: name %s, log_device %s\n", name.c_str(), log_device.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["log_device"] = log_device;
	ld_pp * pdevice = new ld_pp;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t ld_pp::block_size() {
	return m_block_size;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
namespace {
	DEF_EXCEPTION(ld_pp_block_never_written)	// internal exception
}

block_ptr ld_pp::read_block(uint64_t n, uint32_t & generation) {
	// try to read first copy of block
	try {
		block_ptr pb0 = m_pdevice->read(n);
		const char * b0 = pb0.prepare_read();
		// read of first block is succeeded. Check that block numbers are match
		uint64_t block_n = *((uint64_t *)(b0 + m_block_size));
		if (block_n != n) {
			// Block does not match its number - previos (overwritten) block is here.
			throw ld_pp_block_never_written();
		}
		// block numbers are same, take generation
		uint32_t gen0 = *((uint32_t *)(b0 + m_block_size + 8));
		// try read second copy
		block_ptr pb1;
		const char * b1;
		try {
			pb1 = m_pdevice->read(n + 1);
		} catch (std::exception &) {
			// failed to read second copy, return first
			generation = gen0;
//			printf("ld_pp::read_block: block %llu, store %llu, generation %u\n",
//						 n, n, generation);
			return pb0;
		}
		b1 = pb1.prepare_read();
		// read of second copy succeeded. Check that block numbers are match
		block_n = *((uint64_t *)(b1 + m_block_size));
		if (block_n != n) {
			// Block numbers do not match, return first.
			generation = gen0;
//			printf("ld_pp::read_block: block %llu, store %llu, generation %u\n",
//						 n, n, generation);
			return pb0;
		}
		// block numbers are same, take generation
		uint32_t gen1 = *((uint32_t *)(b1 + m_block_size + 8));
		// return block with last generation
		if (gen0 > gen1) {
			generation = gen0;
//			printf("ld_pp::read_block: block %llu, store %llu, generation %u\n",
//						 n, n, generation);
			return pb0;
		} else {
			generation = gen1;
//			printf("ld_pp::read_block: block %llu, store %llu, generation %u\n",
//						 n, n + 1, generation);
			return pb1;
		}
	} catch (ld_pp_block_never_written &) {
		// transalate exception
		throw block_never_written();
	} catch (std::exception &) {
		// failed to read first copy, check second
		block_ptr pb1 = m_pdevice->read(n + 1);
		const char * b1 = pb1.prepare_read();
		// check that block numbers are match
		uint64_t block_n = *((uint64_t *)(b1 + m_block_size));
		if (block_n != n) {
			// Block does not match its number - previos (overwritten) block is here.
			// rethrow original exception
			throw;
		}
		// block numbers are same, take generation
		generation = *((uint32_t *)(b1 + m_block_size + 8));
		// return second copy of block
//		printf("ld_pp::read_block: block %llu, store %llu, generation %u\n",
//					 n, n + 1, generation);
		return pb1;
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr ld_pp::read(uint64_t n) {
	uint32_t generation;
	block_ptr pb = read_block(n, generation);
	return pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_pp::write(uint64_t n, block_ptr & pb) {
	if (n == m_tail_block) {
		// Attempt to overwrite current tail block. Write it to different location. Not
		// necessary to sync, because previos generation synced by logger.
		uint32_t new_generation = m_tail_gen + 1;
		uint64_t store_n = n + (new_generation & 1);
		// add metadata
		char * b = pb.prepare_write();
		*((uint64_t *)(b + m_block_size)) = n;
		*((uint32_t *)(b + m_block_size + 8)) = new_generation;
		// write block
		m_pdevice->write(store_n, pb);
		// update state
		m_tail_gen = new_generation;
//		printf("ld_pp::write: overwrite block %llu, store %llu, generation %u\n",
//					 n, store_n, m_tail_gen);
		return;
	}

	// Logger writes new tail. Check what to do with current tail.
	if ((m_tail_gen & 1) == 0) {
		// Generation of current tail block is even. It is not necessary to move
		// current tail block to its original location, because it is already in place.
		// But if tail generation is not zero, device must be synced before writing new
		// block, because last overwritting of tail block may be not synced by logger.
		if (m_tail_gen != 0) {
//			printf("ld_pp::write: sync tail block %llu, store %llu, generation %u started\n",
//						 m_tail_block, m_tail_block, m_tail_gen);
			m_pdevice->sync(m_tail_lsn);
//			printf("ld_pp::write: sync finished\n");
		}
		// add metadata
		char * b = pb.prepare_write();
		*((uint64_t *)(b + m_block_size)) = n;
		*((uint32_t *)(b + m_block_size + 8)) = 0;
		// write block
		m_pdevice->write(n, pb);
		// update state
		m_tail_block = n;
		m_tail_gen = 0;
//		printf("ld_pp::write: new block %llu, store %llu, generation %u\n",
//					 m_tail_block, m_tail_block, m_tail_gen);
		return;
	}

	// Current tail block is not in place and must be moved to its orginal location
	// before new tail is written. Device must be synced before moving, because last
	// overwritting of current tail block may be not synced by logger. Device must be
	// synced after moving too, in order to be sure that block is moved correctly.

	// read current tail before sync, because sync may clean cache of underlaying device,
	// for example, bd_dwrite.
	block_ptr curr_tail = m_pdevice->read(m_tail_block + 1);
	// sync device
//	printf("ld_pp::write: sync tail before move block %llu, store %llu, generation %u started\n",
//				 m_tail_block, m_tail_block + 1, m_tail_gen);
	m_pdevice->sync(m_tail_lsn);
//	printf("ld_pp::write: sync finished\n");
	// add metadata to current tail block
	char * b = curr_tail.prepare_write();
	*((uint64_t *)(b + m_block_size)) = m_tail_block;
	*((uint32_t *)(b + m_block_size + 8)) = m_tail_gen + 1;
	// move current tail to its orginal location
	m_pdevice->write(m_tail_block, curr_tail);
//	printf("ld_pp::write: move tail block %llu, store %llu, generation %u\n",
//				 m_tail_block, m_tail_block, m_tail_gen + 1);
	// sync device again
//	printf("ld_pp::write: sync tail after move block %llu, store %llu, generation %u started\n",
//				 m_tail_block, m_tail_block, m_tail_gen + 1);
	m_pdevice->sync(m_tail_lsn);
//	printf("ld_pp::write: sync finished\n");
	// write new tail
	// add metadata
	b = pb.prepare_write();
	*((uint64_t *)(b + m_block_size)) = n;
	*((uint32_t *)(b + m_block_size + 8)) = 0;
	// write block
	m_pdevice->write(n, pb);
	// update state
	m_tail_block = n;
	m_tail_gen = 0;
//	printf("ld_pp::write: new block %llu, store %llu, generation %u\n",
//				 m_tail_block, m_tail_block, m_tail_gen);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_pp::set_tail(uint64_t lsn, uint64_t tail_block) {
	uint32_t generation;
	block_ptr tail_pb;
	bool read_failed = false;
	// Read tail
	try {
//		printf("ld_pp::set_tail: started read block %llu\n", tail_block);
		tail_pb = read_block(tail_block, generation);
//		printf("ld_pp::set_tail: finished read block %llu, generation %u\n",
//					 tail_block, generation);
	} catch (std::exception &) {
		read_failed = true;
	}
	if (read_failed) {
		// Cannot read tail, set state such that no moving will required.
		m_tail_block = UINT64_MAX;
		m_tail_gen = 0;
//		printf("ld_pp::set_tail: failed read block %llu\n", tail_block);
	} else if ((generation & 1) == 0) {
		// Generation is even - tail is in place.
		m_tail_block = tail_block;
		m_tail_gen = generation;
//		printf("ld_pp::set_tail: tail block %llu in place\n", tail_block);
	} else {
		// Tail is not in place.
		// Move tail to original block with corresponding generation,
		// because after m_pdevice->set_tail below, underlaying device
		// may prevent to read last generation of last block.

		// add metadata to current tail block
		generation++;
		char * b = tail_pb.prepare_write();
		*((uint64_t *)(b + m_block_size)) = tail_block;
		*((uint32_t *)(b + m_block_size + 8)) = generation;
		m_pdevice->write(tail_block, tail_pb);
//		printf("ld_pp::set_tail: move tail block %llu, store %llu, generation %u\n",
//					 tail_block, tail_block, generation);
		// sync device
//		printf("ld_pp::set_tail: sync tail after move block %llu, store %llu, generation %u started\n",
//					 tail_block, tail_block, generation);
		m_pdevice->sync(lsn);
//		printf("ld_pp::write: sync finished\n");
		// Set the state
		m_tail_block = tail_block;
		m_tail_gen = generation;
	}
	m_pdevice->set_tail(lsn, tail_block);
	m_tail_lsn = lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_pp::sync(uint64_t tail_lsn) {
//	printf("ld_pp::sync: start tail block %llu, store %llu, generation %u\n",
//				 m_tail_block, m_tail_block + (m_tail_gen & 1), m_tail_gen);
	m_pdevice->sync(tail_lsn);
	m_tail_lsn = tail_lsn;
//	printf("ld_pp::sync: finished\n");
}

