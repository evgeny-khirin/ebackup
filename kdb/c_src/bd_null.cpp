///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_null.hpp
/// Author  : Evgeny Khirin <>
/// Description : Null block device.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdint.h>
#include <stdio.h>
#include <assert.h>

#include "utils.hpp"
#include "bd_null.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_null::stats(no_stats_list & list) {
	list.push_back(&m_stats);
}

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_size, BlockSize} - size of block in bytes. Default is 4K.
//   {capacity, Capacity} - device capacity in blocks.
//      Default is (2^64 div block_size).
//--------------------------------------------------------------------
void bd_null::start(const opt_map & options) {
	block_device::start(options);
	m_stats.m_reg_name = get_name();
	opt_map::const_iterator it;
	if ((it = options.find("block_size")) != options.end()) {
		m_block_size = strtoul(it->second.c_str(), NULL, 10);
	} else {
		m_block_size = 4 * 1024;
	}
	if ((it = options.find("capacity")) != options.end()) {
#ifdef _WIN32
		m_capacity = _strtoui64(it->second.c_str(), NULL, 10);
#else
		m_capacity = strtoull(it->second.c_str(), NULL, 10);
#endif
	} else {
		m_capacity = UINT64_MAX / m_block_size;
	}
	m_pb0 = alloc();
	m_pb1 = alloc();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_null::start(const std::string & name, uint32_t block_size,
										uint64_t capacity) {
#ifdef _WIN32
	printf("bd_null: name %s, block_size %u, capacity %I64u, size %f GB\n",
				 name.c_str(), block_size, capacity,
				 (double)capacity * block_size / 1024 / 1024 / 1024);
#else
	printf("bd_null: name %s, block_size %u, capacity %llu, size %f GB\n",
				 name.c_str(), block_size, capacity,
				 (double)capacity * block_size / 1024 / 1024 / 1024);
#endif
	opt_map	opts;
	opts["name"] = name;
	opts["block_size"] = to_string(block_size);
	opts["capacity"] = to_string(capacity);
	bd_null * pdevice = new bd_null;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_null::stop() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_null::del_underlaying() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_null::write(uint64_t n, block_ptr & pb) {
	assert(n < m_capacity);
	m_stats.m_writes++;
	switch (n) {
	case 0:
		m_pb0 = pb;
		break;
	case 1:
		m_pb1 = pb;
		break;
	}
}

//--------------------------------------------------------------------
// operator < for sorting in bd_file::write(block_list& blocks).
// comparison of block pointers missed a sence.
//--------------------------------------------------------------------
inline bool operator<(const block_ptr &, const block_ptr &) {
	return false;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_null::write(block_list & blocks) {
	blocks.sort();
	serial_buffer buf;
	uint64_t first = UINT64_MAX;
	uint64_t curr = UINT64_MAX;
	// write with blocks merging
	for (block_list::const_iterator i = blocks.begin(); i != blocks.end(); i++) {
		m_stats.m_writes++;
		uint64_t n = i->first;
		block_ptr & pb = *i->second;
		assert(n < m_capacity);
		switch (n) {
		case 0:
			m_pb0 = pb;
			break;
		case 1:
			m_pb1 = pb;
			break;
		}
		if (buf.size() == 0) {
			// put first block in merging buffer
			first = n;
			curr = n;
			buf.put(pb.prepare_read(), m_block_size);
		} else if (n == curr + 1) {
			// add block to merging buffer
			curr = n;
			buf.put(pb.prepare_read(), m_block_size);
		} else {
			// start new merge
			buf.rewind();
			first = n;
			curr = n;
			buf.put(pb.prepare_read(), m_block_size);
		}
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_null::read(uint64_t n) {
	assert(n < m_capacity);
	m_stats.m_reads++;
	switch (n) {
	case 0:
		return m_pb0;
	case 1:
		return m_pb1;
		break;
	default:
		throw not_supported();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_null::sync() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t bd_null::block_size() {
	return m_block_size;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t bd_null::capacity() {
	return m_capacity;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_null::alloc() {
	return block_ptr(new block(m_block_size));
}


