///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_part.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device - presents partition on top of
/// underlaying device.
///-------------------------------------------------------------------
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdio.h>
#include <assert.h>

#include "bd_part.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_part::stats(no_stats_list & list) {
	list.push_back(&m_stats);
	if (m_start == 0) {
		m_pdevice->stats(list);
	}
}

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//   {start, Start} - first block of partion.
//   {capacity, Capacity} - capacity of partition in blocks.
//--------------------------------------------------------------------
void bd_part::start(const opt_map & options) {
	intermediate_block_device::start(options);
	m_stats.m_reg_name = get_name();
	opt_map::const_iterator it;
	if ((it = options.find("block_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (block_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	if ((it = options.find("start")) == options.end()) {
		throw missed_param();
	}
	uint64_t capacity = m_pdevice->capacity();
#ifdef _WIN32
	m_start = _strtoui64(it->second.c_str(), NULL, 10);
#else
	m_start = strtoull(it->second.c_str(), NULL, 10);
#endif
	if (m_start >= capacity) {
		throw inv_param();
	}
	if ((it = options.find("capacity")) == options.end()) {
		throw missed_param();
	};
#ifdef _WIN32
	m_capacity = _strtoui64(it->second.c_str(), NULL, 10);
#else
	m_capacity = strtoull(it->second.c_str(), NULL, 10);
#endif
	if (m_capacity <= 0 || m_start + m_capacity > capacity) {
		throw inv_param();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_part::start(const std::string & name, const std::string & block_device,
										uint64_t start, uint64_t capacity) {
#ifdef _WIN32
	printf("bd_part: name %s, block_device %s, start %I64u, capacity %I64u, ",
				 name.c_str(), block_device.c_str(), start, capacity);
#else
	printf("bd_part: name %s, block_device %s, start %" PRIu64 ", capacity %" PRIu64,
				 name.c_str(), block_device.c_str(), start, capacity);
#endif
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	opts["start"] = to_string(start);
	opts["capacity"] = to_string(capacity);
	bd_part * pdevice = new bd_part;
	pdevice->start(opts);
	printf("size %f GB\n", (double)capacity * pdevice->block_size() / 1024 / 1024 / 1024);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_part::stop() {
	if (m_start == 0) {
		intermediate_block_device::stop();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_part::del_underlaying() {
	if (m_start == 0) {
		intermediate_block_device::del_underlaying();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_part::write(uint64_t n, block_ptr & pb) {
	assert(n < m_capacity);
	m_stats.m_writes++;
	m_pdevice->write(m_start + n, pb);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_part::write(block_list & blocks) {
	uint32_t len = 0;
	for (block_list::iterator i = blocks.begin(); i != blocks.end(); i++, len++) {
		uint64_t n = i->first;
		assert(n < m_capacity);
		i->first = m_start + n;
	}
	m_stats.m_writes += len;
	m_pdevice->write(blocks);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_part::read(uint64_t n) {
	assert(n < m_capacity);
	m_stats.m_reads++;
	return m_pdevice->read(m_start + n);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t bd_part::capacity() {
	return m_capacity;
}
