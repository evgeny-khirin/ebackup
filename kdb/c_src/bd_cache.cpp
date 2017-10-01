///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_cache.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device with cache.
///-------------------------------------------------------------------
#include <assert.h>
#include <stdio.h>

#include "bd_cache.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_cache_stats::soft_reset() {
	bd_stats::soft_reset();
	m_cache_stats->soft_reset();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
std::string bd_cache_stats::to_string() {
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
void bd_cache::stats(no_stats_list & list) {
	list.push_back(&m_stats);
	m_pdevice->stats(list);
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
void bd_cache::save_dirty(void * param, uint64_t & n, block_ptr & pb) {
	bd_cache * obj = (bd_cache *)param;
	obj->m_pdevice->write(n, pb);
}

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//   {cache_capacity, Capacity} - cache capacity in blocks.
//--------------------------------------------------------------------
void bd_cache::start(const opt_map & options) {
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
	if ((it = options.find("cache_capacity")) == options.end()) {
		throw missed_param();
	};
	uint32_t cache_capacity = strtoul(it->second.c_str(), NULL, 10);
	m_cache.start(cache_capacity, save_dirty, this);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_cache::start(const std::string & name, const std::string & block_device,
										 uint32_t cache_capacity) {
	printf("bd_cache: name %s, block_device %s, cache_capacity %u\n",
				 name.c_str(), block_device.c_str(), cache_capacity);
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	opts["cache_capacity"] = to_string(cache_capacity);
	bd_cache * pdevice = new bd_cache;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_cache::stop() {
	m_cache.stop();
	m_pdevice->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_cache::del_underlaying() {
	intermediate_block_device::del_underlaying();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_cache::write(uint64_t n, block_ptr & pb) {
	m_stats.m_writes++;
	m_cache.insert(n, pb, true);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_cache::write(block_list & blocks) {
	for (block_list::iterator i = blocks.begin(); i != blocks.end(); i++) {
		uint64_t n = i->first;
		block_ptr & pb = *i->second;
		m_stats.m_writes++;
		m_cache.insert(n, pb, true);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_cache::read(uint64_t n) {
	m_stats.m_reads++;
	block_ptr pb;
	switch (m_cache.lookup(n, pb)) {
	case ok:
		break;
	case not_found:
		pb = m_pdevice->read(n);
		m_cache.insert(n, pb, false);
		break;
	default:
		assert(false);
	}
	return pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_cache::sync() {
	m_cache.sync();
	m_pdevice->sync();
}

