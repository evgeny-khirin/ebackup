///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_disord.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device wich simulates real devices.
///               Main feature - blocks are flushed and written to underlaying
///               device in disorder.
///-------------------------------------------------------------------
#include <stdio.h>
#include <vector>
#include <algorithm>

#include "bd_disord.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//   {buffer_capacity, Capacity} - capacity of buffer in blocks.
//   {seed, Seed} - random number generator's seed. Parameter is optional.
//      If missed, than random number generator initialized with some predefined
//      value.
//--------------------------------------------------------------------
void bd_disord::start(const opt_map & options) {
	intermediate_block_device::start(options);
	opt_map::const_iterator it;
	if ((it = options.find("block_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (block_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	if ((it = options.find("buffer_capacity")) == options.end()) {
		throw missed_param();
	}
	m_buffer_capacity = strtoul(it->second.c_str(), NULL, 10);
	if (m_buffer_capacity <= 0) {
		throw inv_param();
	}
	m_buffer_index = new uint64_t[m_buffer_capacity];
	for (uint32_t i = 0; i < m_buffer_capacity; i++) {
		m_buffer_index[i] = -1;
	}
	if ((it = options.find("seed")) != options.end()) {
		unsigned long seed = strtoul(it->second.c_str(), NULL, 10);
		m_rand.seed(seed);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_disord::start(const std::string & name, const std::string & block_device,
											uint32_t buffer_capacity, int32_t seed) {
	printf("bd_disord: name %s, block_device %s, buffer_capacity %u\n",
				 name.c_str(), block_device.c_str(), buffer_capacity);
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	opts["buffer_capacity"] = to_string(buffer_capacity);
	opts["seed"] = to_string(seed);
	bd_disord * pdevice = new bd_disord;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_disord::stop() {
	flush();
	m_buffer.clear();
	delete m_buffer_index;
	m_buffer_index = NULL;
	m_pdevice->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_disord::del_underlaying() {
	m_buffer.clear();
	intermediate_block_device::del_underlaying();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_disord::write(uint64_t n, block_ptr & pb) {
	buffer_t acc;
	push(n, pb, acc);
	if (acc.empty()) {
		return;
	}
	for (buffer_t::iterator i = acc.begin(); i != acc.end(); i++) {
		m_pdevice->write(i->first, i->second);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_disord::write(block_list & blocks) {
	buffer_t acc;
	for (block_list::iterator i = blocks.begin(); i != blocks.end(); i++) {
		uint64_t n = i->first;
		block_ptr & pb = *i->second;
		push(n, pb, acc);
	}
	if (acc.empty()) {
		return;
	}
	block_list new_blocks;
	for (buffer_t::iterator i = acc.begin(); i != acc.end(); i++) {
		new_blocks.push_back(std::pair<uint64_t, block_ptr *>(i->first, &i->second));
	}
	m_pdevice->write(new_blocks);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_disord::read(uint64_t n) {
	buffer_t::iterator it = m_buffer.find(n);
	if (it != m_buffer.end()) {
		return it->second;
	}
	return m_pdevice->read(n);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_disord::sync() {
	flush();
	m_pdevice->sync();
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
void bd_disord::flush() {
	if (m_buffer.size() == 0) {
		return;
	}
	block_list l;
	for (buffer_t::iterator i = m_buffer.begin(); i != m_buffer.end(); i++) {
		uint64_t n = i->first;
		block_ptr & pb = i->second;
		l.push_back(std::pair<uint64_t, block_ptr *>(n, &pb));
	}
	m_pdevice->write(l);
	m_buffer.clear();
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
void bd_disord::push(uint64_t n, block_ptr& pb, buffer_t & acc) {
	buffer_t::iterator it = m_buffer.find(n);
	if (it != m_buffer.end()) {
		// block already in buffer - just update it.
		it->second = pb;
		return;
	}
	uint32_t buffer_size = m_buffer.size();
	if (buffer_size < m_buffer_capacity) {
		// add new block to buffer;
		m_buffer_index[buffer_size] = n;
		m_buffer[n] = pb;
		return;
	}
	// pop random block and insert new on its place
	uint32_t idx = m_rand() % m_buffer_capacity;
	uint64_t pop_n = m_buffer_index[idx];
	acc[pop_n] = m_buffer[pop_n];
	m_buffer.erase(pop_n);
	m_buffer_index[idx] = n;
	m_buffer[n] = pb;
}

