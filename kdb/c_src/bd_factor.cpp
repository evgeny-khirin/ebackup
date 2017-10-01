///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_factor.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device - block size is  multiplied up
/// by constant factor.
///-------------------------------------------------------------------
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "bd_factor.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//   {factor, Factor} - multiplying factor. Factor must be positive integer.
//--------------------------------------------------------------------
void bd_factor::start(const opt_map & options) {
	intermediate_block_device::start(options);
	opt_map::const_iterator it;
	if ((it = options.find("block_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (block_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	if ((it = options.find("factor")) == options.end()) {
		throw missed_param();
	}
#ifdef _WIN32
	uint32_t factor = (uint32_t)_strtoui64(it->second.c_str(), NULL, 10);
#else
	uint32_t factor = strtoull(it->second.c_str(), NULL, 10);
#endif
	if (factor <= 0) {
		throw inv_param();
	}
	m_capacity = m_pdevice->capacity() / factor;
	if (m_capacity == 0) {
		throw capacity_too_small();
	}
	if ((it = options.find("stop_underlaying")) == options.end()) {
		m_stop_underlaying = true;
	} else {
		if (it->second == "true") {
			m_stop_underlaying = true;
		} else if (it->second == "false") {
			m_stop_underlaying = false;
		} else {
			throw inv_param();
		}
	}
	m_block_size = m_pdevice->block_size() * factor;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_factor::start(const std::string & name, const std::string & block_device,
											uint32_t factor, bool stop_underlaying) {
	printf("bd_factor: name %s, block_device %s, factor %u, stop_underlaying %d\n",
				 name.c_str(), block_device.c_str(), factor, stop_underlaying);
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	opts["factor"] = to_string(factor);
	opts["stop_underlaying"] = to_string(stop_underlaying);
	bd_factor * pdevice = new bd_factor;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_factor::stop() {
	if (m_stop_underlaying) {
		intermediate_block_device::stop();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_factor::del_underlaying() {
	if (m_stop_underlaying) {
		intermediate_block_device::del_underlaying();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_factor::write(uint64_t n, block_ptr & pb) {
	assert(n < m_capacity);
	m_pdevice->write(n, m_block_size, pb);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_factor::write(block_list & blocks) {
	m_pdevice->write(m_block_size, blocks);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_factor::read(uint64_t n) {
	return m_pdevice->read(n, m_block_size);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t bd_factor::block_size() {
	return m_block_size;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t bd_factor::capacity() {
	return m_capacity;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_factor::alloc() {
	return block_ptr(new block(m_block_size));
}


