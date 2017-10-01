///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_pool.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device. Implements pool of devices.
/// Each underlaying device must have same block size. But capacity of each
/// device may be different.
///-------------------------------------------------------------------
#include <assert.h>
#include <stdio.h>

#include "bd_pool.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {devices, [Name]} - list of underlying block devices names. List of
//      devices may be empty.
//   {block_size, BlockSize} - size of block in bytes. Parameter is obligatory
//      when device list is empty. If list is not empty, block size is
//      obtained from first device.
//--------------------------------------------------------------------
void bd_pool::start(const opt_map & options) {
	block_device::start(options);
	m_stats.m_reg_name = get_name();
	opt_map::const_iterator it;
	if ((it = options.find("devices")) == options.end()) {
		throw missed_param();
	}
	std::list<std::string> dev_list;
	parse_atoms_list(it->second, dev_list);
	if (dev_list.empty()) {
		if ((it = options.find("block_size")) == options.end()) {
			throw missed_param();
		}
		m_block_size = strtoul(it->second.c_str(), NULL, 10);
	}
	uint64_t capacity = 0;
	for (std::list<std::string>::iterator i = dev_list.begin(); i != dev_list.end(); i++) {
		block_device * pdevice = (block_device *)whereis(*i);
		if (pdevice == NULL) {
			throw not_registered();
		}
		m_devices.push_back(dev_descr_t(pdevice, capacity));
		capacity += pdevice->capacity();
	}
	m_capacity = capacity;
	if (!dev_list.empty()) {
		m_block_size = m_devices[0].first->block_size();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::start(const std::string & name, const std::string & devices,
										uint32_t block_size) {
	printf("bd_pool: name %s, devices %s, block_size %u\n",
				 name.c_str(), devices.c_str(), block_size);
	opt_map	opts;
	opts["name"] = name;
	opts["devices"] = devices;
	opts["block_size"] = to_string(block_size);
	bd_pool * pdevice = new bd_pool;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::stop() {
	for (devices_t::iterator i = m_devices.begin(); i != m_devices.end(); i++) {
		i->first->stop();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::del_underlaying() {
	for (devices_t::iterator i = m_devices.begin(); i != m_devices.end(); i++) {
		i->first->del_underlaying();
		delete i->first;
	}
	m_devices.clear();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::stats(no_stats_list & list) {
	list.push_back(&m_stats);
	for (devices_t::iterator i = m_devices.begin(); i != m_devices.end(); i++) {
		i->first->stats(list);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::write(uint64_t n, block_ptr & pb) {
	assert(n < m_capacity);
	m_stats.m_writes++;
	uint32_t dev_n = device_for_block(n);
	block_device * pdevice = m_devices[dev_n].first;
	uint64_t dev_first_block = m_devices[dev_n].second;
	pdevice->write(n - dev_first_block, pb);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::write(block_list & blocks) {
	uint32_t dev_count = m_devices.size();
	std::vector<block_list> dev_lists(dev_count);
	uint32_t len = 0;
	for (block_list::iterator i = blocks.begin(); i != blocks.end(); i++, len++) {
		uint64_t n = i->first;
		block_ptr & pb = *i->second;
		assert(n < m_capacity);
		uint32_t dev_n = device_for_block(n);
		uint64_t dev_first_block = m_devices[dev_n].second;
		dev_lists[dev_n].push_back(std::pair<uint64_t, block_ptr *>(n - dev_first_block, &pb));
	}
	m_stats.m_writes += len;
	for (uint32_t i = 0; i < dev_count; i++) {
		if (!dev_lists[i].empty()) {
			block_device * pdevice = m_devices[i].first;
			pdevice->write(dev_lists[i]);
		}
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_pool::read(uint64_t n) {
	assert(n < m_capacity);
	m_stats.m_reads++;
	uint32_t dev_idx = device_for_block(n);
	block_device * pdevice = m_devices[dev_idx].first;
	uint64_t dev_first_block = m_devices[dev_idx].second;
	return pdevice->read(n - dev_first_block);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::sync() {
	uint32_t dev_count = m_devices.size();
	for (uint32_t i = 0; i < dev_count; i++) {
		block_device * pdevice = m_devices[i].first;
		pdevice->sync();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t bd_pool::block_size() {
	return m_block_size;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t bd_pool::capacity() {
	return m_capacity;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_pool::alloc() {
	return block_ptr(new block(m_block_size));
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::add_device(const std::string& name) {
	block_device * pdevice = (block_device *)whereis(name);
	if (pdevice == NULL) {
		throw not_registered();
	}
	add_device(pdevice);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_pool::add_device(block_device * pdevice) {
	m_devices.push_back(dev_descr_t(pdevice, m_capacity));
	m_capacity += pdevice->capacity();
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
uint32_t bd_pool::device_for_block(uint64_t n) {
	uint32_t low = 0;
	uint32_t high = m_devices.size() - 1;
	uint32_t i = high / 2;
	while (low != high) {
		block_device * pdevice = m_devices[i].first;
		uint64_t first_block = m_devices[i].second;
		if (n < first_block) {
			high = i - 1;
			i = low + ((high - low) / 2);
		} else if (n < first_block + pdevice->capacity()) {
			return i;
		} else {
			low = i + 1;
			i = low + (high - i) / 2;
 		}
	}
	return low;
}

