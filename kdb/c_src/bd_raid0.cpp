///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_raid0.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device. Implements software RAID 0 (striped
/// disks). Each underlaying device must have same capacity and block size.
///-------------------------------------------------------------------
#include <assert.h>
#include <stdio.h>

#include "bd_raid0.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {devices, [Name]} - list of underlying block devices names. List must
//      be non-empty.
//--------------------------------------------------------------------
void bd_raid0::start(const opt_map & options) {
	block_device::start(options);
	m_stats.m_reg_name = get_name();
	opt_map::const_iterator it;
	if ((it = options.find("devices")) == options.end()) {
		throw missed_param();
	}
	std::list<std::string> dev_list;
	parse_atoms_list(it->second, dev_list);
	if (dev_list.empty()) {
		throw inv_param();
	}
	for (std::list<std::string>::iterator i = dev_list.begin(); i != dev_list.end(); i++) {
		block_device * pdevice = (block_device *)whereis(*i);
		if (pdevice == NULL) {
			throw not_registered();
		}
		m_devices.push_back(pdevice);
	}
	m_block_size = m_devices[0]->block_size();
	m_capacity = m_devices[0]->capacity() * m_devices.size();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_raid0::start(const std::string & name, const std::string & devices) {
	printf("bd_raid0: name %s, devices %s\n", name.c_str(), devices.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["devices"] = devices;
	bd_raid0 * pdevice = new bd_raid0;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_raid0::stop() {
	for (devices_t::iterator i = m_devices.begin(); i != m_devices.end(); i++) {
		(*i)->stop();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_raid0::del_underlaying() {
	for (devices_t::iterator i = m_devices.begin(); i != m_devices.end(); i++) {
		(*i)->del_underlaying();
		delete *i;
	}
	m_devices.clear();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_raid0::stats(no_stats_list & list) {
	list.push_back(&m_stats);
	for (devices_t::iterator i = m_devices.begin(); i != m_devices.end(); i++) {
		(*i)->stats(list);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_raid0::write(uint64_t n, block_ptr & pb) {
	assert(n < m_capacity);
	m_stats.m_writes++;
	uint32_t dev_count = m_devices.size();
	uint32_t dev_n = n % dev_count;
	uint64_t block_n = n / dev_count;
	m_devices[dev_n]->write(block_n, pb);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_raid0::write(block_list & blocks) {
	uint32_t dev_count = m_devices.size();
	std::vector<block_list> dev_lists(dev_count);
	uint32_t len = 0;
	for (block_list::iterator i = blocks.begin(); i != blocks.end(); i++, len++) {
		uint64_t n = i->first;
		block_ptr & pb = *i->second;
		assert(n < m_capacity);
		uint32_t dev_n = n % dev_count;
		uint64_t block_n = n / dev_count;
		dev_lists[dev_n].push_back(std::pair<uint64_t, block_ptr *>(block_n, &pb));
	}
	m_stats.m_writes += len;
	for (uint32_t i = 0; i < dev_count; i++) {
		if (!dev_lists[i].empty()) {
			m_devices[i]->write(dev_lists[i]);
		}
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_raid0::read(uint64_t n) {
	assert(n < m_capacity);
	m_stats.m_reads++;
	uint32_t dev_count = m_devices.size();
	return m_devices[n % dev_count]->read(n / dev_count);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_raid0::sync() {
	uint32_t dev_count = m_devices.size();
	for (uint32_t i = 0; i < dev_count; i++) {
		m_devices[i]->sync();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t bd_raid0::block_size() {
	return m_block_size;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t bd_raid0::capacity() {
	return m_capacity;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_raid0::alloc() {
	return block_ptr(new block(m_block_size));
}

