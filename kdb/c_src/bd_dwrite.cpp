///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_dwrite.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device, implementing delayed write algorithm.
///-------------------------------------------------------------------
#include <stdio.h>

#include "bd_dwrite.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//   {buffer_capacity, Capacity} - capacity of buffer in blocks.
//--------------------------------------------------------------------
void bd_dwrite::start(const opt_map & options) {
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
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_dwrite::start(const std::string & name, const std::string & block_device,
											uint32_t buffer_capacity) {
	printf("bd_dwrite: name %s, block_device %s, buffer_capacity %u\n",
				 name.c_str(), block_device.c_str(), buffer_capacity);
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	opts["buffer_capacity"] = to_string(buffer_capacity);
	bd_dwrite * pdevice = new bd_dwrite;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_dwrite::stop() {
	flush();
	m_buffer.clear();
	m_pdevice->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_dwrite::del_underlaying() {
	m_buffer.clear();
	intermediate_block_device::del_underlaying();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_dwrite::write(uint64_t n, block_ptr & pb) {
	m_buffer[n] = pb;
	if (m_buffer.size() >= m_buffer_capacity) {
		flush();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_dwrite::write(block_list & blocks) {
	for (block_list::iterator i = blocks.begin(); i != blocks.end(); i++) {
		uint64_t n = i->first;
		block_ptr& pb = *i->second;
		m_buffer[n] = pb;
		if (m_buffer.size() >= m_buffer_capacity) {
			flush();
		}
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_dwrite::read(uint64_t n) {
	buffer_t::iterator it = m_buffer.find(n);
	if (it != m_buffer.end()) {
		return it->second;
	}
	return m_pdevice->read(n);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_dwrite::sync() {
	flush();
	m_pdevice->sync();
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
void bd_dwrite::flush() {
	if (m_buffer.size() == 0) {
		return;
	}
	block_list l;
	for (buffer_t::iterator i = m_buffer.begin(); i != m_buffer.end(); i++) {
		uint64_t n = i->first;
		block_ptr & pb = i->second;
		l.push_back(std::pair<uint64_t, block_ptr * >(n, &pb));
	}
	m_pdevice->write(l);
	m_buffer.clear();
}

