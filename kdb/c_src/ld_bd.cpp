///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : ld_bd.hpp
/// Author  : Evgeny Khirin <>
/// Description : Log device based on block device. Most tricks in this
/// module deal with devices, which flushes and writes blocks in disorder.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <algorithm>

#include "ld_bd.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::format(const std::string & dev_name, const std::string & app_name) {
	block_device * device = (block_device *)whereis(dev_name);
	if (device == NULL) {
		throw not_registered();
	}
	format(device, app_name);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::format(block_device * dev, const std::string & app_name) {
	uint32_t block_size = dev->block_size();
	if (block_size < sizeof(header_t)) {
		throw block_too_small();
	}
	if (app_name.length() > FIELD_SIZE(header_t, m_app_name)) {
		throw internal_error();
	}
	header_t header(true);;
	try {
		header_t old_header(false);
		read_header(dev, old_header);
		// if device was formatted before, continue its session and
		// system generation.
		header.m_session = old_header.m_session + 1;
		header.m_sys_gen = old_header.m_sys_gen + 1;
	} catch (std::exception &) {
	}
	memcpy(header.m_app_name, app_name.c_str(), app_name.length());
	header.m_capacity = dev->capacity();
	// Clean up header 0
	block_ptr pb = dev->alloc();
	char * b = pb.prepare_write();
	memset(b, 0, block_size);
	dev->write(0, pb);
	// clean up header 1
	dev->write(1, pb);
	// Invalidate logical block number of first logical block. So it will be
	// impossible to read it.
	*((uint64_t *)(b + block_size - 16)) = UINT64_MAX;
	dev->write(2, pb);
	// write header, device will be synced automatically
	write_header(dev, header);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
bool ld_bd::is_formatted(const std::string & dev_name, const std::string & app_name) {
	block_device * device = (block_device *)whereis(dev_name);
	if (device == NULL) {
		throw not_registered();
	}
	return is_formatted(device, app_name);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
bool ld_bd::is_formatted(block_device * dev, const std::string & app_name) {
	uint32_t block_size = dev->block_size();
	if (block_size < sizeof(header_t)) {
		throw block_too_small();
	}
	if (app_name.length() > FIELD_SIZE(header_t, m_app_name)) {
		throw internal_error();
	}
	try {
		header_t h(false);
		read_header(dev, h);
		if (dev->capacity() < h.m_capacity) {
			return false;
		}
		char cp_app_name[FIELD_SIZE(header_t, m_app_name)];
		memset(cp_app_name, 0, sizeof(cp_app_name));
		memcpy(cp_app_name, app_name.c_str(), app_name.length());
		return (memcmp(h.m_app_name, cp_app_name, sizeof(cp_app_name)) == 0);
	} catch (std::exception &) {
		return false;
	}
	return true;
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
void ld_bd::read_header(block_device * dev, header_t & header) {
	block_ptr pb0;
	const header_t * phead0 = NULL;
	block_ptr pb1;
	const header_t * phead1 = NULL;
	try {
		// try to read first header
		pb0 = dev->read(0);
		phead0 = (const header_t *)pb0.prepare_read();
	} catch (std::exception &) {
	}
	try {
		// try to read second header
		pb1 = dev->read(1);
		phead1 = (const header_t *)pb1.prepare_read();
	} catch (std::exception &) {
	}
	// analyze first header
	if (phead0->is_ok()) {
		// first header is ok, try to check second
		if (!phead1->is_ok()) {
			// second header is invalid - return first, which is ok.
			memcpy(&header, phead0, sizeof(header_t));
			return;
		}
		// both headers are ok - compare their generations
		if (phead0->m_generation > phead1->m_generation) {
			memcpy(&header, phead0, sizeof(header_t));
			return;
		} else {
			memcpy(&header, phead1, sizeof(header_t));
			return;
		}
	}
	// first header is invalid check second.
	if (!phead1->is_ok()) {
		throw bad_header();
	}
	memcpy(&header, phead1, sizeof(header_t));
	return;
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
void ld_bd::write_header(block_device * pdev, header_t & header) {
	header.m_generation++;
	uint64_t n = header.m_generation % 2;
	block_ptr pb = pdev->alloc();
	char * b = pb.prepare_write();
	memcpy(b, &header, sizeof(header));
	// first sync data and then update the header. otherwise
	// header may be written, but data no. So data before tail will be
	// invalid.
	pdev->sync();
	pdev->write(n, pb);
	pdev->sync();
}

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//--------------------------------------------------------------------
void ld_bd::start(const opt_map & options) {
	log_device::start(options);
	opt_map::const_iterator it;
	if ((it = options.find("block_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (block_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	m_block_size = m_pdevice->block_size();
	if (m_block_size < std::max((uint32_t)17, (uint32_t)sizeof(header_t))) {
		throw block_too_small();
	}
	m_block_size -= 16;
	read_header(m_pdevice, m_header);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::start(const std::string & name, const std::string & block_device) {
	printf("ld_bd: name %s, block_device %s\n",
				 name.c_str(), block_device.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	ld_bd * pdevice = new ld_bd;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t ld_bd::block_size() {
	return m_block_size;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t ld_bd::get_head() {
	return m_header.m_head_lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t ld_bd::get_checkpoint() {
	return m_header.m_checkpoint_lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t ld_bd::get_tail() {
	return m_header.m_tail_lsn;
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
uint64_t ld_bd::translate_block_n(uint64_t logical_n) {
	// correct capacity since two first blocks are reserved for headers
	uint64_t effective_capacity = m_header.m_capacity - 2;
	uint64_t distance = logical_n - m_header.m_head_logical;
	if (distance >= effective_capacity) {
		throw out_of_capacity();
	}
	// translate block number
	uint64_t physical_n = m_header.m_head_phys + distance;
	if (physical_n < m_header.m_capacity) {
		return physical_n;
	}
	return physical_n - m_header.m_capacity + 2;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr ld_bd::read(uint64_t logical_n) {
	uint64_t physical_n = translate_block_n(logical_n);
	block_ptr pb = m_pdevice->read(physical_n);
	// check that block number stored in block's metadata is correct
	const char * b = pb.prepare_read();
	uint64_t stored_n = *((uint64_t *)(b + m_block_size));
	if (stored_n != logical_n) {
		throw block_never_written();
	}
	// make sure that block has correct session number, higher blocks numbers
	// from old sessions are discarded.
	uint64_t stored_session = *((uint64_t *)(b + m_block_size + 8));
	if (logical_n >= m_header.m_first_session_block &&
			stored_session < m_header.m_session) {
		throw block_never_written();
	}
	return pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::write(uint64_t logical_n, block_ptr & pb) {
	// check capacity
	check_capacity();
	uint64_t physical_n = translate_block_n(logical_n);
	// add block's metadata
	char * b = pb.prepare_write();
	*((uint64_t *)(b + m_block_size)) = logical_n;
	*((uint64_t *)(b + m_block_size + 8)) = m_header.m_session;
	m_pdevice->write(physical_n, pb);
	// update max written block number.
	m_max_block_n = std::max(m_max_block_n, logical_n);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::set_head(uint64_t lsn, uint64_t head_block, uint64_t checkpoint_lsn) {
	// work with copy of header
	header_t new_header = m_header;
	new_header.m_head_logical = head_block;
	new_header.m_head_phys = translate_block_n(new_header.m_head_logical);
	new_header.m_head_lsn = lsn;
	new_header.m_checkpoint_lsn = checkpoint_lsn;
	write_header(m_pdevice,new_header);
	// everything is ok - update header and reset syncs counter
	m_header = new_header;
	m_syncs = 0;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::set_tail(uint64_t lsn, uint64_t tail_block) {
	// work with copy of header
	header_t new_header = m_header;
	new_header.m_tail_lsn = lsn;
	new_header.m_session++;
	new_header.m_first_session_block = tail_block + 1;
	write_header(m_pdevice,new_header);
	// everything is ok - update header and reset syncs counter
	m_header = new_header;
	m_syncs = 0;
	m_max_block_n = tail_block;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::sync(uint64_t tail_lsn) {
	assert(tail_lsn >= m_header.m_tail_lsn);
	m_syncs++;
	// write header to save new tail every 100 syncs
	if (m_syncs < 100) {
		m_pdevice->sync();
		m_header.m_tail_lsn = tail_lsn;
	} else {
		// work with copy of header
		header_t new_header = m_header;
		new_header.m_tail_lsn = tail_lsn;
		write_header(m_pdevice,new_header);
		// everything is ok - update header and reset syncs counter
		m_header = new_header;
		m_syncs = 0;
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::check_capacity() {
	uint64_t new_capacity = m_pdevice->capacity();
	if (new_capacity == m_header.m_capacity) {
		return;
	}
	if (m_max_block_n <= m_header.m_head_logical) {
		// nothing is written still
		return;
	}
	uint64_t max_phys = translate_block_n(m_max_block_n);
	// We can upgrade capacity when data in buffer reside sequentially
	// and continuously only.
	if (max_phys - m_header.m_head_phys != m_max_block_n - m_header.m_head_logical) {
		return;
	}
	// work with copy of header
	header_t new_header = m_header;
	new_header.m_capacity = new_capacity;
	write_header(m_pdevice,new_header);
	// everything is ok - update header and reset syncs counter
	m_header = new_header;
	m_syncs = 0;
	return;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::stop() {
	if (m_syncs != 0) {
		write_header(m_pdevice, m_header);
	}
	m_pdevice->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::del_underlaying() {
	m_pdevice->del_underlaying();
	delete m_pdevice;
	m_pdevice = NULL;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void ld_bd::stats(no_stats_list & list) {
	m_pdevice->stats(list);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr ld_bd::alloc() {
	return m_pdevice->alloc();
}
