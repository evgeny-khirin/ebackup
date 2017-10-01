///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_adler.hpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device - data integrity controlled
/// by ADLER-32 check sum.
///-------------------------------------------------------------------
#include <stdio.h>

#include "bd_adler.hpp"
#include "utils.hpp"
#include "zlib.h"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//--------------------------------------------------------------------
void bd_adler::start(const opt_map & options) {
	intermediate_block_device::start(options);
	opt_map::const_iterator it;
	if ((it = options.find("block_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (block_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	m_block_size = m_pdevice->block_size() - 4;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_adler::start(const std::string & name, const std::string & block_device) {
	printf("bd_adler: name %s, block_device %s\n", name.c_str(), block_device.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	bd_adler * pdevice = new bd_adler;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_adler::write(uint64_t n, block_ptr & pb) {
	// add block's metadata
	char * b = pb.prepare_write();
	uint32_t adler = adler32(0L, Z_NULL, 0);
	adler = adler32(adler, (const Bytef *)b, m_block_size);
	*((uint32_t *)(b + m_block_size)) = adler;
	m_pdevice->write(n, pb);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_adler::write(block_list & blocks) {
	for (block_list::iterator i = blocks.begin(); i != blocks.end(); i++) {
		// add block's metadata
		block_ptr & pb = *i->second;
		char * b = pb.prepare_write();
		uint32_t adler = adler32(0L, Z_NULL, 0);
		adler = adler32(adler, (const Bytef *)b, m_block_size);
		*((uint32_t *)(b + m_block_size)) = adler;
	}
	m_pdevice->write(blocks);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_adler::read(uint64_t n) {
	block_ptr pb = m_pdevice->read(n);
	const char * b = pb.prepare_read();
	uint32_t adler = adler32(0L, Z_NULL, 0);
	adler = adler32(adler, (const Bytef *)b, m_block_size);
	if (*((uint32_t *)(b + m_block_size)) != adler) {
		throw bad_checksum();
	}
	return pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t bd_adler::block_size() {
	return m_block_size;
}

