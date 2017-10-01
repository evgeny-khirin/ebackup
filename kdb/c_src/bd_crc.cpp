///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_crc.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device - data integrity controlled
/// by CRC-32 check sum.
///-------------------------------------------------------------------
#include <stdio.h>

#include "bd_crc.hpp"
#include "utils.hpp"
#include "zlib.h"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//--------------------------------------------------------------------
void bd_crc::start(const opt_map & options) {
	intermediate_block_device::start(options);
	opt_map::const_iterator it;
	// resolve block device
	if ((it = options.find("block_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (block_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	// set block size
	m_block_size = m_pdevice->block_size() - 4;
	// set trailer
	if ((it = options.find("trailer")) != options.end()) {
#ifdef _WIN32
		m_trailer = _strtoui64(it->second.c_str(), NULL, 10);
#else
		m_trailer = strtoull(it->second.c_str(), NULL, 10);
#endif
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crc::start(const std::string & name, const std::string & block_device,
									 uint64_t trailer) {
#ifdef _WIN32
	printf("bd_crc: name %s, block_device %s, trailer %I64u\n",
				 name.c_str(), block_device.c_str(), trailer);
#else
	printf("bd_crc: name %s, block_device %s, trailer %llu\n",
				 name.c_str(), block_device.c_str(), trailer);
#endif
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	opts["trailer"] = to_string(trailer);
	bd_crc * pdevice = new bd_crc;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crc::write(uint64_t n, block_ptr & pb) {
	// add block's metadata
	char * b = pb.prepare_write();
	uint32_t crc = crc32(0L, Z_NULL, 0);
	crc = crc32(crc, (const Bytef *)b, m_block_size);
	crc = crc32(crc, (const Bytef *)&m_trailer, sizeof(m_trailer));
	*((uint32_t *)(b + m_block_size)) = crc;
	m_pdevice->write(n, pb);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crc::write(block_list & blocks) {
	for (block_list::iterator i = blocks.begin(); i != blocks.end(); i++) {
		// add block's metadata
		block_ptr & pb = *i->second;
		char * b = pb.prepare_write();
		uint32_t crc = crc32(0L, Z_NULL, 0);
		crc = crc32(crc, (const Bytef *)b, m_block_size);
		crc = crc32(crc, (const Bytef *)&m_trailer, sizeof(m_trailer));
		*((uint32_t *)(b + m_block_size)) = crc;
	}
	m_pdevice->write(blocks);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_crc::read(uint64_t n) {
	block_ptr pb = m_pdevice->read(n);
	const char * b = pb.prepare_read();
	uint32_t crc = crc32(0L, Z_NULL, 0);
	crc = crc32(crc, (const Bytef *)b, m_block_size);
	crc = crc32(crc, (const Bytef *)&m_trailer, sizeof(m_trailer));
	if (*((uint32_t *)(b + m_block_size)) != crc) {
		throw bad_checksum();
	}
	return pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t bd_crc::block_size() {
	return m_block_size;
}

