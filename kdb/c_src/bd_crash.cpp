///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_crash.cpp
/// Author  : Evgeny Khirin <>
/// Description : Intermediate block device wich simulates power failure by
/// random corruption of last written block.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>

#include "bd_crash.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_device, Name} - name of underlaying device.
//   {seed, Seed} - random number generator's seed. Parameter is optional.
//      If missed, than random number generator initialized with some predefined
//      value.
//--------------------------------------------------------------------
void bd_crash::start(const opt_map & options) {
	scoped_lock<rmutex> lock(m_mutex);
	intermediate_block_device::start(options);
	opt_map::const_iterator it;
	if ((it = options.find("block_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (block_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	if ((it = options.find("seed")) != options.end()) {
		unsigned long seed = strtoul(it->second.c_str(), NULL, 10);
		m_rand.seed(seed);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crash::start(const std::string & name, const std::string & block_device,
										 int32_t seed) {
	printf("bd_crash: name %s, block_device %s\n", name.c_str(), block_device.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["block_device"] = block_device;
	opts["seed"] = to_string(seed);
	bd_crash * pdevice = new bd_crash;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crash::stop() {
	if (m_power_failure) {
		return;
	}
	m_pdevice->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crash::write(uint64_t n, block_ptr & pb) {
	reproducible_ops();
	scoped_lock<rmutex> lock(m_mutex);
	if (m_power_failure) {
		throw power_failure();
	}
	assert(n < capacity());
	if (m_last_n != UINT64_MAX) {
		m_pdevice->write(m_last_n, m_last_pb);
	}
	m_last_n = n;
	m_last_pb = pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crash::write(block_list & blocks) {
	reproducible_ops();
	scoped_lock<rmutex> lock(m_mutex);
	if (m_power_failure) {
		throw power_failure();
	}
	if (!blocks.empty()) {
		if (m_last_n != UINT64_MAX) {
			m_pdevice->write(m_last_n, m_last_pb);
		}
		m_last_n = blocks.back().first;
		assert(m_last_n < capacity());
		m_last_pb = *blocks.back().second;
		blocks.pop_back();
	}
	m_pdevice->write(blocks);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_crash::read(uint64_t n) {
	scoped_lock<rmutex> lock(m_mutex);
	assert(n < capacity());
	if (m_power_failure) {
		throw power_failure();
	}
	if (n == m_last_n) {
		return m_last_pb;
	}
	return m_pdevice->read(n);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crash::sync() {
	reproducible_ops();
	scoped_lock<rmutex> lock(m_mutex);
	if (m_power_failure) {
		throw power_failure();
	}
	if (m_last_n != UINT64_MAX) {
		m_pdevice->write(m_last_n, m_last_pb);
		m_last_pb = block_ptr();
	}
	m_last_n = UINT64_MAX;
	m_pdevice->sync();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_crash::ops(bool corrupt) {
	scoped_lock<rmutex> lock(m_mutex);
#ifdef _WIN32
	printf("bd_crash::power_failure (%s): corrupt flag %d, last_written %I64d\n",
				 get_name().c_str(), (int)corrupt, m_last_n);
#else
	printf("bd_crash::power_failure (%s): corrupt flag %d, last_written %lld\n",
				 get_name().c_str(), (int)corrupt, m_last_n);
#endif
	m_power_failure = true;
	if (!corrupt || m_last_n == UINT64_MAX) {
		return;
	}
	const char * last_b = m_last_pb.prepare_read();
	block_ptr old_pb;
	try {
		old_pb = m_pdevice->read(m_last_n);
	} catch (read_failed &) {
		old_pb = m_pdevice->alloc();
		char * b = old_pb.prepare_write();
		memset(b, 0xdd, m_pdevice->block_size());
	}
	char * old_b = old_pb.prepare_write();
	uint32_t block_size = m_pdevice->block_size();
	uint32_t written_bytes = m_rand() % block_size;
	memcpy(old_b, last_b, written_bytes);
	m_pdevice->write(m_last_n, old_pb);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void bd_crash::reproducible_ops() {
/*
	static const char * crash_devs[] = {
		"log_bd_crash",
		"data_bd_crash"
	};
	if ((m_rand() % 30000) == 1) {
		for (unsigned i = 0; i < sizeof(crash_devs) / sizeof(*crash_devs); i++) {
			bd_crash * pcrash = (bd_crash *)whereis(crash_devs[i]);
			if (pcrash != NULL) {
				pcrash->ops(true);
			}
		}
		throw power_failure();
	}
*/
}

