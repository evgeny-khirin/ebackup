///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd.cpp
/// Author  : Evgeny Khirin <>
/// Description : Provides abstraction of general block device.
///-------------------------------------------------------------------
#include "bd.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// block stats
//--------------------------------------------------------------------
block::stats_t		block::m_stats;

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void block::stats_t::soft_reset() {
	m_allocs = m_frees = m_copies = 0;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
std::string block::stats_t::to_string() {
	std::stringstream ss;
	ss << no_stats::to_string() << "allocs " << m_allocs << ", frees " <<
		m_frees << ", copies " << m_copies;
	return ss.str();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_stats::soft_reset() {
	m_writes = m_reads = 0;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
std::string bd_stats::to_string() {
	std::stringstream ss;
	ss << no_stats::to_string() << "writes " << m_writes << ", reads " << m_reads;
	return ss.str();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void block_device::write(uint64_t n, uint32_t block_size, block_ptr & pb) {
	throw not_supported();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void block_device::write(uint32_t block_size, block_list & blocks) {
	throw not_supported();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr block_device::read(uint64_t n, uint32_t block_size) {
	throw not_supported();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_block_device::stop() {
	m_pdevice->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_block_device::del_underlaying() {
	m_pdevice->del_underlaying();
	delete m_pdevice;
	m_pdevice = NULL;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_block_device::stats(no_stats_list & list) {
	m_pdevice->stats(list);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_block_device::write(uint64_t n, block_ptr & pb) {
	m_pdevice->write(n, pb);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_block_device::write(block_list & blocks) {
	m_pdevice->write(blocks);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr intermediate_block_device::read(uint64_t n) {
	return m_pdevice->read(n);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_block_device::sync() {
	m_pdevice->sync();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t intermediate_block_device::block_size() {
	return m_pdevice->block_size();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t intermediate_block_device::capacity() {
	return m_pdevice->capacity();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr intermediate_block_device::alloc() {
	return m_pdevice->alloc();
}


