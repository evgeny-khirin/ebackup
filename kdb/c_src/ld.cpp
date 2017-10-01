///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : ld.hpp
/// Author  : Evgeny Khirin <>
/// Description : Log device interface. It provides two important abstaractions
/// for logger:
///    1. Infinite device. Logger always writes to device sequantialy and can
///       peridocally trim device from begining. Last block can be written multiple
///       times, so device should take care that early written and flushed data remain
///       correct in those cases (ping-pong write).
///    2. Early error detection feature. If block is written to device, than
///       on read device should return correct block or error. Attempt to read
///       block that never written should return error.
///-------------------------------------------------------------------
#include "ld.hpp"

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
void log_device::write(block_list& blocks) {
	throw not_supported();
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
void log_device::sync() {
	throw not_supported();
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
uint64_t log_device::capacity() {
	throw not_supported();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_log_device::stop() {
	m_pdevice->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_log_device::del_underlaying() {
	m_pdevice->del_underlaying();
	delete m_pdevice;
	m_pdevice = NULL;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_log_device::stats(no_stats_list & list) {
	m_pdevice->stats(list);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t intermediate_log_device::get_head() {
	return m_pdevice->get_head();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t intermediate_log_device::get_checkpoint() {
	return m_pdevice->get_checkpoint();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_log_device::set_head(uint64_t lsn, uint64_t head_block,
																			 uint64_t checkpoint_lsn) {
	m_pdevice->set_head(lsn, head_block, checkpoint_lsn);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t intermediate_log_device::get_tail() {
	return m_pdevice->get_tail();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_log_device::set_tail(uint64_t lsn, uint64_t tail_block) {
	m_pdevice->set_tail(lsn, tail_block);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr intermediate_log_device::read(uint64_t n) {
	return m_pdevice->read(n);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_log_device::write(uint64_t n, block_ptr & pb) {
	m_pdevice->write(n, pb);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void intermediate_log_device::sync(uint64_t tail_lsn) {
	m_pdevice->sync(tail_lsn);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t intermediate_log_device::block_size() {
	return m_pdevice->block_size();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr intermediate_log_device::alloc() {
	return m_pdevice->alloc();
}


