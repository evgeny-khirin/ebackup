///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : logger.cpp
/// Author  : Evgeny Khirin <>
/// Description : Logger for transaction systems.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>
#include <assert.h>

#include "logger.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void blogger::stop() {
	if (m_tail_valid) {
		sync(m_tail_lsn);
	}
	m_tail_pb = block_ptr();
	m_pdevice->stop();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void blogger::del_underlaying() {
	m_tail_pb = block_ptr();
	m_pdevice->del_underlaying();
	delete m_pdevice;
	m_pdevice = NULL;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void blogger::stats(no_stats_list & list) {
	m_pdevice->stats(list);
}

//--------------------------------------------------------------------
// public function
// Supported options:
//		{log_device, Name} - underlaying log device.
//--------------------------------------------------------------------
void blogger::start(const opt_map & options) {
	named_object::start(options);
	opt_map::const_iterator it;
	if ((it = options.find("log_device")) == options.end()) {
		throw missed_param();
	}
	m_pdevice = (log_device *)whereis(it->second);
	if (m_pdevice == NULL) {
		throw not_registered();
	}
	m_block_size = m_pdevice->block_size() - 4;
	m_head_lsn = m_pdevice->get_head();
	m_checkpoint_lsn = m_pdevice->get_checkpoint();
	m_tail_lsn = m_pdevice->get_tail();
	find_tail();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void blogger::start(const std::string & name, const std::string & log_device) {
	printf("logger: name %s, log_device %s\n", name.c_str(), log_device.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["log_device"] = log_device;
	blogger * plogger = new blogger;
	plogger->start(opts);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void blogger::find_tail() {
	// first find tail lsn
	try {
		serial_buffer buffer;
		while (read_forward_internal(buffer, m_tail_lsn, m_tail_lsn) == ok) {
			// nothing to do
		}
	} catch (std::exception &) {
	}
	m_tail_block = m_tail_lsn / m_block_size;
	m_tail_offset = m_tail_lsn % m_block_size;
	if (m_tail_offset > 0) {
		m_tail_pb = m_pdevice->read(m_tail_block);
	} else {
		m_tail_pb = m_pdevice->alloc();
	}
	m_tail_valid = true;
	m_pdevice->set_tail(m_tail_lsn, m_tail_offset == 0 && m_tail_block > 0 ? m_tail_block - 1 : m_tail_block);
	sync(m_tail_lsn);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
result blogger::read_exact(serial_buffer & buffer, uint32_t size, uint64_t & lsn) {
	uint64_t block_n = lsn / m_block_size;
	uint32_t offset = lsn % m_block_size;
	block_ptr pb;
	while (size != 0) {
		if (offset == m_block_size) {
			// end of block reached - update lsn
			block_n++;
			offset = 0;
		}
		// read block
		uint32_t stored_in_block;
		if (m_tail_valid && block_n == m_tail_block) {
			pb = m_tail_pb;
			stored_in_block = m_tail_offset;
		} else {
			pb = m_pdevice->read(block_n);
			const char * b = pb.prepare_read();
			stored_in_block = *((uint32_t *)(b + m_block_size));
			assert(!m_tail_valid || stored_in_block == m_block_size);
		}
		// calculate number of bytes that can be copied from current block.
		if (offset >= stored_in_block) {
			assert(!m_tail_valid);
			return my_eof;
		}
		uint32_t copy_size = stored_in_block - offset;
		if (copy_size > size) {
			copy_size = size;
		}
		const char * b = pb.prepare_read();
		buffer.put(b + offset, copy_size);
		size -= copy_size;
		offset += copy_size;
		lsn += copy_size;
	}
	return ok;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
result blogger::read_forward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) {
	assert(lsn >= m_head_lsn);
	if (lsn >= m_tail_lsn) {
		return my_eof;
	}
	return read_forward_internal(buffer, lsn, next_lsn);
}

//--------------------------------------------------------------------
// private function
//--------------------------------------------------------------------
result blogger::read_forward_internal(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) {
	uint32_t size;
	buffer.rewind();
	uint64_t temp = lsn;
	result res = read_exact(buffer, 4, temp);
	if (res != ok) {
		return res;
	}
	buffer.get(size);
	buffer.rewind();
	if ((res = read_exact(buffer, size, temp)) != ok) {
		return res;
	}
	// not necessary to read next 4 bytes, just increment lsn. see below comments.
	if (m_tail_valid) {
		next_lsn = temp + 4;
		assert(next_lsn <= m_tail_lsn);
		assert(next_lsn > m_head_lsn);
		return ok;
	}
	// called from find_tail - make sure that we can read last 4 bytes too
	serial_buffer temp_buffer;
	res = read_exact(temp_buffer, 4, temp);
	if (res != ok) {
		return res;
	}
	next_lsn = temp;
	return ok;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
result blogger::read_backward(serial_buffer & buffer, uint64_t lsn, uint64_t & next_lsn) {
	assert(lsn <= m_tail_lsn);
	if (lsn <= m_head_lsn) {
		return my_eof;
	}
	uint64_t size_lsn = lsn - 4;
	uint32_t size;
	buffer.rewind();
	result res = read_exact(buffer, 4, size_lsn);
	assert(res == ok);
	buffer.get(size);
	buffer.rewind();
	uint64_t start_lsn = lsn - size - 4;
	res = read_exact(buffer, size, start_lsn);
	assert(res == ok);
	next_lsn = lsn - size - 8;
	assert(next_lsn < m_tail_lsn);
	assert(next_lsn >= m_head_lsn);
	return ok;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t blogger::write(const void * pdata, uint32_t size, uint64_t & redo_lsn) {
	redo_lsn = m_tail_lsn;
	write_exact(&size, 4);
	write_exact(pdata, size);
	write_exact(&size, 4);
	return m_tail_lsn;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void blogger::write_exact(const void * pdata, uint32_t size) {
	const char * p = (char *)pdata;
	while (size != 0) {
		if (m_tail_offset == m_block_size) {
			// tail block is full - write it and update lsn
			// add block's metadata
			char * b = m_tail_pb.prepare_write();
			*((uint32_t *)(b + m_block_size)) = m_tail_offset;
			// write tail block
			m_pdevice->write(m_tail_block, m_tail_pb);
			m_tail_block++;
			m_tail_offset = 0;
			// alloc new block.
			m_tail_pb = m_pdevice->alloc();
		}
		// calculate number of bytes that can be copied to tail.
		uint32_t copy_size = m_block_size - m_tail_offset;
		if (copy_size > size) {
			copy_size = size;
		}
		char * b = m_tail_pb.prepare_write();
		memcpy(b + m_tail_offset, p, copy_size);
		size -= copy_size;
		p += copy_size;
		m_tail_offset += copy_size;
		m_tail_lsn += copy_size;
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void blogger::sync(uint64_t lsn) {
	if (lsn <= m_last_sync) {
		// LSN already synced
		return;
	}
	// write tail block and then sync.
	// add block's metadata
	if (m_tail_offset > 0) {
		char * b = m_tail_pb.prepare_write();
		// store number of free bytes in block;
		*((uint32_t *)(b + m_block_size)) = m_tail_offset;
		m_pdevice->write(m_tail_block, m_tail_pb);
	}
	m_pdevice->sync(m_tail_lsn);
	m_last_sync = m_tail_lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void blogger::set_head(uint64_t head_lsn, uint64_t checkpoint_lsn) {
	assert(head_lsn >= m_head_lsn && head_lsn <= m_tail_lsn);
	assert(checkpoint_lsn >= head_lsn && checkpoint_lsn <= m_tail_lsn);
	uint32_t head_offset = head_lsn % m_block_size;
	uint64_t head_block = head_lsn / m_block_size;
	m_pdevice->set_head(head_lsn,
											head_offset == 0 && head_block > 0 ? head_block - 1 : head_block,
											checkpoint_lsn);
	m_head_lsn = head_lsn;
	m_checkpoint_lsn = checkpoint_lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t blogger::get_head() {
	return m_head_lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t blogger::get_checkpoint() {
	return m_checkpoint_lsn;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t blogger::get_tail() {
	return m_tail_lsn;
}


