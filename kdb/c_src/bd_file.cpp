///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd_file.cpp
/// Author  : Evgeny Khirin <>
/// Description : File block device.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifndef _WIN32
#include <unistd.h>
#else
#include <windows.h>
#include <io.h>
#endif
#include <string.h>
#include <assert.h>

#include "bd_file.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::stats(no_stats_list & list) {
	list.push_back(&m_stats);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
bd_file::~bd_file() {
#ifdef _WIN32
	_close(m_fd);
#else
	close(m_fd);
#endif
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
#ifdef _WIN32
static int open_file(const std::string & name) {
	wchar_t wname[MAX_PATH];
	MultiByteToWideChar(CP_UTF8, 0, name.c_str(), name.length() + 1, wname, MAX_PATH);
	return _wopen(wname,
								O_RDWR | O_CREAT | _O_BINARY,
								_S_IREAD | _S_IWRITE);
}
#else
static int open_file(const std::string & name) {
	return open(name.c_str(),
							O_RDWR | O_CREAT | O_LARGEFILE,
							S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
}
#endif

//--------------------------------------------------------------------
// public function
// Supported options:
//   {block_size, BlockSize} - size of block in bytes. Default is 4K.
//   {capacity, Capacity} - device capacity in blocks.
//      Default is (2^64 div block_size).
//   {file, FileName} - file name.
//--------------------------------------------------------------------
void bd_file::start(const opt_map & options) {
	block_device::start(options);
	m_stats.m_reg_name = get_name();
	opt_map::const_iterator it;
	if ((it = options.find("block_size")) != options.end()) {
		m_block_size = strtoul(it->second.c_str(), NULL, 10);
	} else {
		m_block_size = 4 * 1024;
	}
	if ((it = options.find("capacity")) != options.end()) {
#ifdef _WIN32
		m_capacity = _strtoui64(it->second.c_str(), NULL, 10);
#else
		m_capacity = strtoull(it->second.c_str(), NULL, 10);
#endif
	} else {
		m_capacity = UINT64_MAX / m_block_size;
	}
	if ((it = options.find("file")) == options.end()) {
		throw missed_param();
	}
	m_fd = open_file(it->second);
	if (m_fd == -1) {
		throw open_failed();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::start(const std::string & name, uint32_t block_size,
										uint64_t capacity, const std::string & file) {
#ifdef _WIN32
	printf("bd_file: name %s, file %s, block_size %u, capacity %I64u, size %f GB\n",
				 name.c_str(), file.c_str(), block_size, capacity,
				 (double)capacity * block_size / 1024 / 1024 / 1024);
#else
	printf("bd_file: name %s, file %s, block_size %u, capacity %llu, size %f GB\n",
				 name.c_str(), file.c_str(), block_size, capacity,
				 (double)capacity * block_size / 1024 / 1024 / 1024);
#endif
	opt_map	opts;
	opts["name"] = name;
	opts["block_size"] = to_string(block_size);
	opts["capacity"] = to_string(capacity);
	opts["file"] = file;
	bd_file * pdevice = new bd_file;
	pdevice->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::stop() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::del_underlaying() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::write(uint64_t n, block_ptr & pb) {
	assert(n < m_capacity);
	write(n, m_block_size, pb);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
#ifdef _WIN32
typedef int32_t ssize_t;
static ssize_t pwrite64(int fd, const void *buf, size_t count, uint64_t offset) {
  if ((uint64_t)_lseeki64(fd, offset, SEEK_SET) != offset) {
		throw seek_failed();
	}
	return _write(fd, buf, count);
}
#endif

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
#ifdef _WIN32
static ssize_t pread64(int fd, void *buf, size_t count, uint64_t offset) {
  if ((uint64_t)_lseeki64(fd, offset, SEEK_SET) != offset) {
		throw seek_failed();
	}
	return _read(fd, buf, count);
}
#endif

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::write(uint64_t n, uint32_t block_size, block_ptr & pb) {
	assert(block_size % m_block_size == 0);
	assert(n < m_capacity / (block_size / m_block_size));
	m_stats.m_writes++;
	ssize_t count = pwrite64(m_fd, pb.prepare_read(), block_size, block_size * n);
	if ((uint32_t)count != block_size) {
		throw write_failed();
	}
}

//--------------------------------------------------------------------
// operator < for sorting in bd_file::write(block_list& blocks).
// comparison of block pointers missed a sence.
//--------------------------------------------------------------------
inline bool operator<(const block_ptr &, const block_ptr &) {
	return false;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::write(block_list & blocks) {
	write(m_block_size, blocks);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::write(uint32_t block_size, block_list & blocks) {
	assert(block_size % m_block_size == 0);
	blocks.sort();
	serial_buffer buf;
	uint64_t first = UINT64_MAX;
	uint64_t curr = UINT64_MAX;
	// write with blocks merging
	for (block_list::const_iterator i = blocks.begin(); i != blocks.end(); i++) {
		m_stats.m_writes++;
		uint64_t n = i->first;
		block_ptr & pb = *i->second;
		assert(n < m_capacity / (block_size / m_block_size));
		if (buf.size() == 0) {
			// put first block in merging buffer
			first = n;
			curr = n;
			buf.put(pb.prepare_read(), block_size);
		} else if (n == curr + 1) {
			// add block to merging buffer
			curr = n;
			buf.put(pb.prepare_read(), block_size);
		} else {
			// flush merging buffer
			ssize_t count = pwrite64(m_fd, buf.data(), buf.size(), block_size * first);
			if ((uint32_t)count != buf.size()) {
				throw write_failed();
			}
			// start new merge
			buf.rewind();
			first = n;
			curr = n;
			buf.put(pb.prepare_read(), block_size);
		}
	}
	if (buf.size() != 0) {
		// flush merging buffer
		ssize_t count = pwrite64(m_fd, buf.data(), buf.size(), block_size * first);
		if ((uint32_t)count != buf.size()) {
			throw write_failed();
		}
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_file::read(uint64_t n) {
	assert(n < m_capacity);
	return read(n, m_block_size);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_file::read(uint64_t n, uint32_t block_size) {
	assert(block_size % m_block_size == 0);
	assert(n < m_capacity / (block_size / m_block_size));
	m_stats.m_reads++;
	block_ptr pb(new block(block_size));
	ssize_t count = pread64(m_fd, pb.prepare_write(), block_size, block_size * n);
	if ((uint32_t)count != block_size) {
		throw read_failed();
	}
	return pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void bd_file::sync() {
#ifdef _WIN32
	if (_commit(m_fd) != 0) {
		throw sync_failed();
	}
#else
	if (fdatasync(m_fd) != 0) {
		throw sync_failed();
	}
#endif
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint32_t bd_file::block_size() {
	return m_block_size;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
uint64_t bd_file::capacity() {
	return m_capacity;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
block_ptr bd_file::alloc() {
	return block_ptr(new block(m_block_size));
}


