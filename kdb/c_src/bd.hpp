///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bd.hpp
/// Author  : Evgeny Khirin <>
/// Description : Provides abstraction of general block device.
///-------------------------------------------------------------------
#ifndef __bd_hpp__
#define __bd_hpp__

#include <stdint.h>
#include <assert.h>
#include <list>

#include "no.hpp"
#include "pointers.hpp"

//--------------------------------------------------------------------
// class block of block device.
//--------------------------------------------------------------------
class block {
	friend class cow_ptr<block, linked_ptr<block> >;
	friend class bd_file;
	friend class bd_null;
	friend class bd_pool;
	friend class bd_raid0;
	friend class bd_factor;

public:
	struct stats_t: public no_stats {
		uint64_t		m_allocs;
		uint64_t		m_frees;
		uint64_t		m_copies;

		stats_t() {
			m_allocs = m_frees = m_copies = 0;
			m_reg_name = "block";
		}

		// see base class;
		virtual void soft_reset();

		// see base class
		virtual std::string to_string();
	};

private:
	sized_ptr				m_data;
	uint32_t				m_block_size;
	static stats_t	m_stats;

	block() {
		m_block_size = 0;
	}

	block(uint32_t block_size) {
		m_stats.m_allocs++;
		m_block_size = block_size;
		m_data.alloc(block_size);
	}

	// called from cow_ptr
	void copy(const block & other) {
		m_stats.m_copies++;
		m_block_size = other.m_block_size;
		m_data.alloc(m_block_size);
		memcpy(m_data.data(), other.m_data.data(), m_block_size);
	}

	// called from cow_ptr
	const char * prepare_read() const {
		return (const char *)m_data.data();
	}

	// called from cow_ptr
	char * prepare_write() {
		return (char *)m_data.data();
	}

public:
	~block() {
		if (m_data.data() != NULL) {
			m_stats.m_frees++;
			m_data.free();
		}
	}

	static void stats(no_stats_list & acc) {
		acc.push_back(&m_stats);
	}
};

//--------------------------------------------------------------------
// block pointer with copy-on-write semantic
//--------------------------------------------------------------------
typedef cow_ptr<block, linked_ptr<block> > block_ptr;

//--------------------------------------------------------------------
// list of blocks
//--------------------------------------------------------------------
typedef std::list<std::pair<uint64_t, block_ptr *> > block_list;

//--------------------------------------------------------------------
// Block device interface.
//--------------------------------------------------------------------
class block_device: public named_object {
	friend class block;

public:
	//--------------------------------------------------------------------
	// Function: write(uint64_t n, block_ptr & pb).
	// Description: Writes block to device. Block numbers start from 0.
	//--------------------------------------------------------------------
	virtual void write(uint64_t n, block_ptr & pb) = 0;

	//--------------------------------------------------------------------
	// Function: write(uint64_t n, uint32_t block_size, block_ptr & pb).
	// Description: Writes block of custom size to device. Block numbers start from 0.
	// Custom block size must be multiple of original.
	//--------------------------------------------------------------------
	virtual void write(uint64_t n, uint32_t block_size, block_ptr & pb);

	//--------------------------------------------------------------------
	// Function: write(block_list & blocks).
	// Description: Writes list of blocks to device. Returns ok, if all blocks
	// written to device successfully. List may modified by device, for example
	// sorted.
	//--------------------------------------------------------------------
	virtual void write(block_list & blocks) = 0;

	//--------------------------------------------------------------------
	// Function: write(uint32_t block_size, block_list & blocks).
	// Description: Writes list of blocks with custom size to device. Returns ok,
	// if all blocks written to device successfully. List may modified by device,
	// for example sorted. Custom block size must be multiple of original.
	//--------------------------------------------------------------------
	virtual void write(uint32_t block_size, block_list & blocks);

	//--------------------------------------------------------------------
	// Function: block_ptr read(uint64_t n).
	// Description: Reads block from device. Block numbers start from 0.
	//--------------------------------------------------------------------
	virtual block_ptr read(uint64_t n) = 0;

	//--------------------------------------------------------------------
	// Function: block_ptr read(uint64_t n, uint32_t block_size).
	// Description: Reads block of custom size from device. Block numbers start from 0.
	// Custom block size must be multiple of original.
	//--------------------------------------------------------------------
	virtual block_ptr read(uint64_t n, uint32_t block_size);

	//--------------------------------------------------------------------
	// Function: sync().
	// Description: Flushes the device. All pending data are guaranteed to be written
	// on physical device at function completion.
	//--------------------------------------------------------------------
	virtual void sync() = 0;

	//--------------------------------------------------------------------
	// Function: uint32_t block_size().
	// Description: Returns block size of device.
	//--------------------------------------------------------------------
	virtual uint32_t block_size() = 0;

	//--------------------------------------------------------------------
	// Function: uint64_t capacity().
	// Description: Returns capacity of device.
	//--------------------------------------------------------------------
	virtual uint64_t capacity() = 0;

	//--------------------------------------------------------------------
	// Function: block_ptr alloc().
	// Description: Allocates block from device.
	//--------------------------------------------------------------------
	virtual block_ptr alloc() = 0;
};

//--------------------------------------------------------------------
// Block device common statistics
//--------------------------------------------------------------------
struct bd_stats: public no_stats {
	uint64_t		m_writes;		// number of written blocks
	uint64_t		m_reads;		// number of read blocks

	bd_stats() {m_writes = m_reads = 0;}

	// see base class.
	virtual void soft_reset();

	// see base class.
	virtual std::string to_string();
};

//--------------------------------------------------------------------
// Intermediate block device. Passes all calls to underlaying device.
//--------------------------------------------------------------------
class intermediate_block_device: public block_device {
protected:
	block_device * m_pdevice;			// pointer to underlaying device.

public:
	//--------------------------------------------------------------------
	// Function: intermediate_block_device().
	// Description: Constructor.
	//--------------------------------------------------------------------
	intermediate_block_device() {m_pdevice = NULL;}

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual void stats(no_stats_list & list);

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual void write(block_list & blocks);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual void sync();

	// see base class.
	virtual uint32_t block_size();

	// see base class.
	virtual uint64_t capacity();

	// see base class.
	virtual block_ptr alloc();
};

#endif /* __bd_hpp__ */

