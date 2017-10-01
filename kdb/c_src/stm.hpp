///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : stm.hpp
/// Author  : Evgeny Khirin <>
/// Description : Storage manager based on intervals list.
///-------------------------------------------------------------------
#ifndef __stm_hpp__
#define __stm_hpp__

#include <stdint.h>
#include <stdio.h>

#include <map>

#include "bm.hpp"

//--------------------------------------------------------------------
// Storage manager.
//--------------------------------------------------------------------
class stm: public resource_mgr {
private:
	typedef std::map<uint64_t, uint64_t> interval_map;	// maps interval start to its length.

	//--------------------------------------------------------------------
	// STM node
	//--------------------------------------------------------------------
	struct node_t: public decoded_block {
		uint64_t			m_next;
		interval_map	m_map;

		node_t() {m_next = UINT64_MAX;}

		node_t(uint64_t next) {m_next = next;}

		virtual void serialize(serial_buffer & buf) {
			buf.put(m_next);
			buf.put((uint32_t)m_map.size());
			for (interval_map::iterator i = m_map.begin(); i != m_map.end(); i++) {
				uint64_t start = i->first;
				uint64_t len = i->second;
				buf.put(start);
				buf.put(len);
			}
		}

		void deserialize(serial_buffer & buf) {
			buf.get(m_next);
			uint32_t size;
			buf.get(size);
			for (uint32_t i = 0; i < size; i++) {
				uint64_t start;
				uint64_t len;
				buf.get(start);
				buf.get(len);
				m_map[start] = len;
			}
		}

		virtual void dump() {
			printf("^^^^^^^^^^^^^^^^^^^^^ STM node dump START ^^^^^^^^^^^^^^^^^^^^^^^^^^\n");
#ifdef _WIN32
			printf("next %I64u, map size %u\n", m_next, m_map.size());
#else
			printf("next %llu, map size %u\n", m_next, m_map.size());
#endif
			printf("intervals:\n");
			for (interval_map::iterator i = m_map.begin(); i != m_map.end(); i++) {
				uint64_t start = i->first;
				uint64_t len = i->second;
#ifdef _WIN32
				printf("\tstart %I64u, length %I64u\n", start, len);
#else
				printf("\tstart %llu, length %llu\n", start, len);
#endif
			}
			printf("^^^^^^^^^^^^^^^^^^^^^ STM node dump END ^^^^^^^^^^^^^^^^^^^^^^^^^^\n");
		}
	};

private:
	uint64_t					m_used;								// number of blocks, used on device
	uint64_t					m_head;								// list head block_number
	uint64_t					m_capacity;						// device capacity, used for automatic
	uint64_t					m_checkpoint_lsn;
	trans_mgr *				m_tm;
	buffer_mgr *			m_bm;
	uint32_t					m_intervals_in_node;	// number of intervals stored in node

	//--------------------------------------------------------------------
	// Function: check_capacity().
	// Description: Checks, if device capacity is changed.
	//--------------------------------------------------------------------
	void check_capacity();

	//--------------------------------------------------------------------
	// Function: node_decoder(uint64_t n, serial_buffer & buf) -> decoded_block_ptr
	// Description: Decodes node from raw buffer.
	//--------------------------------------------------------------------
	static decoded_block_ptr node_decoder(uint64_t n, serial_buffer & buf);

	//--------------------------------------------------------------------
	// Function: alloc_internal() -> uint64_t
	// Description: Same as alloc, but without transaction.
	//--------------------------------------------------------------------
	uint64_t alloc_internal();

	//--------------------------------------------------------------------
	// Function: free_internal(uint64_t n).
	// Description: Same as free, but without transaction.
	//--------------------------------------------------------------------
	void free_internal(uint64_t n);

	//--------------------------------------------------------------------
	// Function: insert(node_t & node, uint64_t node_n, uint64_t n, bool do_log) -> uint64_t
	// Description: Attempts to insert block into node.
	// Returns redo LSN or 0 if block can not be inserted in node.
	//--------------------------------------------------------------------
	uint64_t insert(node_t & node, uint64_t node_n, uint64_t n, bool do_log);

	//--------------------------------------------------------------------
	// Function: new_head(decoded_block_ptr & header_pb, uint64_t n)
	// Description: Creates new head from block n.
	//--------------------------------------------------------------------
	void new_head(uint64_t n);

	//--------------------------------------------------------------------
	// Function: remove(node_t & node, uint64_t n) -> bool
	// Description: Undoes free or redoes alloc operation by removing block from node.
	// Block must be in node's map.
	//--------------------------------------------------------------------
	static void remove(node_t & node, uint64_t n);

public:
	//--------------------------------------------------------------------
	// Function: stm().
	// Description: Constructor.
	//--------------------------------------------------------------------
	stm() {
		m_used = 0;
		m_head = UINT64_MAX;
		m_capacity = 0;
		m_checkpoint_lsn = 0;
		m_tm = NULL;
		m_bm = NULL;
		m_intervals_in_node = 0;
	}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the transaction manager.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {transaction_manager, Name} - transaction manager.
	//   {buffer_manager, Name} - buffer manager.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & trans_mgr,
										const std::string & buffer_mgr);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual void stats(no_stats_list & list);

	// see base class.
	virtual void undo(uint64_t n, const void * data, uint32_t size, void * opaque);

	// see base class.
	virtual void redo(uint64_t n, bool state_only, const void * data, uint32_t size, uint64_t lsn);

	// see base class.
	virtual void checkpoint();

	// see base class.
	virtual void recover_state(const void * data, uint32_t size, uint64_t redo_lsn);

	//--------------------------------------------------------------------
	// Function: alloc() -> uint64_t
	// Description: Allocates block on device.
	//--------------------------------------------------------------------
	uint64_t alloc();

	//--------------------------------------------------------------------
	// Function: free(uint64_t n).
	// Description: Frees block on device.
	//--------------------------------------------------------------------
	void free(uint64_t n);

	//--------------------------------------------------------------------
	// Function: used() -> uint64_t
	// Description: Returns number of allocated blocks on device, including
	// used by STM itself.
	//--------------------------------------------------------------------
	uint64_t used() {return m_used;}

	//--------------------------------------------------------------------
	// Function: is_full() -> bool
	// Description: Returns returns true if there is no free blocks left.
	//--------------------------------------------------------------------
	bool is_full() {
		if (m_used < m_capacity) {
			return false;
		}
		if (m_bm->capacity() > m_capacity) {
			return false;
		}
		return true;
	}
};

#endif // __stm_hpp__

