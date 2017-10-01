///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : lru_cache.hpp
/// Author  : Evgeny Khirin <>
/// Description : Implements LRU (least recently used) cache.
///-------------------------------------------------------------------
#ifndef __lru_cache_hpp__
#define __lru_cache_hpp__

#include <stdint.h>

#include <list>
#ifdef _WIN32
#include <map>
#else
#include <tr1/unordered_map>
#endif

#include "status.hpp"

//--------------------------------------------------------------------
// LRU cache stats.
//--------------------------------------------------------------------
struct lru_stats {
	uint64_t m_capacity;
	uint64_t m_lookups;		// number of lookups
	uint64_t m_hits;			// number of hits
	uint64_t m_dirty;			// number of dirty items
	uint64_t m_inserts;		// total number of inserts into cache
	uint64_t m_updates;		// how many times old items are updated

	void hard_reset() {
		m_capacity = m_lookups = m_hits = m_dirty = m_inserts = m_updates = 0;
	}

	void soft_reset() {
		m_lookups = m_hits = m_inserts = m_updates = 0;
	}
};

//--------------------------------------------------------------------
// Implements cache with least recently used algorithm.
//--------------------------------------------------------------------
template <class K, class V>
class lru_cache {
public:
	typedef void (*save_dirty_fun)(void *, K &, V &);

private:
	// cache entry
	struct entry {
		K 				m_key;
		V 				m_value;
		bool 			m_is_dirty;
		bool			m_is_locked;

	private:
		entry();

	public:
		entry(const K & k, const V & v, bool is_dirty) {
			m_key = k;
			m_value = v;
			m_is_dirty = is_dirty;
			m_is_locked = false;
		}
	};

	typedef std::list<entry> lru_list;	// when entry is accessed, it is
																			// moved to end of list. So candidates
																			// on remove are in begining of list.
	typedef typename lru_list::iterator list_iter;
#ifdef _WIN32
	typedef std::map<K, list_iter> lru_map;	// map for lookup by key
#else
	typedef std::tr1::unordered_map<K, list_iter> lru_map;	// map for lookup by key
#endif
	typedef typename lru_map::iterator map_iter;

private:
	uint32_t 				m_capacity;			// cache capacity
	save_dirty_fun	m_save_dirty;		// pointer on save_dirty function
	void * 					m_param;				// parameter of save_dirty_fun
	lru_list 				m_list;					// LRU list
	lru_map 				m_map;					// list index
	lru_stats 			m_stats;				// cache statistics

private:
	//--------------------------------------------------------------------
	// Function: insert_new(const K & k, const V & v, bool is_dirty).
	// Description: Inserts new key-value pair into cache. If cache is full,
	// than oldest key-value pair flushed from cache.
	// LRU property is updated by this operation.
	//--------------------------------------------------------------------
	void insert_new(const K & k, const V & v, bool is_dirty) {
		// Don't use m_list.size(), becase it may be O(N) time.
		if (m_map.size() == m_capacity) {
			// remove list head - oldest element, if it is not locked
			for (list_iter i = m_list.begin(); i != m_list.end(); i++) {
				entry & h = *i;
				if (h.m_is_locked) {
					// entry is locked, continue scan
					continue;
				}
				// entry can be removed from cache
				// update stats and save dirty
				if (h.m_is_dirty) {
					m_save_dirty(m_param, h.m_key, h.m_value);
					m_stats.m_dirty--;
				}
				m_map.erase(h.m_key);
				m_list.erase(i);
				break;
			}
			if (m_map.size() == m_capacity) {
				// whole cache is locked, can not insert - just save dirty
				if (is_dirty) {
					m_save_dirty(m_param, (K &)k, (V &)v);
				}
				return;
			}
		}
		// update stats
		if (is_dirty) {
			m_stats.m_dirty++;
		}
		// create new entry
		entry new_entry(k, v, is_dirty);
		// put new entry into end of list
		list_iter new_list_it = m_list.insert(m_list.end(), new_entry);
		// add to map
		m_map[k] = new_list_it;
	}

public:
	//--------------------------------------------------------------------
	// Function: lru_cache().
	// Description: Costructor. Cache is not ready to use after constructor.
	// Use start function to initialize the cache.
	//--------------------------------------------------------------------
	lru_cache() {
		m_capacity = 0;
		m_save_dirty = NULL;
		m_param = NULL;
		m_stats.hard_reset();
	}

	//--------------------------------------------------------------------
	// Function: start(int capacity, save_dirty_fun save_dirty, void* save_dirty_param).
	// Description: Initializes cache.
	//--------------------------------------------------------------------
	void start(uint32_t capacity, save_dirty_fun save_dirty, void * save_dirty_param) {
		if (capacity <= 0) {
			throw inv_param();
		}
		m_capacity = capacity;
		m_save_dirty = save_dirty;
		m_param = save_dirty_param;
		m_stats.m_capacity = capacity;
	}

	//--------------------------------------------------------------------
	// Function: stop().
	// Description: Stops cache.
	//--------------------------------------------------------------------
	void stop() {
		sync();
	}

	//--------------------------------------------------------------------
	// Function: lookup(const K & k, V & v) -> ok | not_found
	// Description: Searches object by key.
	// LRU property is updated by this operation.
	//--------------------------------------------------------------------
	result lookup(const K & k, V & v) {
		m_stats.m_lookups++;
		map_iter it = m_map.find(k);
		if (it == m_map.end()) {
			return not_found;
		}
		m_stats.m_hits++;
		// save LRU list iterator and result
		list_iter old_list_it = it->second;
		entry & e = *old_list_it;
		v = e.m_value;
		// move found entry to end of list
		list_iter new_list_it = m_list.insert(m_list.end(), e);
		// update map
		it->second = new_list_it;
		// delete old entry in list
		m_list.erase(old_list_it);
		return ok;
	}

	//--------------------------------------------------------------------
	// Function: insert(const K & k, const V & v, bool is_dirty).
	// Description: Inserts key-value pair into cache. If key already presented
	// in cache, than its value is updated.
	// LRU property is updated by this operation.
	//--------------------------------------------------------------------
	void insert(const K & k, const V & v, bool is_dirty) {
		// update stats
		m_stats.m_inserts++;
		// lookup item
		map_iter it = m_map.find(k);
		if (it == m_map.end()) {
			insert_new(k, v, is_dirty);
			return;
		}
		// save LRU list iterator
		list_iter old_list_it = it->second;
		entry& old_entry = *old_list_it;
		// update stats
		if (!old_entry.m_is_dirty && is_dirty) {
			m_stats.m_dirty++;
		}
		m_stats.m_updates++;
		// create new entry
		entry new_entry(k, v, is_dirty);
		// move new entry to end of list
		list_iter new_list_it = m_list.insert(m_list.end(), new_entry);
		// update map
		it->second = new_list_it;
		// delete old entry in list
		m_list.erase(old_list_it);
	}

	//--------------------------------------------------------------------
	// Function: void lock(const K & k)
	// Description: Locks object in cache. Locked object can not be flushed
	// from cache untill next update.
	//--------------------------------------------------------------------
	void lock(const K & k) {
		map_iter map_it = m_map.find(k);
		if (map_it == m_map.end()) {
			return;
		}
		list_iter list_it = map_it->second;
		entry & e = *list_it;
		e.m_is_locked = true;
	}

	//--------------------------------------------------------------------
	// Function: is_locked(const K & k) -> bool
	// Description: Returns true, if object is locked.
	//--------------------------------------------------------------------
	bool is_locked(const K & k) {
		map_iter map_it = m_map.find(k);
		if (map_it == m_map.end()) {
			return false;
		}
		list_iter list_it = map_it->second;
		entry & e = *list_it;
		return e.m_is_locked;
	}

	//--------------------------------------------------------------------
	// Function: save(const K & k)
	// Description: Saves dirty object by key. Assumes that object is dirty
	// and presented in cache.
	//--------------------------------------------------------------------
	void save(const K & k) {
		map_iter map_it = m_map.find(k);
		list_iter list_it = map_it->second;
		entry & e = *list_it;
		assert(!e.m_is_locked);
		assert(e.m_is_dirty);
		m_save_dirty(m_param, e.m_key, e.m_value);
		m_stats.m_dirty--;
		e.m_is_dirty = false;
	}

	//--------------------------------------------------------------------
	// Function: sync().
	// Description: Flushes all dirty items. Assumes that there are no
	// locked objects in cache.
	//--------------------------------------------------------------------
	void sync() {
		for (list_iter i = m_list.begin(); i != m_list.end(); i++) {
			entry & e = *i;
			if (e.m_is_dirty) {
				assert(!e.m_is_locked);
				m_save_dirty(m_param, e.m_key, e.m_value);
				e.m_is_dirty = false;
				m_stats.m_dirty--;
			}
		}
	}

	//--------------------------------------------------------------------
	// Function: stats() -> lru_stats *
	// Description: Obtains cache stats.
	//--------------------------------------------------------------------
	lru_stats * stats() {
		return &m_stats;
	}
};

#endif // __lru_cache_hpp__

