///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : utils.hpp
/// Author  : Evgeny Khirin <>
/// Description : Unsorted utilities.
///-------------------------------------------------------------------
#ifndef __utils_hpp__
#define __utils_hpp__

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <string>
#include <list>
#include <sstream>

#include "status.hpp"
#include "pointers.hpp"

//--------------------------------------------------------------------
// Macro for calculation field size.
//--------------------------------------------------------------------
#define FIELD_SIZE(struct, name)	sizeof(((struct *)0)->name)

//--------------------------------------------------------------------
// Function: void parse_atoms_list(std::string& str, std::list<std::string>& res).
// Description: Parses string, representing list of erlang atoms.
// Atoms with coma, like 'one,two,three', are not supported.
// Parameters:
//    str = [atom()]
//--------------------------------------------------------------------
void parse_atoms_list(const std::string& str, std::list<std::string>& res);

//--------------------------------------------------------------------
// Function: to_string(X) -> std::string
// Description: Converts integral types to string.
//--------------------------------------------------------------------
template <class T>
std::string to_string(const T & t) {
	std::stringstream ss;
	ss << t;
	return ss.str();
}

inline std::string to_string(bool t) {
	if (t) {
		return "true";
	}
	return "false";
}

//--------------------------------------------------------------------
// Function: msleep(unsigned miliseconds).
// Description: Sleeps required number of miliseconds
//--------------------------------------------------------------------
void msleep(unsigned miliseconds);

//--------------------------------------------------------------------
// Temporary buffer. Automatically frees its memory.
//--------------------------------------------------------------------
struct temp_buffer {
	char * 	m_pdata;
	uint32_t	m_size;
private:
	uint32_t	m_allocated;

public:
	temp_buffer() {
		m_pdata = NULL;
		m_size = m_allocated = 0;
	}

	~temp_buffer() {
		if (m_pdata != NULL) {
			free(m_pdata);
		}
	}

	void alloc(uint32_t size) {
		if (m_pdata == NULL) {
			m_pdata = (char *)malloc(size);
			m_size = size;
			m_allocated = size;
			return;
		}
		if (size <= m_allocated) {
			m_size = size;
			return;
		}
		m_pdata = (char *)realloc(m_pdata, size);
		m_size = size;
		m_allocated = size;
	}
};

//--------------------------------------------------------------------
// Serial buffer. Automatically frees its memory.
//--------------------------------------------------------------------
struct serial_buffer {
private:
	sized_ptr	m_ptr;
	char * 		m_data;
	uint32_t	m_put_offset;
	uint32_t	m_get_offset;
	uint32_t	m_allocated;
	bool			m_read_only;
	bool			m_own_buffer;

	void alloc(uint32_t size) {
		if (size <= m_allocated) {
			return;
		}
		if (!m_own_buffer) {
			throw not_own_buffer();
		}
		if (size < 1024) {
			size = 1024;
		}
		m_data = (char *)m_ptr.realloc(size);
		m_allocated = m_ptr.size();
	}

public:
	serial_buffer() {
		m_data = NULL;
		m_put_offset = m_get_offset = m_allocated = 0;
		m_read_only = false;
		m_own_buffer = true;
	}

	serial_buffer(const void * data, uint32_t size, bool read_only) {
		m_data = (char *)data;
		m_allocated = size;
		m_own_buffer = false;
		m_read_only = read_only;
		m_get_offset = 0;
		if (read_only) {
			m_put_offset = size;
		} else {
			m_put_offset = 0;
		}
	}

	~serial_buffer() {
		m_ptr.free();
	}

	void rewind() {
		if (m_read_only) {
			m_get_offset = 0;
		} else {
			m_put_offset = m_get_offset = 0;
		}
	}

	void put(const void * data, uint32_t size) {
		assert(!m_read_only);
		alloc(m_put_offset + size);
		memcpy(m_data + m_put_offset, data, size);
		m_put_offset += size;
	}

	template<class T>
	void put(const T & v) {
		assert(!m_read_only);
		alloc(m_put_offset + sizeof(T));
		*((T *)(m_data + m_put_offset)) = v;
		m_put_offset += sizeof(T);
	}

	const void * get_ptr(uint32_t size) {
		assert(m_put_offset - m_get_offset >= size);
		const void * data = m_data + m_get_offset;
		m_get_offset += size;
		return data;
	}

	template<class T>
	void get(T & v) {
		assert(m_put_offset - m_get_offset >= sizeof(T));
		v = *((T *)(m_data + m_get_offset));
		m_get_offset += sizeof(T);
	}

	const char * data() const {
		return m_data;
	}

	uint32_t size() const {
		return m_put_offset;
	}

	const char * curr_get_data() const {
		return m_data + m_get_offset;
	}

	uint32_t curr_get_offset() const {
		return m_get_offset;
	}

	bool eob() const {
		// returns true, if end of buffer is reached
		return (m_get_offset == m_put_offset);
	}
};

#endif // __utils_hpp__

