///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : term.hpp
/// Author  : Evgeny Khirin <>
/// Description : Erlang terms wrapper for B-tree.
///-------------------------------------------------------------------
#ifndef __term_hpp__
#define __term_hpp__

#include <map>

#include "erl_interface.h"
#include "utils.hpp"
#include "pointers.hpp"

//--------------------------------------------------------------------
// term pointer
//--------------------------------------------------------------------
class term;
typedef linked_ptr<term>		term_ptr;

//--------------------------------------------------------------------
// term class
//--------------------------------------------------------------------
class term {
public:
	// terms comparator for STL containers
	struct less	{
		//--------------------------------------------------------------------
		// Function: operator()(const term_ptr & x, const term_ptr & y) -> bool
		// Description: Compares two terms.
		//--------------------------------------------------------------------
		bool operator()(const term_ptr & x, const term_ptr & y) const {
			return (erl_compare_ext((unsigned char *)x->data(),
															(unsigned char *)y->data()) == -1);
		}
	};

private:
	sized_ptr		m_data;
	uint32_t		m_size;

	//--------------------------------------------------------------------
	// Function: term().
	// Description: Constructor.
	//--------------------------------------------------------------------
	term();

public:
	//--------------------------------------------------------------------
	// Function: term(const void * data, uint32_t size).
	// Description: Constructor.
	//--------------------------------------------------------------------
	term(const void * data, uint32_t size) {
		m_data.alloc(size);
		m_size = size;
		memcpy(m_data.data(), data, size);
	}

	//--------------------------------------------------------------------
	// Function: ~term().
	// Description: Destructor.
	//--------------------------------------------------------------------
	~term() {
		m_data.free();
	}

	//--------------------------------------------------------------------
	// Function: data() -> const char *
	// Description: Returns term data pointer.
	//--------------------------------------------------------------------
	const char * data() const {
		return (const char *)m_data.data();
	}

	//--------------------------------------------------------------------
	// Function: size() -> uint32_t
	// Description: Returns term size.
	//--------------------------------------------------------------------
	uint32_t size() const {
		return m_size;
	}

	//--------------------------------------------------------------------
	// Function: serialize(serial_buffer & buf)
	// Description: Puts term into buffer.
	//--------------------------------------------------------------------
	void serialize(serial_buffer & buf) const {
		buf.put(m_size);
		buf.put(m_data.data(), m_size);
	}

	//--------------------------------------------------------------------
	// Function: deserialize(serial_buffer & buf) -> term_ptr
	// Description: Gets term from buffer.
	//--------------------------------------------------------------------
	static term_ptr deserialize(serial_buffer & buf) {
		uint32_t size;
		buf.get(size);
		const void * data = buf.get_ptr(size);
		return term_ptr(new term(data, size));
	}
};

#endif // __term_hpp__

