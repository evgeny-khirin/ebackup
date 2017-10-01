///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : drv_terms.hpp
/// Author  : Evgeny Khirin <>
/// Description : Minimal set of Erlang terms necessary for driver's
/// communications.
///-------------------------------------------------------------------
#ifndef __drv_terms_hpp__
#define __drv_terms_hpp__

#ifdef _WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#endif
#include <string>
#include <list>

#include "utils.hpp"

//--------------------------------------------------------------------
// erlang term
//--------------------------------------------------------------------
class erl_term {
protected:
	erl_term() {}

public:
	virtual ~erl_term() {}
	virtual void to_binary(serial_buffer & buf) const = 0;
};

//--------------------------------------------------------------------
// atom term
//--------------------------------------------------------------------
class erl_atom: public erl_term {
	std::string		m_atom;

public:
	erl_atom(const char * v) {
		m_atom.assign(v);
	}

	virtual void to_binary(serial_buffer & buf) const {
		buf.put((uint8_t)100);
		buf.put(htons(m_atom.size()));
		buf.put(m_atom.data(), m_atom.size());
	}
};

//--------------------------------------------------------------------
// integer term
//--------------------------------------------------------------------
class erl_integer: public erl_term {
	uint32_t		m_value;

public:
	erl_integer(uint32_t v) {
		m_value = v;
	}

	virtual void to_binary(serial_buffer & buf) const {
		buf.put((uint8_t)98);
		buf.put(htonl(m_value));
	}
};

//--------------------------------------------------------------------
// integer term
//--------------------------------------------------------------------
class erl_uint64: public erl_term {
	uint64_t		m_value;

public:
	erl_uint64(uint64_t v) {
		m_value = v;
	}

	virtual void to_binary(serial_buffer & buf) const {
		buf.put((uint8_t)110);
		buf.put((uint8_t)8);
		buf.put((uint8_t)0);
		buf.put(m_value);
	}
};

//--------------------------------------------------------------------
// tuple term
//--------------------------------------------------------------------
class erl_tuple: public erl_term {
private:
	typedef std::list<const erl_term *>		list_t;

private:
	list_t		m_list;
	uint32_t	m_arity;

public:
	erl_tuple() {
		m_arity = 0;
	}

	void push_back(const erl_term & t) {
		m_list.push_back(&t);
		m_arity++;
	}

	virtual void to_binary(serial_buffer & buf) const {
		if (m_arity <= UINT8_MAX) {
			buf.put((uint8_t)104);
			buf.put((uint8_t)m_arity);
		} else {
			buf.put((uint8_t)105);
			buf.put(htonl(m_arity));
		}
		for (list_t::const_iterator i = m_list.begin(); i != m_list.end(); i++) {
			const erl_term * t = *i;
			t->to_binary(buf);
		}
	}
};

//--------------------------------------------------------------------
// binary term
//--------------------------------------------------------------------
class erl_binary: public erl_term {
	const void *	m_data;
	uint32_t			m_size;

public:
	erl_binary(const void *	data, uint32_t size) {
		m_data = data;
		m_size = size;
	}

	virtual void to_binary(serial_buffer & buf) const {
		buf.put((uint8_t)109);
		buf.put(htonl(m_size));
		buf.put(m_data, m_size);
	}
};

//--------------------------------------------------------------------
// Erlang reply
//--------------------------------------------------------------------
class erl_reply {
public:
	static void serialize(serial_buffer & buf, const erl_term & t) {
		buf.put((uint8_t)131);
		t.to_binary(buf);
	}
};

#endif // __drv_terms_hpp__

