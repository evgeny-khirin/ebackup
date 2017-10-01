///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : pointers.hpp
/// Author  : Evgeny Khirin <>
/// Description : Miscellaneous smart pointers.
///-------------------------------------------------------------------
#ifndef __pointers_hpp__
#define __pointers_hpp__

#include <string.h>

#include "heap.hpp"

//--------------------------------------------------------------------
// sized pointer for malloc_ex and free_ex functions.
//--------------------------------------------------------------------
class sized_ptr {
private:
	void *											m_ptr;
	uint32_t										m_size;

public:
	sized_ptr() {
		m_ptr = NULL;
		m_size = 0;
	}

	sized_ptr(uint32_t size) {
		alloc(size);
	}

	void * alloc(uint32_t size) {
		m_ptr = malloc_ex(size);
		m_size = size;
		return m_ptr;
	}

	void * realloc(uint32_t new_size) {
		if (new_size <= m_size) {
			return m_ptr;
		}
		void * ptr = malloc_ex(new_size);
		if (m_ptr != NULL) {
			memcpy(ptr, m_ptr, m_size);
			free_ex(m_ptr, m_size);
		}
		m_ptr = ptr;
		m_size = new_size;
		return m_ptr;
	}

	void free() {
		if (m_ptr != NULL) {
			free_ex(m_ptr, m_size);
		}
	}

	void * data() const {
		return m_ptr;
	}

	uint32_t size() const {
		return m_size;
	}
};

//--------------------------------------------------------------------
// non-intrusive smart pointer based on double linked list
//--------------------------------------------------------------------
template <class T>
class linked_ptr {
private:
	T *														m_data;
	mutable const linked_ptr *		m_prev;
	mutable const linked_ptr *		m_next;

private:
	void acquire(const linked_ptr & r)  {
		// insert this to the list
		m_data = r.m_data;
		m_next = r.m_next;
		m_next->m_prev = this;
		m_prev = &r;
		r.m_next = this;
	}

	void release() {
		if (unique()) {
			// delete non-shared pointer
			delete m_data;
			return;
		}
		// remove this object from list
		m_prev->m_next = m_next;
		m_next->m_prev = m_prev;
	}

public:
	linked_ptr() {
		m_data = NULL;
		m_prev = m_next = this;
	}

	linked_ptr(T * data) {
		m_data = data;
		m_prev = m_next = this;
	}

	linked_ptr(const linked_ptr & r) {
		acquire(r);
	}

	~linked_ptr() {
		release();
	}

	bool unique() const {
		return m_data == NULL || m_prev == this;
	}

	T * get() const {
		return m_data;
	}

	linked_ptr & operator=(const linked_ptr & other) {
		if (&other == this) {
			return *this;
		}
		release();
		acquire(other);
		return *this;
	}

	T & operator*() const {
		return *m_data;
	}

	T * operator->() const {
		return m_data;
	}
};

//--------------------------------------------------------------------
// intrusive smart pointer based on reference count. class T must be
// inhereted from smart_ptr::data or has accessible m_ref_count field.
//--------------------------------------------------------------------
template <class T>
class smart_ptr {
public:
	class data {
		friend class smart_ptr;
	private:
		uint32_t	m_ref_count;

	public:
		data() {
			m_ref_count = 1;
		}

		virtual ~data() {}
	};

private:
	T *		m_data;

private:
	void acquire(const smart_ptr & r)  {
		m_data = r.m_data;
		if (m_data != NULL) {
			m_data->m_ref_count++;
		}
	}

	void release() {
		if (unique()) {
			// delete non-shared pointer
			delete m_data;
			return;
		}
		m_data->m_ref_count--;
	}

public:
	smart_ptr() {
		m_data = NULL;
	}

	smart_ptr(T * data) {
		m_data = data;
	}

	smart_ptr(const smart_ptr & r) {
		acquire(r);
	}

	~smart_ptr() {
		release();
	}

	bool unique() const {
		return m_data == NULL || m_data->m_ref_count == 1;
	}

	T * get() const {
		return m_data;
	}

	smart_ptr & operator=(const smart_ptr & other) {
		if (&other == this) {
			return *this;
		}
		release();
		acquire(other);
		return *this;
	}

	T & operator*() const {
		return *m_data;
	}

	T * operator->() const {
		return m_data;
	}
};

//--------------------------------------------------------------------
// "copy-on-write" pointer
//--------------------------------------------------------------------
template <class T, class ref_ptr = smart_ptr<T>, class P = char>
class cow_ptr
{
private:
	ref_ptr m_sp;

	//--------------------------------------------------------------------
	// Function: detach().
	// Description: Creates copy object if necessary.
	//--------------------------------------------------------------------
	void detach()
	{
		if (!m_sp.unique()) {
			T* tmp = m_sp.get();
			T* copy_ptr = new T;
			copy_ptr->copy(*tmp);
			m_sp = ref_ptr(copy_ptr);
		}
	}

public:
	//--------------------------------------------------------------------
	// Function: cow_ptr().
	// Description: Constructor.
	//--------------------------------------------------------------------
	cow_ptr() {}

	//--------------------------------------------------------------------
	// Function: cow_ptr(T* t).
	// Description: Constructor.
	//--------------------------------------------------------------------
	cow_ptr(T* t): m_sp(t) {}

	//--------------------------------------------------------------------
	// Function: cow_ptr(const ref_ptr& refptr).
	// Description: Constructor.
	//--------------------------------------------------------------------
	cow_ptr(const ref_ptr& refptr): m_sp(refptr) {}

	//--------------------------------------------------------------------
	// Function: cow_ptr(const cow_ptr& cowptr).
	// Description: Copy constructor.
	//--------------------------------------------------------------------
	cow_ptr(const cow_ptr& cowptr): m_sp(cowptr.m_sp) {}

	//--------------------------------------------------------------------
	// Function: operator=(const cow_ptr& rhs) -> cow_ptr&.
	// Description: Assigment operator.
	//--------------------------------------------------------------------
	cow_ptr& operator=(const cow_ptr& r) {
		m_sp = r.m_sp; // no need to check for self-assignment with smart pointer
		return *this;
	}

	//--------------------------------------------------------------------
	// Function: prepare_read() -> const P *.
	// Description: Dereferences pointer for read only operation.
	//--------------------------------------------------------------------
	const P * prepare_read() const {
		return m_sp->prepare_read();
	}

	//--------------------------------------------------------------------
	// Function: prepare_write() -> P *.
	// Description: Dereferences pointer for write operation. Creates copy of
	// object if it has more than one reference.
	//--------------------------------------------------------------------
	P * prepare_write() {
		detach();
		return m_sp->prepare_write();
	}
};

#endif // __pointers_hpp__

