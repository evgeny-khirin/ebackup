///-------------------------------------------------------------------
/// Copyright (c) 2002-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : heap.cpp
/// Author  : Evgeny Khirin <>
/// Description : Replacement of standard memory mangement routines.
///-------------------------------------------------------------------
#include <stdint.h>
#include <assert.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <sys/mman.h>
#endif

#include "mutex.hpp"

namespace {
	//--------------------------------------------------------------------
	// simple single linked list
	//--------------------------------------------------------------------
	class stack {
	private:
		struct node_t {
			node_t *	m_next;
		};

	private:
		node_t *			m_head;

	public:
		inline bool is_empty() const {
			return m_head == NULL;
		}

		inline void push(void * data) {
			node_t * node = (node_t *)data;
			node->m_next = m_head;
			m_head = node;
		}

		inline void * pop() {
			void * r = m_head;
			m_head = m_head->m_next;
			return r;
		}
	};
}

//--------------------------------------------------------------------
// Function: count_zero_leadings_bits(uint32_t n) -> uint32_t
// Description: Counts number of leading zero bits in number.
// Base algorithm is presented below.
// Implementated algorithm goes in reverse order and uses table for
// performance improvement.
// uint32_t count_zero_leadings_bits(uint32_t n) {
// 	if (n == 0) {
// 		return 32;
// 	}
// 	uint32_t zero_bits = 0;
// 	if (n <= 0x0000FFFF) {zero_bits += 16; n <<= 16;}
// 	if (n <= 0x00FFFFFF) {zero_bits +=  8; n <<=  8;}
// 	if (n <= 0x0FFFFFFF) {zero_bits +=  4; n <<=  4;}
// 	if (n <= 0x3FFFFFFF) {zero_bits +=  2; n <<=  2;}
// 	if (n <= 0x7FFFFFFF) {zero_bits +=  1;}
// 	return zero_bits;
// }
//--------------------------------------------------------------------
static inline uint32_t count_zero_leadings_bits(uint32_t n) {
	uint32_t zero_bits = 32;
	uint32_t temp;
	static const uint32_t helper_table[256] = {
		0,
		1,
		2, 2,
		3, 3, 3, 3,
		4, 4, 4, 4, 4, 4, 4, 4,
		5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
		6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
		8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8
	};
	if ((temp = n >> 16) != 0) {
		zero_bits -= 16;
		n = temp;
	}
	if ((temp = n >> 8) != 0) {
		zero_bits -= 8;
		n = temp;
	}
	return zero_bits - helper_table[n];
}

//--------------------------------------------------------------------
// Function: log2(uint32_t n) -> uint32_t
// Description: Calculates logarithm 2 of N.
//--------------------------------------------------------------------
static inline uint32_t log2(uint32_t n) {
	return 31 - count_zero_leadings_bits(n);
}

//--------------------------------------------------------------------
// Function: pow2_round_up(uint32_t n) -> uint32_t
// Description: Rounds up given number nearst greather power of two.
// Zero returned in case of overflow.
//--------------------------------------------------------------------
static inline uint32_t pow2_round_up(uint32_t n) {
	if (n == 0) {
		return 1;
	}
	n--;
	n |= n >> 1;
	n |= n >> 2;
	n |= n >> 4;
	n |= n >> 8;
	n |= n >> 16;
	return n + 1;
}

//--------------------------------------------------------------------
// free chunks lists
//--------------------------------------------------------------------
static stack 					g_free_chunks[32];
static uint32_t 			g_chunk_sizes[32] = {1, 2, 4, 8, 16, 32, 64, 128,
	256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
	524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864,
	134217728, 268435456, 536870912, 1073741824, 2147483648u};
static uint32_t 			g_mmap_idx = 16;
static rmutex *				g_lock = new rmutex;

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
#ifdef _WIN32
#ifndef	NDEBUG
static int getpagesize() {
	SYSTEM_INFO s;
	GetSystemInfo(&s);
	return s.dwPageSize;
}
#endif
#endif

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
#ifdef _WIN32
// those flags are ignored on windows
#define PROT_READ			0
#define PROT_WRITE		0
#define MAP_PRIVATE		0
#define MAP_ANONYMOUS 0
static void * mmap(void *addr, size_t length, int prot, int flags,
                  int fd, uint64_t offset) {
	// ignore parameters
	(void)prot;
	(void)flags;
	(void)fd;
	(void)offset;
	return VirtualAlloc(addr, length, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
}
#endif

//--------------------------------------------------------------------
// Function: heap_init()
// Description: Initializes memory allocation system.
//--------------------------------------------------------------------
namespace {
	class heap_init {
	public:
		heap_init() {
			assert(log2(getpagesize()) <= g_mmap_idx);
		}
	} g_heap_init;
}

//--------------------------------------------------------------------
// Minimal allocation size
//--------------------------------------------------------------------
#define MIN_ALLOC_SIZE			16
#define MEM_ALLOC_ALIGMENT	8

//--------------------------------------------------------------------
// Function: malloc_ex(uint32_t & size) -> void *
// Description: Allocates memory at least required size. Size is rounded to
// nearst greather power of two.
//--------------------------------------------------------------------
void * malloc_ex(uint32_t & size) {
	// round required size
	if (size <= MIN_ALLOC_SIZE) {
		size = MIN_ALLOC_SIZE;
	} else {
		size = pow2_round_up(size);
	}
	// calculate index suitable for required size
	uint32_t req_idx = log2(size);
	uint32_t curr_idx = req_idx;
	// find first non-empty list
	scoped_lock<rmutex> lock(g_lock);
	while (curr_idx < 32 && g_free_chunks[curr_idx].is_empty()) {
		curr_idx++;
	}
	// all lists are empty
	if (curr_idx == 32) {
		if (req_idx > g_mmap_idx) {
			curr_idx = req_idx;
		} else {
			curr_idx = g_mmap_idx;
		}
		void * p = mmap(NULL, g_chunk_sizes[curr_idx],
										PROT_READ | PROT_WRITE,
										MAP_PRIVATE | MAP_ANONYMOUS,
										-1, 0);
		g_free_chunks[curr_idx].push(p);
	}
	// extract chunk
	char * p = (char *)g_free_chunks[curr_idx].pop();
	// if chunk too big, split it
	while (curr_idx != req_idx) {
		curr_idx--;
		g_free_chunks[curr_idx].push(p + g_chunk_sizes[curr_idx]);
	}
	return p;
}

//--------------------------------------------------------------------
// Function: free_ex(void * p, uint32_t size)
// Description: Freees memory allocated with malloc_ex.
//--------------------------------------------------------------------
void free_ex(void * p, uint32_t size) {
	// calculate index suitable for required size
	uint32_t idx = log2(size);
	scoped_lock<rmutex> lock(g_lock);
	g_free_chunks[idx].push(p);
}

#if !defined(_WIN32)
//#if !defined(_WIN32) || defined(NDEBUG)
//--------------------------------------------------------------------
// Function: malloc(uint32_t size) -> void *
// Description: Replacement of standard malloc function.
//--------------------------------------------------------------------
extern "C" void * malloc(uint32_t size) throw() {
	size += MEM_ALLOC_ALIGMENT;
	char * p = (char *)malloc_ex(size);
	*((uint32_t *)p) = size;
	return p + MEM_ALLOC_ALIGMENT;
}

//--------------------------------------------------------------------
// Function: free(void * p)
// Description: Replacement of standard free function.
//--------------------------------------------------------------------
extern "C" void free(void * p) throw() {
	if (p == NULL) {
		return;
	}
	char * p1 = (char *)p - MEM_ALLOC_ALIGMENT;
	uint32_t size = *((uint32_t *)p1);
	free_ex(p1, size);
}

//--------------------------------------------------------------------
// Function: realloc(void * p, uint32_t new_size) -> void *
// Description: Replacement of standard realloc function.
//--------------------------------------------------------------------
extern "C" void * realloc(void * p, uint32_t new_size) throw() {
	if (p == NULL) {
		if (new_size == 0) {
			return NULL;
		}
		return malloc(new_size);
	}
	if (new_size == 0) {
		free(p);
		return NULL;
	}
	char * p1 = (char *)p - MEM_ALLOC_ALIGMENT;
	uint32_t size = *((uint32_t *)p1) - MEM_ALLOC_ALIGMENT;
	if (size >= new_size) {
		return p;
	}
	void * p2 = malloc(new_size);
	memcpy(p2, p, size);
	free(p);
	return p2;
}

#endif // !_WIN32 || NDEBUG
