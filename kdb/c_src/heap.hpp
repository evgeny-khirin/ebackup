///-------------------------------------------------------------------
/// Copyright (c) 2002-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : heap.cpp
/// Author  : Evgeny Khirin <>
/// Description : Replacement of standard memory mangement routines.
///-------------------------------------------------------------------
#ifndef __heap_hpp__
#define __heap_hpp__

#include <stdint.h>

//--------------------------------------------------------------------
// Function: malloc_ex(uint32_t & size) -> void *
// Description: Allocates memory at least required size. Size is rounded to
// nearst greather power of two.
//--------------------------------------------------------------------
void * malloc_ex(uint32_t & size);

//--------------------------------------------------------------------
// Function: free_ex(void * p, uint32_t size)
// Description: Freees memory allocated with malloc_ex.
//--------------------------------------------------------------------
void free_ex(void * p, uint32_t size);

#endif // __heap_hpp__

