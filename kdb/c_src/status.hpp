///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : status.hpp
/// Author  : Evgeny Khirin <>
/// Description : Error codes.
///-------------------------------------------------------------------
#ifndef __status_hpp__
#define __status_hpp__

#include <exception>

//--------------------------------------------------------------------
// result values
//--------------------------------------------------------------------
enum result {
	ok,												// everything is fine
	my_eof,										// end of file reached
	not_found									// object not found
};

//--------------------------------------------------------------------
// Function: res_to_str(result r) -> const char *
// Description: Translates result value to string.
//--------------------------------------------------------------------
const char * res_to_str(result r);

//--------------------------------------------------------------------
// KDB exception - used in linker script in linux.
//--------------------------------------------------------------------
class kdb_exception: public std::exception {
};

//--------------------------------------------------------------------
// Exception definition macro
//--------------------------------------------------------------------
#define DEF_EXCEPTION(e)															\
	class e: public kdb_exception {											\
	public:																							\
		virtual const char * what() const throw() {				\
			return #e;																			\
			}																								\
	};

//--------------------------------------------------------------------
// Exceptions forward declarartions for code completion in some editors.
//--------------------------------------------------------------------
class already_registered;
class not_registered;
class missed_param;
class inv_param;
class not_supported;
class open_failed;
class seek_failed;
class write_failed;
class read_failed;
class sync_failed;
class bad_checksum;
class list_parse;
class power_failure;
class block_too_small;
class bad_header;
class block_never_written;
class out_of_capacity;
class test_failed;
class no_free_space;
class timeout;
class capacity_too_small;
class key_value_pair_too_big;
class not_own_buffer;
class internal_error;
class not_recovered;

//--------------------------------------------------------------------
// Exceptions
//--------------------------------------------------------------------
DEF_EXCEPTION(already_registered)				// object already registered
DEF_EXCEPTION(not_registered)						// object is not registered
DEF_EXCEPTION(missed_param)							// obligatory parameter is missed
DEF_EXCEPTION(inv_param)								// invalid parameter is passed
DEF_EXCEPTION(not_supported)						// unsupported function is called
DEF_EXCEPTION(open_failed)							// open operation is failed
DEF_EXCEPTION(seek_failed)							// seek operation is failed
DEF_EXCEPTION(write_failed)							// write operation is failed
DEF_EXCEPTION(read_failed)							// read operation is failed
DEF_EXCEPTION(sync_failed)							// sync operation is failed
DEF_EXCEPTION(bad_checksum)							// invalid checksum
DEF_EXCEPTION(list_parse)								// failed to parse list
DEF_EXCEPTION(power_failure)						// simulates power failure
DEF_EXCEPTION(block_too_small)					// size of block is too small
DEF_EXCEPTION(bad_header)								// signature or version check of
																				// header is failed.
DEF_EXCEPTION(block_never_written)			// attempt to read block, which
																				// never written
DEF_EXCEPTION(out_of_capacity)					// block number is greater than
																				// device capacity.
DEF_EXCEPTION(test_failed)							// unit test failed
DEF_EXCEPTION(no_free_space)						// no free space on device
DEF_EXCEPTION(timeout)									// operation timed out
DEF_EXCEPTION(capacity_too_small)				// device capacity is too small
DEF_EXCEPTION(key_value_pair_too_big)		// size of key-value is too big for
																				// storing in B-tree
DEF_EXCEPTION(not_own_buffer)						// attempt to store too much data
																				// in serial buffer
DEF_EXCEPTION(internal_error)						// bug in program
DEF_EXCEPTION(not_recovered)						// TM is not recovered or stopped

#endif // __status_hpp__

