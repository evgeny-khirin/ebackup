///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : ld.hpp
/// Author  : Evgeny Khirin <>
/// Description : Log device interface. It provides two important abstaractions
/// for logger:
///    1. Infinite device. Logger always writes to device sequantialy and can
///       peridocally trim device from begining. Last block can be written multiple
///       times, so device should take care that early written and flushed data remain
///       correct in those cases (ping-pong write).
///    2. Early error detection feature. If block is written to device, than
///       on read device should return correct block or error. Attempt to read
///       block that never written should return error.
///-------------------------------------------------------------------
#ifndef __ld_hpp__
#define __ld_hpp__

#include "bd.hpp"
#include "utils.hpp"

//--------------------------------------------------------------------
// Log device interface.
//--------------------------------------------------------------------
class log_device: public block_device {
public:
	//--------------------------------------------------------------------
	// Function: get_head() -> uint64_t
	// Description: Returns LSN of head. Log device should take care that
	// head saved secure.
	//--------------------------------------------------------------------
	virtual uint64_t get_head() = 0;

	//--------------------------------------------------------------------
	// Function: get_low_watter_mark() -> uint64_t
	// Description: Returns transaction manager checkpoint LSN.
	//--------------------------------------------------------------------
	virtual uint64_t get_checkpoint() = 0;

	//--------------------------------------------------------------------
	// Function: set_head(uint64_t head_lsn, uint64_t head_block, uint64_t low_watter_mark).
	// Description: Sets head and checkpoint LSNs. It is device responsibility to store them
	// securely.
	//--------------------------------------------------------------------
	virtual void set_head(uint64_t head_lsn, uint64_t head_block, uint64_t checkpoint_lsn) = 0;

	//--------------------------------------------------------------------
	// Function: get_tail() -> uint64_t
	// Description: Returns LSN of tail. Tail lsn passed on sync call.
	// Log device do not required to store tail securely on sync calls,
	// but tail must be stored securely on set_tail call. Tail must be greater or
	// equal to head.
	//--------------------------------------------------------------------
	virtual uint64_t get_tail() = 0;

	//--------------------------------------------------------------------
	// Function: set_tail(uint64_t lsn, uint64_t tail_block).
	// Description: Called by logger after internal recovery process to save tail LSN.
	// It is device responsibility to store this LSN securely.
	//--------------------------------------------------------------------
	virtual void set_tail(uint64_t lsn, uint64_t tail_block) = 0;

	//--------------------------------------------------------------------
	// Function: read(uint64_t n) -> block_ptr
	// Description: Reads block from device. Block numbers start from 0.
	// Attempt to read block that never written must return error.
	//--------------------------------------------------------------------
	virtual block_ptr read(uint64_t n) = 0;

	//--------------------------------------------------------------------
	// Function: write(uint64_t n, block_ptr & pb).
	// Description: Writes block to device. Size of data should be equal to
	// size of block on device. Last block can be overwritten few times by
	// logger, when logger has incomplete block which must be synchronized.
	// So devices those not support atomic block writes, must not be passed
	// directly to logger. Instead they should be connected to logger through
	// ping-pong intermediate device.
	//--------------------------------------------------------------------
	virtual void write(uint64_t n, block_ptr & pb) = 0;

	//--------------------------------------------------------------------
	// Function: sync(uint64_t tail_lsn)
	// Description: Ensures that all pending writes are finished successfully.
	// Device can save (but not required) tail for futher return in get_tail function.
	//--------------------------------------------------------------------
	virtual void sync(uint64_t tail_lsn) = 0;

	//--------------------------------------------------------------------
	// Function: block_size() -> uint32_t
	// Description: Returns block size of device.
	//--------------------------------------------------------------------
	virtual uint32_t block_size() = 0;

	//--------------------------------------------------------------------
	// Function: block_ptr alloc().
	// Description: Allocates block from device.
	//--------------------------------------------------------------------
	virtual block_ptr alloc() = 0;

	// Those functions from base class are pronhibited in log device.
private:
	virtual void write(block_list & blocks);
	virtual void sync();
	virtual uint64_t capacity();
};

//--------------------------------------------------------------------
// Intermediate log device. Passes all calls to underlaying device.
//--------------------------------------------------------------------
class intermediate_log_device: public log_device {
protected:
	log_device* m_pdevice;			// pointer to underlaying device.

public:
	//--------------------------------------------------------------------
	// Function: intermediate_log_device().
	// Description: Constructor.
	//--------------------------------------------------------------------
	intermediate_log_device() {m_pdevice = NULL;}

	// see base class.
	virtual void stop();

	// see base class.
	virtual void stats(no_stats_list & list);

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual uint64_t get_head();

	// see base class.
	virtual uint64_t get_checkpoint();

	// see base class.
	virtual void set_head(uint64_t lsn, uint64_t head_block, uint64_t checkpoint_lsn);

	// see base class.
	virtual uint64_t get_tail();

	// see base class.
	virtual void set_tail(uint64_t lsn, uint64_t tail_block);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual void sync(uint64_t tail_lsn);

	// see base class.
	virtual uint32_t block_size();

	// see base class.
	virtual block_ptr alloc();
};

#endif // __ld_hpp__

