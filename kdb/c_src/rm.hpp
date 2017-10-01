///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : rm.hpp
/// Author  : Evgeny Khirin <>
/// Description : Resource manager interface.
///-------------------------------------------------------------------
#ifndef __rm_hpp__
#define __rm_hpp__

#include "ld.hpp"

//--------------------------------------------------------------------
// Resource manager interface.
//--------------------------------------------------------------------
class resource_mgr: public named_object {
public:
	//--------------------------------------------------------------------
	// Function: undo(const void * data, uint32_t size, void * opaque)
	// Description: Undoes update log entry. Called by transaction manager as part
	// of rollback or recovery process. By default, does nothing.
	//    n = block number
	//    data = update data, written by resource manager with tm:log_update function.
	//    size = size of data.
	//    opaque = void . Resource manager must pass this parameter as is to
	//       tm:log_compensate.
	//--------------------------------------------------------------------
	virtual void undo(uint64_t n, const void * data, uint32_t size, void * opaque);

	//--------------------------------------------------------------------
	// Function: redo(uint64_t n, bool state_only, const void * data, uint32_t size, uint64_t redo_lsn)
	// Description: Redoes compensate log entry. Called by transaction
	// manager as part of recovery process. By default, does nothing.
	//    n = block number
	// 		state_only = no physical redo on block, redo state only, if necessary.
	//    data = update or compensation data, written by resource manager with
	//       tm:log_update or tm:log_compensate function.
	//    size = size of data.
	//    redo_lsn = redo lsn. Pass this LSN to bm:write function, if necessary.
	//--------------------------------------------------------------------
	virtual void redo(uint64_t n, bool state_only, const void * data, uint32_t size, uint64_t redo_lsn);

	//--------------------------------------------------------------------
	// Function: checkpoint()
	// Description: Tells resource manager to save its state in checkpoint.
	// By default, does nothing.
	//--------------------------------------------------------------------
	virtual void checkpoint();

	//--------------------------------------------------------------------
	// Function: recover_state(const void * data, uint32_t size, uint64_t redo_lsn)
	// Description: Resource manager must recover its state saved on last checkpoint.
	// Called by transaction manager as part of recovery process.  By default,
	// does nothing.
	//    data = state data, written by resource manager with tm:log_rm_state function.
	//    size = size of data.
	//    redo_lsn = state's redo LSN. Resource manager uses this LSN in order
	//       to know when to start redo operations on state variables.
	//--------------------------------------------------------------------
	virtual void recover_state(const void * data, uint32_t size, uint64_t redo_lsn);

	//--------------------------------------------------------------------
	// Function: recover_finished()
	// Description: Called by transaction manager when recover process is terminated
	// to notify resource manager. By default, does nothing.
	//--------------------------------------------------------------------
	virtual void recover_finished();
};

#endif // __rm_hpp__

