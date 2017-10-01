///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : mutex.hpp
/// Author  : Evgeny Khirin <>
/// Description : Miscelaneos mutex implementations.
///-------------------------------------------------------------------
#ifndef __mutex_hpp__
#define __mutex_hpp__

#include <stdint.h>
#include <assert.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/sysinfo.h>
#include <pthread.h>
#endif

#include "utils.hpp"

//--------------------------------------------------------------------
// Scoped lock
//--------------------------------------------------------------------
template <class Lock>
class scoped_lock {
private:
	Lock *	m_lock;

	scoped_lock();

public:
	scoped_lock(Lock & lock) {
		m_lock = &lock;
		m_lock->lock();
	}

	scoped_lock(Lock * lock) {
		m_lock = lock;
		if (m_lock != NULL) {
			m_lock->lock();
		}
	}

	~scoped_lock() {
		if (m_lock != NULL) {
			m_lock->unlock();
		}
	}
};

//--------------------------------------------------------------------
// Recursive mutex
//--------------------------------------------------------------------
class rmutex {
private:
#ifdef _WIN32
	CRITICAL_SECTION	m_mutex;
	uint32_t					m_depth;
#else
	pthread_mutex_t		m_mutex;
#endif

public:
	//--------------------------------------------------------------------
	// Function: recursive_mutex().
	// Description: Constructor.
	//--------------------------------------------------------------------
	rmutex() {
#ifdef _WIN32
		InitializeCriticalSection(&m_mutex);
		m_depth = 0;
#else
		pthread_mutexattr_t attr;
		int rc = pthread_mutexattr_init(&attr);
		assert(rc == 0);
		rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE_NP);
		assert(rc == 0);
		rc = pthread_mutex_init(&m_mutex, &attr);
		assert(rc == 0);
		rc = pthread_mutexattr_destroy(&attr);
		assert(rc == 0);
#endif
	}

	//--------------------------------------------------------------------
	// Function: ~rmutex().
	// Description: Destructor.
	//--------------------------------------------------------------------
	~rmutex() {
#ifdef _WIN32
		DeleteCriticalSection(&m_mutex);
#else
		pthread_mutex_destroy(&m_mutex);
#endif
	}

	//--------------------------------------------------------------------
	// Function: lock(uint32_t msecs = 5000).
	// Description: Attempts to lock mutex during number of miliseconds and
	// than throws timeout exception.
	//--------------------------------------------------------------------
#ifdef _WIN32
	void lock(uint32_t msecs = 5000) {
		uint32_t slept = 0;
		do {
			if (TryEnterCriticalSection(&m_mutex)) {
				m_depth++;
				return;
			}
			msleep(1);
			slept += 1;
		} while (slept < msecs);
		throw timeout();
	}
#else
	void lock(uint32_t msecs = 5000) {
		uint32_t slept = 0;
		do {
			if (pthread_mutex_trylock(&m_mutex) == 0) {
				return;
			}
			msleep(1);
			slept += 1;
		} while (slept < msecs);
		throw timeout();
	}
#endif

	//--------------------------------------------------------------------
	// Function: unlock() -> bool.
	// Description: Unlocks mutex. Returns true if mutex unlocked successfully.
	//--------------------------------------------------------------------
	bool unlock() {
#ifdef _WIN32
		if (m_depth == 0) {
			return false;
		}
		m_depth--;
		LeaveCriticalSection(&m_mutex);
		return true;
#else
		return pthread_mutex_unlock(&m_mutex) == 0;
#endif
	}

	//--------------------------------------------------------------------
	// Function: unlock_completely().
	// Description: Unlocks mutex completely.
	//--------------------------------------------------------------------
	void unlock_completely() {
		while (unlock()) {
		}
	}
};

#endif // __mutex_hpp__

