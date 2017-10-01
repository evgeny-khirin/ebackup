///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : thread_pool.hpp
/// Author  : Evgeny Khirin <>
/// Description : Threads pool
///-------------------------------------------------------------------
#ifndef __thread_pool_hpp__
#define __thread_pool_hpp__

#include <assert.h>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <queue>

//--------------------------------------------------------------------
// Threads pool class
//--------------------------------------------------------------------
class thread_pool {
private:
  boost::thread_group									m_pool; 				// pool itself
	std::queue<boost::function0<void> >	m_queue;				// jobs queue
	boost::mutex												m_mutex;				// serialization mutex
	boost::condition										m_job_ready;		// signaled when m_queue
																											// is not empty
	boost::condition										m_job_finished;	// signaled when job finished
	uint32_t														m_size;					// size of pool
	uint32_t														m_running;			// number of jobs currently
																											// executed
	bool																m_stopped;			// is pool stopped?

	//--------------------------------------------------------------------
	// Function: run(thread_pool * pool, const boost::function0<void>& fun).
	// Description: Executes a job.
	//--------------------------------------------------------------------
	static void run(thread_pool * pool) {
		while (true) {
			boost::function0<void> job;
			{
				boost::mutex::scoped_lock scoped_lock(pool->m_mutex);
				while (pool->m_queue.empty()) {
					if (pool->m_stopped) {
						return;
					}
					pool->m_job_ready.wait(scoped_lock);
				}
				job = pool->m_queue.front();
				pool->m_queue.pop();
				pool->m_running++;
			}
			job();
			{
				boost::mutex::scoped_lock scoped_lock(pool->m_mutex);
				pool->m_running--;
				pool->m_job_finished.notify_one();
			}
		}
	}

public:
	//--------------------------------------------------------------------
	// Function: thread_pool().
	// Description: Constructor.
	//--------------------------------------------------------------------
	thread_pool() {m_stopped = false; m_size = m_running = 0;}

	//--------------------------------------------------------------------
	// Function: start(uint32_t threads).
	// Description: Starts the thread pool.
	//--------------------------------------------------------------------
	void start(uint32_t threads) {
		for (uint32_t i = 0; i < threads; i++) {
			m_pool.create_thread(boost::bind(run, this));
		}
		m_size = threads;
	}

	//--------------------------------------------------------------------
	// Function: size() -> uint32_t
	// Description: Returns size of thread pool.
	//--------------------------------------------------------------------
	uint32_t size() const {return m_size;}

	//--------------------------------------------------------------------
	// Function: stop().
	// Description: Stops the thread pool.
	//--------------------------------------------------------------------
	void stop() {
		{
			boost::mutex::scoped_lock scoped_lock(m_mutex);
			m_stopped = true;
			m_job_ready.notify_all();
		}
		wait();
		{
			boost::mutex::scoped_lock scoped_lock(m_mutex);
			m_job_ready.notify_all();
		}
		m_pool.join_all();
	}

	//--------------------------------------------------------------------
	// Function: spawn(const boost::function0<void>& func).
	// Description: Add job to the thread pool.
	//--------------------------------------------------------------------
	void spawn(const boost::function0<void>& fun) {
		boost::mutex::scoped_lock scoped_lock(m_mutex);
		assert(!m_stopped);
		m_queue.push(fun);
		m_job_ready.notify_one();
	}

	//--------------------------------------------------------------------
	// Function: wait().
	// Description: Blocks execution until all jobs are finished.
	//--------------------------------------------------------------------
	void wait() {
		while (true) {
			boost::mutex::scoped_lock scoped_lock(m_mutex);
			if (m_queue.empty() && m_running == 0) {
				return;
			}
			m_job_finished.wait(scoped_lock);
		}
	}
};

#endif // __thread_pool_hpp__

