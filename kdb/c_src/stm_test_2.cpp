///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : stm_test_2.cpp
/// Author  : Evgeny Khirin <>
/// Description : Unit test for increase capacity feature of stm module.
/// Test checks correct program behaivior with many devices in case of
/// power failures and does not measure performance.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <boost/random/linear_congruential.hpp>
#include <set>
#include <vector>

#include "bd_file.hpp"
#include "bd_factor.hpp"
#include "bd_pool.hpp"
#include "bd_crash.hpp"
#include "bd_disord.hpp"
#include "bd_crc.hpp"
#include "bd_adler.hpp"
#include "bd_dwrite.hpp"
#include "bd_cache.hpp"
#include "ld_bd.hpp"
#include "ld_pp.hpp"
#include "logger.hpp"
#include "tm.hpp"
#include "bm.hpp"
#include "stm.hpp"
#include "thread_pool.hpp"
#include "utils.hpp"

#ifdef WIN32
#define DATA_FILE		"r:/test.dat"
#define LOG_FILE		"r:/test.log"
#else
#define DATA_FILE		"/var/ramdisk/test.dat"
#define LOG_FILE		"/var/ramdisk/test.log"
#endif

#define MAX_DEVS				32
#define MAX_DEVS_TOTAL	64
#define MIN_CAPACITY	5000

//--------------------------------------------------------------------
// global variables
//--------------------------------------------------------------------
uint32_t						g_log_block_size;
uint32_t						g_data_block_size;
int32_t							g_loops;
boost::minstd_rand	g_rand;
int32_t							g_seed;
rmutex						g_mutex;
bool								g_log_bd_crash_runs;
bool								g_data_bd_crash_runs;
bool								g_finished;
uint64_t						g_log_capacity;
uint64_t						g_data_capacity;
uint32_t						g_log_devs_count;						// number of underlaying devices for
																								// bd_pool or bd_raid0 device
uint32_t						g_data_devs_count;					// number of underlaying devices for
																								// bd_pool or bd_raid0 device
uint64_t						g_log_devs_capacity[MAX_DEVS_TOTAL];	// capacity of each device
uint64_t						g_data_devs_capacity[MAX_DEVS_TOTAL];	// capacity of each device
uint32_t						g_log_factor;
uint32_t						g_data_factor;

//--------------------------------------------------------------------
// returns random value: [0..N-1]
//--------------------------------------------------------------------
uint32_t random_value_0(uint32_t n) {
	return g_rand() % n;
}

//--------------------------------------------------------------------
// returns random value: [1..N]
//--------------------------------------------------------------------
uint32_t random_value_1(uint32_t n) {
	return random_value_0(n) + 1;
}

//--------------------------------------------------------------------
// starts device tree
//--------------------------------------------------------------------
void start_devs() {
	printf("\nstarting devices\n");
	// start log_bd_file devices
	std::string devs_list("[");
	for (uint32_t i = 0; i < g_log_devs_count; i++) {
		char dev_name[32];
		char fac_name[32];
		char file_name[256];
		sprintf(dev_name, "log_bd_file_%u", i + 1);
		sprintf(file_name, "%s.%u", LOG_FILE, i + 1);
		bd_file::start(dev_name, g_log_block_size, g_log_devs_capacity[i], file_name);
		sprintf(fac_name, "log_bd_factor_%u", i + 1);
		bd_factor::start(fac_name, dev_name, g_log_factor, true);
		if (i > 0) {
			devs_list += ',';
		}
		devs_list += fac_name;
	}
	devs_list += "]";
	// start log top level compound device
	bd_pool::start("log_bd_device", devs_list, 0);
	// check that block size and capacity correctly reported by compound devices.
	block_device * pdevice = (block_device *)whereis("log_bd_device");
	if (g_log_capacity / g_log_factor != pdevice->capacity()) {
		printf("ERROR: wrong capacity %llu reported by log_bd_device, expected %llu\n",
					 pdevice->capacity(), g_log_capacity / g_log_factor);
		throw test_failed();
	}
	if (g_log_block_size * g_log_factor != pdevice->block_size()) {
		printf("ERROR: wrong block_size %u reported by log_bd_device, expected %u\n",
					 pdevice->block_size(), g_log_block_size * g_log_factor);
		throw test_failed();
	}
	// start log_bd_crash device
	{
		scoped_lock<rmutex> lock(g_mutex);
		bd_crash::start("log_bd_crash", "log_bd_device", g_seed);
		g_log_bd_crash_runs = true;
	}
	// start log_bd_disord device
	bd_disord::start("log_bd_disord", "log_bd_crash", random_value_1(32), g_seed);
	// start log_bd_crc device
	bd_crc::start("log_bd_crc", "log_bd_disord", 0);
	// start log_bd_adler device
	bd_adler::start("log_bd_adler", "log_bd_crc");
	// start log_bd_dwrite device
	bd_dwrite::start("log_bd_dwrite", "log_bd_adler", random_value_1(32));
	// start log_bd_cache device
	bd_cache::start("log_bd_cache", "log_bd_dwrite", random_value_1(32));
	// start ld_bd device
	if (!ld_bd::is_formatted("log_bd_cache", "STM_TEST_2")) {
		ld_bd::format("log_bd_cache", "STM_TEST_2");
	}
	ld_bd::start("ld_bd", "log_bd_cache");
	// start ld_pp device
	ld_pp::start("ld_pp", "ld_bd");
	// start logger
	blogger::start("logger", "ld_pp");
	// start transaction manager
	uint64_t cp_log_size = MIN_CAPACITY / 8 * g_log_block_size;
	trans_mgr::start("tm", "logger", "bm", cp_log_size);
	// start data_bd_file devices
	devs_list = "[";
	for (uint32_t i = 0; i < g_data_devs_count; i++) {
		char dev_name[32];
		char fac_name[32];
		char file_name[256];
		sprintf(dev_name, "data_bd_file_%u", i + 1);
		sprintf(file_name, "%s.%u", DATA_FILE, i + 1);
		bd_file::start(dev_name, g_data_block_size, g_data_devs_capacity[i], file_name);
		sprintf(fac_name, "data_bd_factor_%u", i + 1);
		bd_factor::start(fac_name, dev_name, g_data_factor, true);
		if (i > 0) {
			devs_list += ',';
		}
		devs_list += fac_name;
	}
	devs_list += "]";
	// start data top level compound device
	bd_pool::start("data_bd_device", devs_list, 0);
	// check that block size and capacity correctly reported by compound devices.
	pdevice = (block_device *)whereis("data_bd_device");
	if (g_data_capacity / g_data_factor != pdevice->capacity()) {
		printf("ERROR: wrong capacity %llu reported by data_bd_device, expected %llu\n",
					 pdevice->capacity(), g_data_capacity / g_data_factor);
		throw test_failed();
	}
	if (g_data_block_size * g_data_factor != pdevice->block_size()) {
		printf("ERROR: wrong block_size %u reported by data_bd_device, expected %u\n",
					 pdevice->block_size(), g_data_block_size * g_data_factor);
		throw test_failed();
	}
	// start data_bd_crash device
	{
		scoped_lock<rmutex> lock(g_mutex);
		bd_crash::start("data_bd_crash", "data_bd_device", g_seed);
		g_data_bd_crash_runs = true;
	}
	// start data_bd_disord device
	bd_disord::start("data_bd_disord", "data_bd_crash", random_value_1(32), g_seed);
	// start data_bd_crc device
	ld_bd * pld_bd = (ld_bd *)whereis("ld_bd");
	bd_crc::start("data_bd_crc", "data_bd_disord", pld_bd->system_generation());
	// start data_bd_dwrite device
	bd_dwrite::start("data_bd_dwrite", "data_bd_crc", random_value_1(32));
	// start buffer manager
	buffer_mgr::start("bm", "data_bd_dwrite", "tm", random_value_1(32));
	// start STM
	stm::start("stm", "tm", "bm");
	// recover TM
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	tm->recover();
}

//--------------------------------------------------------------------
// stops device tree in reverse order.
//--------------------------------------------------------------------
void stop_devs(bool do_stop) {
	scoped_lock<rmutex> lock(g_mutex);
	g_log_bd_crash_runs = g_data_bd_crash_runs = false;
	// stp TM
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	if (do_stop && tm != NULL) {
		tm->stop();
	}
	// stop BM
	buffer_mgr * bm = (buffer_mgr *)whereis("bm");
	if (do_stop && bm != NULL) {
		bm->stop();
	}
	// stop STM
	stm * pstm = (stm *)whereis("stm");
	if (do_stop && pstm != NULL) {
		pstm->stop();
	}
	// delete STM
	if (pstm != NULL) {
		pstm->del_underlaying();
		delete pstm;
	}
	// delete BM
	if (bm != NULL) {
		bm->del_underlaying();
		delete bm;
	} else {
		// power crash may occur in start_devs and BM may be not started.
		// delete devices in reverse order in that case.
		delete whereis("data_bd_dwrite");
		delete whereis("data_bd_crc");
		delete whereis("data_bd_disord");
		delete whereis("data_bd_crash");
		delete whereis("data_bd_device");
		for (uint32_t i = 0; i < g_data_devs_count; i++) {
			char dev_name[32];
			sprintf(dev_name, "data_bd_factor_%u", i + 1);
			delete whereis(dev_name);
			sprintf(dev_name, "data_bd_file_%u", i + 1);
			delete whereis(dev_name);
		}
	}
	// delete TM
	if (tm != NULL) {
		tm->del_underlaying();
		delete tm;
	} else {
		// power crash may occur in start_devs and TM may be not started.
		// delete devices in reverse order in that case.
		delete whereis("logger");
		delete whereis("ld_pp");
		delete whereis("ld_bd");
		delete whereis("log_bd_cache");
		delete whereis("log_bd_dwrite");
		delete whereis("log_bd_adler");
		delete whereis("log_bd_crc");
		delete whereis("log_bd_disord");
		delete whereis("log_bd_crash");
		delete whereis("log_bd_device");
		for (uint32_t i = 0; i < g_log_devs_count; i++) {
			char dev_name[32];
			sprintf(dev_name, "log_bd_factor_%u", i + 1);
			delete whereis(dev_name);
			sprintf(dev_name, "log_bd_file_%u", i + 1);
			delete whereis(dev_name);
		}
	}
}

//--------------------------------------------------------------------
// test global variables
//--------------------------------------------------------------------
std::set<uint64_t> g_allocated_set;

//--------------------------------------------------------------------
// allocation test
//--------------------------------------------------------------------
void test_alloc(uint64_t & last_block_n) {
	last_block_n = UINT64_MAX;
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	stm * pstm = (stm *)whereis("stm");
	uint32_t id = tm->begin_transaction(hard);
	uint64_t n = UINT64_MAX;
	try {
		n = pstm->alloc();
		if (g_allocated_set.find(n) != g_allocated_set.end()) {
			printf("ERROR: block %llu allocated twice\n", n);
			throw test_failed();
		}
		if (n >= g_data_capacity / g_data_factor) {
			printf("ERROR: allocated block %llu is out of device capacity %llu\n",
						 n, g_data_capacity / g_data_factor);
			throw test_failed();
		}
	} catch (no_free_space &) {
		if (g_allocated_set.size() != g_data_capacity / g_data_factor) {
			printf("ERROR: STM reports no_free_space, allocated set size %u, data capacity %llu\n",
						 g_allocated_set.size(), g_data_capacity / g_data_factor);
			throw test_failed();
		}
		if (!pstm->is_full()) {
			printf("ERROR: STM reports no_free_space, but is_full() returns false\n");
			throw test_failed();
		}
	}
	// random rollback
	if (random_value_1(10) == 1) {
		tm->rollback(id);
		// check that used is correct
		if (pstm->used() != g_allocated_set.size()) {
			printf("ERROR: wrong STM used %llu after rollback of alloc, expected %u\n",
						 pstm->used(), g_allocated_set.size());
			throw test_failed();
		}
		return;
	}
	// Commit is not atomic. It is guaranteed that if commit succeeded, than data are in place.
	// If commit is failed, than data may be in place as well. So save it for latter correction.
	last_block_n = n;
	tm->commit(id);
	last_block_n = UINT64_MAX;
	// insert in allocated set
	if (n != UINT64_MAX) {
		g_allocated_set.insert(n);
	}
	// check that used is correct
	if (pstm->used() != g_allocated_set.size()) {
		printf("ERROR: wrong STM used %llu after alloc commit, expected %u\n",
					 pstm->used(), g_allocated_set.size());
		throw test_failed();
	}
	return;
}

//--------------------------------------------------------------------
// random free test
//--------------------------------------------------------------------
void test_random_free(uint64_t & last_block_n) {
	last_block_n = UINT64_MAX;
	if (g_allocated_set.size() == 0) {
		return;
	}
	// find block for free
	uint32_t skip = random_value_0(g_allocated_set.size());
	uint64_t n = UINT64_MAX;
	for (std::set<uint64_t>::iterator i = g_allocated_set.begin(); i != g_allocated_set.end(); i++) {
		if (skip-- == 0) {
			n = *i;
			break;
		}
	}
	stm * pstm = (stm *)whereis("stm");
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	uint32_t id = tm->begin_transaction(hard);
	pstm->free(n);
	// random  rollback
	if (random_value_1(10) == 1) {
		tm->rollback(id);
		// check used
		if (pstm->used() != g_allocated_set.size()) {
			printf("ERROR: wrong STM used %llu after rollback of free, expected %u\n",
						 pstm->used(), g_allocated_set.size());
			throw test_failed();
		}
		return;
	}
	// Commit is not atomic. It is guaranteed that if commit succeeded, than data are in place.
	// If commit is failed, than data may be in place as well. So save it for latter correction.
	last_block_n = n;
	tm->commit(id);
	last_block_n = UINT64_MAX;
	// remove from allocated set
	g_allocated_set.erase(n);
	// check used
	if (pstm->used() != g_allocated_set.size()) {
		printf("ERROR: wrong STM used %llu after free commit, expected %u\n",
					 pstm->used(), g_allocated_set.size());
		throw test_failed();
	}
}

//--------------------------------------------------------------------
// increase log capacity test
//--------------------------------------------------------------------
void increase_log_capacity() {
	if (g_log_devs_count < MAX_DEVS_TOTAL && random_value_1(10000) == 1) {
		// increase log device capacity
		g_log_devs_count++;
		do {
			g_log_devs_capacity[g_log_devs_count - 1] = random_value_1(1000) / g_log_factor * g_log_factor;
		} while (g_log_devs_capacity[g_log_devs_count - 1] == 0);
		g_log_capacity += g_log_devs_capacity[g_log_devs_count - 1];
		printf("increasing log capacity %u\n", g_log_devs_count);
		char dev_name[32];
		char file_name[256];
		sprintf(dev_name, "log_bd_file_%u", g_log_devs_count);
		sprintf(file_name, "%s.%u", LOG_FILE, g_log_devs_count);
		bd_file::start(dev_name, g_log_block_size, g_log_devs_capacity[g_log_devs_count - 1], file_name);
		char fac_name[32];
		sprintf(fac_name, "log_bd_factor_%u", g_log_devs_count);
		bd_factor::start(fac_name, dev_name, g_log_factor, true);
		bd_pool * pdev = (bd_pool *)whereis("log_bd_device");
		pdev->add_device(fac_name);
		printf("log capacity %u increased succesfully\n", g_log_devs_count);
	}
}

//--------------------------------------------------------------------
// increase data capacity test
//--------------------------------------------------------------------
void increase_data_capacity() {
	if (g_data_devs_count < MAX_DEVS_TOTAL && random_value_1(10000) == 1) {
		// increase log device capacity
		g_data_devs_count++;
		do {
			g_data_devs_capacity[g_data_devs_count - 1] = random_value_1(1000) / g_data_factor * g_data_factor;
		} while (g_data_devs_capacity[g_data_devs_count - 1] == 0);
		g_data_capacity += g_data_devs_capacity[g_data_devs_count - 1];
		printf("increasing data capacity: %u\n", g_data_devs_count);
		char dev_name[32];
		char file_name[256];
		sprintf(dev_name, "data_bd_file_%u", g_data_devs_count);
		sprintf(file_name, "%s.%u", DATA_FILE, g_data_devs_count);
		bd_file::start(dev_name, g_data_block_size, g_data_devs_capacity[g_data_devs_count - 1], file_name);
		char fac_name[32];
		sprintf(fac_name, "data_bd_factor_%u", g_data_devs_count);
		bd_factor::start(fac_name, dev_name, g_data_factor, true);
		bd_pool * pdev = (bd_pool *)whereis("data_bd_device");
		pdev->add_device(fac_name);
		printf("data capacity %u increased succesfully\n", g_data_devs_count);
	}
}

//--------------------------------------------------------------------
// runs test
//--------------------------------------------------------------------
void run_test() {
	stm * pstm = (stm *)whereis("stm");
	printf("TEST: ============ loops left %d\n", g_loops);
	static bool fill = true;
	static int last_op = 0;
	static uint64_t last_block_n = UINT64_MAX;
	bool first_loop = true;
	while (g_loops > 0) {
		uint64_t used = pstm->used();
		uint32_t set_size = g_allocated_set.size();
		if (!first_loop && used != set_size) {
			printf("ERROR 0: STM used %llu does not match allocated set size %u\n",
						 used, set_size);
			throw test_failed();
		} else if (used != set_size) {
			// first loop
			if (last_block_n == UINT64_MAX || last_op == 0) {
				printf("ERROR 1: STM used %llu does not match allocated set size %u, "
							 "last block %llu, last op %d\n",
							 used, set_size, last_block_n, last_op);
				throw test_failed();
			}
			if (last_op == 1) {
				// last operation was succeeded alloc and commit failed. In that case block is not inserted
				// in allocation set, but probably removed from free list in STM. So used will be greater
				// on 1 than set size and block must be inserted in set.
				if (used - set_size == 1) {
					g_allocated_set.insert(last_block_n);
				} else {
					printf("ERROR 2: STM used %llu does not match allocated set size %u, "
								 "last block %llu, last op %d\n",
								 used, set_size, last_block_n, last_op);
					throw test_failed();
				}
			}
			if (last_op == 2) {
				// last operation was succeeded free and commit failed. In that case block is not removed
				// from allocation set, but probably added to free list in STM. So set size will be greater
				// on 1 than used and block must be removed from set.
				if (set_size - used  == 1) {
					g_allocated_set.erase(last_block_n);
				} else {
					printf("ERROR 3: STM used %llu does not match allocated set size %u, "
								 "last block %llu, last op %d\n",
								 used, set_size, last_block_n, last_op);
					throw test_failed();
				}
			}
		}
		first_loop = false;
		increase_log_capacity();
		increase_data_capacity();
		if (pstm->used() == g_data_capacity / g_data_factor) {
			if (!pstm->is_full()) {
				printf("ERROR: wrong is_full() result on full STM\n");
				throw test_failed();
			}
		} else if (pstm->is_full()) {
			printf("ERROR: wrong is_full() result on non-full STM\n");
			throw test_failed();
		}
		if (used == 0) {
			fill = true;
		}
		if (used == g_data_capacity / g_data_factor) {
			fill = false;
		}
		if (g_loops % 10000 == 0) {
			printf("loops left %d, device usage %f%%\n",
						 g_loops, (double)used / (g_data_capacity / g_data_factor) * 100);
		}
		last_block_n = UINT64_MAX;
		last_op = 0;
		if (fill) {
			switch (random_value_1(3)) {
			case 1:
				last_op = 2;
				test_random_free(last_block_n);
				break;
			default:
				last_op = 1;
				test_alloc(last_block_n);
			}
		} else {
			switch (random_value_1(3)) {
			case 1:
				last_op = 1;
				test_alloc(last_block_n);
				break;
			default:
				last_op = 2;
				test_random_free(last_block_n);
			}
		}
		g_loops--;
	}
}

//--------------------------------------------------------------------
// killer loop.
//--------------------------------------------------------------------
void killer_loop() {
	int power_n = 1;
	boost::minstd_rand my_rand;
	my_rand.seed(g_seed);
	while (true) {
		if (g_finished) {
			return;
		}
		msleep(my_rand() % 2000);
		if (g_finished) {
			return;
		}
		scoped_lock<rmutex> lock(g_mutex);
		if (g_log_bd_crash_runs) {
			bd_crash * pcrash = (bd_crash *)whereis("log_bd_crash");
			pcrash->ops(true);
		}
		if (g_data_bd_crash_runs) {
			bd_crash * pcrash = (bd_crash *)whereis("data_bd_crash");
			pcrash->ops(true);
		}
		if (g_log_bd_crash_runs || g_data_bd_crash_runs) {
			printf("\n****************************** power failure %d\n\n", power_n);
			power_n++;
			g_log_bd_crash_runs = g_data_bd_crash_runs = false;
		}
	}
}

//--------------------------------------------------------------------
// main
//--------------------------------------------------------------------
int main() {
	// delete files.
#ifdef WIN32
	system("del r:\\test.dat*");
	system("del r:\\test.log*");
#else
	system("rm -rf /var/ramdisk/test.dat*");
	system("rm -rf /var/ramdisk/test.log*");
#endif
	// initialize random namber generator
	g_seed = (uint32_t)time(NULL);
//	g_seed = 1273037919;
	printf("seed : %u\n", g_seed);
	g_rand.seed(g_seed);
	// skip first 10000 numbers to get better randomization
	for (int i = 0; i < 10000; i++) {
		random_value_0(100);
	}
	// initialize test parameters
	g_log_block_size = 128 + random_value_1(1024);
	g_data_block_size = 128 + random_value_1(1024);
	g_loops = random_value_1(1000000);
	g_log_capacity = MIN_CAPACITY * 4 + random_value_1(10000);
	g_data_capacity = MIN_CAPACITY + random_value_1(10000);
	g_log_devs_count = random_value_1(MAX_DEVS);
	g_data_devs_count = random_value_1(MAX_DEVS);
	g_log_factor = random_value_1(4);
	g_data_factor = random_value_1(4);
	// init log devices
	if (g_log_devs_count == 1) {
		g_log_devs_capacity[0] = g_log_capacity / g_log_factor * g_log_factor;
		g_log_capacity = g_log_devs_capacity[0];
	} else {
		uint64_t mid_capacity = g_log_capacity / g_log_devs_count * 2;
		do {
			g_log_capacity = 0;
			for (uint32_t i = 0; i < g_log_devs_count; i++) {
				uint64_t dev_capacity;
				do {
					dev_capacity = random_value_1((uint32_t)mid_capacity) / g_log_factor * g_log_factor;
				} while (dev_capacity == 0);
				g_log_devs_capacity[i] = dev_capacity;
				g_log_capacity += dev_capacity;
			}
		} while (g_log_capacity < MIN_CAPACITY);
	}
	// init data devices
	if (g_data_devs_count == 1) {
		g_data_devs_capacity[0] = g_data_capacity / g_data_factor * g_data_factor;
		g_data_capacity = g_data_devs_capacity[0];
	} else {
		uint64_t mid_capacity = g_data_capacity / g_data_devs_count * 2;
		do {
			g_data_capacity = 0;
			for (uint32_t i = 0; i < g_data_devs_count; i++) {
				uint64_t dev_capacity;
				do {
					dev_capacity = random_value_1((uint32_t)mid_capacity) / g_data_factor * g_data_factor;
				} while (dev_capacity == 0);
				g_data_devs_capacity[i] = dev_capacity;
				g_data_capacity += dev_capacity;
			}
		} while (g_data_capacity < MIN_CAPACITY);
	}
	printf("loops %u\n", g_loops);
	printf("log block size %u, log capacity %llu, log device size %f MB\n",
				 g_log_block_size, g_log_capacity,
				 g_log_capacity * g_log_block_size / 1024.0 / 1024.0);
	printf("data block size %u, data capacity %llu, data device size %f MB\n",
				 g_data_block_size, g_data_capacity,
				 g_data_capacity * g_data_block_size / 1024.0 / 1024.0);
	// execute test
	thread_pool killer;
	killer.start(1);
	killer.spawn(killer_loop);
	while (true) {
		try {
			start_devs();
			run_test();
			stop_devs(true);
			start_devs();
			stop_devs(true);
			break;
		} catch (power_failure &) {
			stop_devs(false);
		}
	}
	g_finished = true;
	killer.stop();
	// report ok
	printf("ok\n");
	return  0;
}

