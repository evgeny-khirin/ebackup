///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : ld_bd_test.cpp
/// Author  : Evgeny Khirin <>
/// Description : Unit test for ld_bd. Test checks correct program behaivior
/// with many devices in case of power failures and does not measure performance.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <boost/random/linear_congruential.hpp>

#include "bd_file.hpp"
#include "bd_factor.hpp"
#include "bd_pool.hpp"
#include "bd_raid0.hpp"
#include "bd_crash.hpp"
#include "bd_disord.hpp"
#include "bd_crc.hpp"
#include "bd_adler.hpp"
#include "bd_dwrite.hpp"
#include "bd_cache.hpp"
#include "ld_bd.hpp"
#include "logger.hpp"
#include "thread_pool.hpp"
#include "utils.hpp"

#ifdef WIN32
#define DATA_FILE		"r:/test.dat"
#define LOG_FILE		"r:/test.log"
#else
#define DATA_FILE		"/var/ramdisk/test.dat"
#define LOG_FILE		"/var/ramdisk/test.log"
#endif

#define MAX_DEVS		32

//--------------------------------------------------------------------
// top device type
//--------------------------------------------------------------------
enum top_level_device {
	bd_pool_device,
	bd_raid0_device,
};

//--------------------------------------------------------------------
// global variables
//--------------------------------------------------------------------
uint32_t						g_block_size;
int32_t							g_loops;
boost::minstd_rand	g_rand;
int32_t							g_seed;
rmutex							g_mutex;
bool								g_bd_crash_runs;
bool								g_finished;
uint64_t						g_capacity;
uint32_t							g_devs_count;								// number of underlaying devices for
																								// bd_pool or bd_raid0 device
uint64_t						g_devs_capacity[MAX_DEVS];	// capacity of each device
top_level_device		g_dev_type;									// device type
uint32_t						g_factor;

//--------------------------------------------------------------------
// returns random value: [1..N]
//--------------------------------------------------------------------
uint32_t random_value(uint32_t n) {
	return (g_rand() % n) + 1;
}

//--------------------------------------------------------------------
// starts device tree
//--------------------------------------------------------------------
void start_devs() {
	printf("\nstarting devices\n");
	// start bd_file devices
	std::string devs_list("[");
	for (uint32_t i = 0; i < g_devs_count; i++) {
		char dev_name[32];
		char fac_name[32];
		char file_name[256];
		sprintf(dev_name, "bd_file_%u", i + 1);
		sprintf(file_name, "%s.%u", LOG_FILE, i + 1);
		bd_file::start(dev_name, g_block_size, g_devs_capacity[i], file_name);
		sprintf(fac_name, "bd_factor_%u", i + 1);
		bd_factor::start(fac_name, dev_name, g_factor, true);
		if (i > 0) {
			devs_list += ',';
		}
		devs_list += fac_name;
	}
	devs_list += "]";
	// start top level compound device
	switch (g_dev_type) {
	case bd_pool_device:
		bd_pool::start("bd_device", devs_list, 0);
		break;
	case bd_raid0_device:
		bd_raid0::start("bd_device", devs_list);
		break;
	default:
		printf("ERROR: unknown top level device type\n");
		throw test_failed();
	}
	// check that block size and capacity correctly reported by compound devices.
	block_device * pdevice = (block_device *)whereis("bd_device");
	if (g_capacity / g_factor != pdevice->capacity()) {
#ifdef WIN32
		printf("ERROR: wrong capacity %I64u reported by  bd_device, expected %I64u\n",
					 pdevice->capacity(), g_capacity / g_factor);
#else
		printf("ERROR: wrong capacity %llu reported by  bd_device, expected %llu\n",
					 pdevice->capacity(), g_capacity / g_factor);
#endif
		throw test_failed();
	}
	if (g_block_size * g_factor != pdevice->block_size()) {
		printf("ERROR: wrong block_size %u reported by  bd_device, expected %u\n",
					 pdevice->block_size(), g_block_size * g_factor);
		throw test_failed();
	}
	// start bd_crash device
	{
		scoped_lock<rmutex> lock(g_mutex);
		bd_crash::start("bd_crash", "bd_device", g_seed);
		g_bd_crash_runs = true;
	}
	// start bd_disord device
	bd_disord::start("bd_disord", "bd_crash", random_value(32), g_seed);
	// start bd_crc device
	bd_crc::start("bd_crc", "bd_disord", 0);
	// start bd_adler device
	bd_adler::start("bd_adler", "bd_crc");
	// start bd_dwrite device
	bd_dwrite::start("bd_dwrite", "bd_adler", random_value(32));
	// start bd_cache device
	bd_cache::start("bd_cache", "bd_dwrite", random_value(32));
	// start ld_bd device
	if (!ld_bd::is_formatted("bd_cache", "LD_BD_TEST")) {
		ld_bd::format("bd_cache", "LD_BD_TEST");
	}
	ld_bd::start("ld_bd", "bd_cache");
	// start logger
	blogger::start("logger", "ld_bd");
}

//--------------------------------------------------------------------
// prints statistics for devices.
//--------------------------------------------------------------------
void print_stats() {
	printf("\nstatistics\n");
	logger * plogger = (logger *)whereis("logger");
	no_stats_list list;
	plogger->stats(list);
	for (no_stats_list::const_iterator i = list.begin(); i != list.end(); i++) {
		std::string s = (*i)->to_string();
		printf("%s\n", s.c_str());
	}
}

//--------------------------------------------------------------------
// stops device tree in reverse order.
//--------------------------------------------------------------------
void stop_devs() {
	scoped_lock<rmutex> lock(g_mutex);
	g_bd_crash_runs = false;
//	print_stats();
	logger * plogger = (logger *)whereis("logger");
	if (plogger != NULL) {
		try {
			plogger->stop();
		} catch (power_failure &) {
		}
		plogger->del_underlaying();
		delete plogger;
	} else {
		// power crash may occur in start_devs and logger may be not started.
		// delete devices in reverse order in that case.
		delete whereis("ld_bd");
		delete whereis("bd_cache");
		delete whereis("bd_dwrite");
		delete whereis("bd_adler");
		delete whereis("bd_crc");
		delete whereis("bd_disord");
		delete whereis("bd_crash");
		delete whereis("bd_device");
		for (uint32_t i = 0; i < g_devs_count; i++) {
			char dev_name[32];
			sprintf(dev_name, "bd_factor_%u", i + 1);
			delete whereis(dev_name);
			sprintf(dev_name, "bd_file_%u", i + 1);
			delete whereis(dev_name);
		}
	}
}

//--------------------------------------------------------------------
// prepares buffer for writing to log.
//--------------------------------------------------------------------
void init_buffer(int32_t v, temp_buffer & buffer) {
	buffer.alloc(32 + random_value(g_block_size * 3));
	uint32_t len = sprintf(buffer.m_pdata, "{%u,%u}", v, buffer.m_size);
	memset(buffer.m_pdata + len + 1, 0xff, buffer.m_size - len - 1);
}

//--------------------------------------------------------------------
// checks buffer, returns value stored in buffer
//--------------------------------------------------------------------
int32_t check_buffer(serial_buffer & buffer) {
	if (buffer.size() <= 32) {
		printf("ERROR: buffer is less than 33 bytes: %u\n",
					 buffer.size());
		throw test_failed();
	}
	int32_t v;
	uint32_t size;
	sscanf(buffer.data(), "{%u,%u}", &v, &size);
	if (size != buffer.size()) {
		printf("ERROR: wrong buffer size: size %u, buffer size %u\n",
					 size, buffer.size());
		throw test_failed();
	}
	return v;
}

//--------------------------------------------------------------------
// reads log in forward direction
//--------------------------------------------------------------------
int32_t read_log_forward(logger * plogger) {
	uint64_t lsn = plogger->get_head();
	int32_t max_in_log = -1;
	serial_buffer buffer;
	uint64_t tail_lsn = plogger->get_tail();
	result res;
	while ((res = plogger->read_forward(buffer,lsn,lsn)) == ok) {
		if (lsn > tail_lsn) {
#ifdef WIN32
			printf("ERROR: read forward past of log: tail %I64u, lsn %I64u\n",
						 tail_lsn, lsn);
#else
			printf("ERROR: read forward past of log: tail %llu, lsn %llu\n",
						 tail_lsn, lsn);
#endif
			throw test_failed();
		}
		int32_t new_max_in_log = check_buffer(buffer);
		if (max_in_log == -1 || new_max_in_log == max_in_log + 1) {
			max_in_log = new_max_in_log;
		} else {
			printf("ERROR: read forward is out of order\n");
			throw test_failed();
		}
	}
	if (res != my_eof) {
		throw test_failed();
	}
	if (lsn != tail_lsn) {
#ifdef WIN32
		printf("ERROR: read forward: unexpected eof, tail %I64u, lsn %I64u\n",
					 tail_lsn, lsn);
#else
		printf("ERROR: read forward: unexpected eof, tail %llu, lsn %llu\n",
					 tail_lsn, lsn);
#endif
		throw test_failed();
	}
	return max_in_log;
}

//--------------------------------------------------------------------
// reads log in backward direction
//--------------------------------------------------------------------
void read_log_backward(logger * plogger, int32_t max_in_log) {
	uint64_t head_lsn = plogger->get_head();
	uint64_t lsn = plogger->get_tail();
	result res;
	serial_buffer buffer;
	while ((res = plogger->read_backward(buffer,lsn,lsn)) == ok) {
		if (lsn < head_lsn) {
#ifdef WIN32
			printf("ERROR: read backward past of log: head %I64u, lsn %I64u\n",
						 head_lsn, lsn);
#else
			printf("ERROR: read backward past of log: head %llu, lsn %llu\n",
						 head_lsn, lsn);
#endif
			throw test_failed();
		}
		if (check_buffer(buffer) != max_in_log) {
			printf("ERROR: read backward is out of order\n");
			throw test_failed();
		}
		max_in_log--;
	}
	if (res != my_eof) {
		throw test_failed();
	}
	if (lsn != head_lsn) {
		printf("ERROR: read backward: unexpected eof\n");
		throw test_failed();
	}
}

//--------------------------------------------------------------------
// sets new log head
//--------------------------------------------------------------------
void set_new_head(logger * plogger, int32_t max_in_log) {
	uint32_t count = random_value(1000);
	static uint64_t saved_head_lsn;
	uint64_t head_lsn = plogger->get_head();
	if (head_lsn < saved_head_lsn) {
#ifdef WIN32
		printf("ERROR: set_new_head: saved head %I64u is greater than logger's %I64u\n",
					 saved_head_lsn, head_lsn);
#else
		printf("ERROR: set_new_head: saved head %llu is greater than logger's %llu\n",
					 saved_head_lsn, head_lsn);
#endif
		throw test_failed();
	}
	uint64_t lsn = plogger->get_tail();
	while (count-- > 0) {
		serial_buffer buffer;
		switch (plogger->read_backward(buffer,lsn,lsn)) {
		case ok:
			if (lsn < head_lsn) {
#ifdef WIN32
				printf("ERROR: read backward past of log: head %I64u, lsn %I64u\n",
							 head_lsn, lsn);
#else
				printf("ERROR: read backward past of log: head %llu, lsn %llu\n",
							 head_lsn, lsn);
#endif
				throw test_failed();
			}
			if (check_buffer(buffer) != max_in_log) {
				printf("ERROR: read backward is out of order\n");
				throw test_failed();
			}
			break;
		case my_eof:
			if (lsn != head_lsn) {
#ifdef WIN32
				printf("ERROR: read backward: unexpected eof head %I64u, lsn %I64u\n",
							 head_lsn, lsn);
#else
				printf("ERROR: read backward: unexpected eof head %llu, lsn %llu\n",
							 head_lsn, lsn);
#endif
				throw test_failed();
			}
			return;
		default:
			throw test_failed();
		}
		max_in_log--;
	}
	plogger->set_head(lsn, lsn);
	saved_head_lsn = lsn;
}

//--------------------------------------------------------------------
// runs test
//--------------------------------------------------------------------
void run_test() {
	logger * plogger = (logger *)whereis("logger");
	uint64_t head_lsn = plogger->get_head();
	uint64_t tail_lsn = plogger->get_tail();
#ifdef WIN32
	printf("TEST: head %I64u, tail %I64u\n", head_lsn, tail_lsn);
#else
	printf("TEST: head %llu, tail %llu\n", head_lsn, tail_lsn);
#endif
	static uint64_t last_synced_lsn;
	if (tail_lsn < last_synced_lsn) {
#ifdef WIN32
		printf("ERROR: tail %I64u is less than last synced lsn %I64u\n",
					 tail_lsn, last_synced_lsn);
#else
		printf("ERROR: tail %llu is less than last synced lsn %llu\n",
					 tail_lsn, last_synced_lsn);
#endif
		throw test_failed();
	}
	static int32_t last_synced = -1;
	int32_t max_in_log = read_log_forward(plogger);
	if (max_in_log < last_synced) {
		printf("ERROR: log is not complete: last synced %d, max in log %d\n",
					 last_synced, max_in_log);
		throw test_failed();
	}
	printf("TEST: ============ loops left %d\n", g_loops - max_in_log);
	read_log_backward(plogger, max_in_log);
	while (max_in_log++ < g_loops) {
		temp_buffer buffer;
		init_buffer(max_in_log, buffer);
		uint64_t redo_lsn = UINT64_MAX;
		uint64_t lsn = plogger->write(buffer.m_pdata, buffer.m_size, redo_lsn);
		if (lsn <= redo_lsn) {
#ifdef WIN32
			printf("ERROR: invalid redo lsn %I64u, undo lsn %I64u\n",
						 redo_lsn, lsn);
#else
			printf("ERROR: invalid redo lsn %llu, undo lsn %llu\n",
						 redo_lsn, lsn);
#endif
			throw test_failed();
		}
		switch (random_value(500)) {
		case 1:
			plogger->sync(lsn);
			last_synced = max_in_log;
			last_synced_lsn = lsn;
			set_new_head(plogger, max_in_log);
			break;
		case 2:
			plogger->sync(lsn);
			last_synced = max_in_log;
			last_synced_lsn = lsn;
			break;
		}
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
		msleep(my_rand() % 1000);
		if (g_finished) {
			return;
		}
		scoped_lock<rmutex> lock(g_mutex);
		bd_crash * pcrash = (bd_crash *)whereis("bd_crash");
		if (g_bd_crash_runs) {
			pcrash->ops(false);
			printf("\n****************************** power failure %d\n\n", power_n);
			power_n++;
			g_bd_crash_runs = false;
		}
	}
}

//--------------------------------------------------------------------
// main
//--------------------------------------------------------------------
#define MIN_CAPACITY	50000

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
//	g_seed = 1268316220;
	printf("seed : %u\n", g_seed);
	g_rand.seed(g_seed);
	// skip first 10000 numbers to get better randomization
	for (int i = 0; i < 10000; i++) {
		random_value(100);
	}
	// initialize test parameters
	g_block_size = 128 + random_value(1024);
	g_loops = 200000 + random_value(1000000);
	g_capacity = MIN_CAPACITY + random_value(50000);
	g_devs_count = random_value(MAX_DEVS);
	g_factor = random_value(4);
	switch (random_value(2)) {
	case 1:
		g_dev_type = bd_pool_device;
		if (g_devs_count == 1) {
			g_devs_capacity[0] = g_capacity / g_factor * g_factor;
			g_capacity = g_devs_capacity[0];
		} else {
			uint64_t mid_capacity = g_capacity / g_devs_count * 2;
			do {
				g_capacity = 0;
				for (uint32_t i = 0; i < g_devs_count; i++) {
					uint64_t dev_capacity;
					do {
						dev_capacity = random_value((uint32_t)mid_capacity) / g_factor * g_factor;
					} while (dev_capacity == 0);
					g_devs_capacity[i] = dev_capacity;
					g_capacity += dev_capacity;
				}
			} while (g_capacity < MIN_CAPACITY);
		}
		break;
	case 2:
		g_dev_type = bd_raid0_device;
		uint64_t dev_capacity = g_capacity / g_devs_count / g_factor * g_factor;
		g_capacity = dev_capacity * g_devs_count;
		for (uint32_t i = 0; i < g_devs_count; i++) {
			g_devs_capacity[i] = dev_capacity;
		}
		break;
	}
#ifdef WIN32
	printf("block size %u, loops %u, capacity %I64u, device size %f MB\n",
				 g_block_size, g_loops, g_capacity,
				 g_capacity * g_block_size / 1024.0 / 1024.0);
#else
	printf("block size %u, loops %u, capacity %llu, device size %f MB\n",
				 g_block_size, g_loops, g_capacity,
				 g_capacity * g_block_size / 1024.0 / 1024.0);
#endif
	// execute test
	thread_pool killer;
	killer.start(1);
	killer.spawn(killer_loop);
	while (true) {
		try {
			start_devs();
			run_test();
			stop_devs();
			start_devs();
			stop_devs();
			break;
		} catch (power_failure &) {
			stop_devs();
		}
	}
	g_finished = true;
	killer.stop();
	// report ok
	printf("ok\n");
	return  0;
}

