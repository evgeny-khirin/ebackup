///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : stm_test_perf.cpp
/// Author  : Evgeny Khirin <>
/// Description : STM performance test.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <boost/random/linear_congruential.hpp>
#include <vector>

#include "bd_file.hpp"
#include "bd_factor.hpp"
#include "bd_null.hpp"
#include "bd_part.hpp"
#include "bd_adler.hpp"
#include "bd_crc.hpp"
#include "bd_dwrite.hpp"
#include "bd_cache.hpp"
#include "ld_bd.hpp"
#include "ld_pp.hpp"
#include "logger.hpp"
#include "nlogger.hpp"
#include "tm.hpp"
#include "bm.hpp"
#include "stm.hpp"

#define FILE				"/dev/sdb"
#define FILE_SIZE		(320072933376ULL)
#define LOG_SIZE		(1ULL * 1024 * 1024 * 1024)
#define CP_LOG_SIZE (256ULL * 1024 * 1024)

//--------------------------------------------------------------------
// global variables
//--------------------------------------------------------------------
uint32_t								g_block_size = 4096;
int32_t									g_loops = 3;
uint64_t								g_capacity = FILE_SIZE / g_block_size;
uint64_t								g_log_capacity =  LOG_SIZE / g_block_size;
uint64_t								g_data_capacity = g_capacity - g_log_capacity;
uint32_t								g_factor = 16;
std::vector<uint32_t>		g_free_set(g_data_capacity / g_factor);
boost::minstd_rand			g_rand;
int32_t									g_seed;

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
	// start bd_file device
	bd_file::start("bd_file", g_block_size, g_capacity, FILE);
	bd_factor::start("bd_factor", "bd_file", g_factor, true);
	// start log partition
	bd_part::start("log_part", "bd_file", 0, g_log_capacity);
	// start check sum device
	bd_crc::start("log_crc", "log_part", 0);
	// start log_dwrite
	bd_dwrite::start("log_dwrite", "log_crc", 16);
	// format log device
	ld_bd::format("log_dwrite", "STM_TEST_PERF");
	// ld_bd device
	ld_bd::start("ld_bd", "log_dwrite");
	// ld_pp device
	ld_pp::start("ld_pp", "ld_bd");
	// start logger
	blogger::start("logger", "ld_pp");
	// start transaction manager
	trans_mgr::start("tm", "logger", "bm", CP_LOG_SIZE);
	// start data partition
	bd_part::start("data_part", "bd_factor", g_log_capacity / g_factor, g_data_capacity / g_factor);
	// start data_crc device
	ld_bd * pld_bd = (ld_bd *)whereis("ld_bd");
	bd_crc::start("data_crc", "data_part", pld_bd->system_generation());
	// start data_dwrite
	bd_dwrite::start("data_dwrite", "data_crc", 64);
	// start buffer manager
	buffer_mgr::start("bm", "data_dwrite", "tm", 1024);
	// start STM
	stm::start("stm", "tm", "bm");
	// recover TM
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	tm->recover();
}

//--------------------------------------------------------------------
// prints statistics for devices.
//--------------------------------------------------------------------
void print_stats() {
	printf("\nstatistics\n");
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	buffer_mgr * bm = (buffer_mgr *)whereis("bm");
	no_stats_list list;
	block::stats(list);
	tm->stats(list);
	bm->stats(list);
	for (no_stats_list::const_iterator i = list.begin(); i != list.end(); i++) {
		std::string s = (*i)->to_string();
		printf("%s\n", s.c_str());
	}
}

//--------------------------------------------------------------------
// stops device tree in reverse order.
//--------------------------------------------------------------------
void stop_devs() {
	print_stats();
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	tm->stop();
	stm * pstm = (stm *)whereis("stm");
	pstm->stop();
	buffer_mgr * bm = (buffer_mgr *)whereis("bm");
	bm->stop();
	pstm->del_underlaying();
	delete pstm;
	bm->del_underlaying();
	delete bm;
	tm->del_underlaying();
}

//--------------------------------------------------------------------
// init free set
//--------------------------------------------------------------------
void init_free_set() {
	uint32_t set_size = g_data_capacity / g_factor;
	for (uint32_t i = 0; i < set_size; i++) {
		g_free_set[i] = i;
	}
}

//--------------------------------------------------------------------
// shuffle free set
//--------------------------------------------------------------------
void shuffle_free_set() {
	for (uint32_t i = 0; i < g_free_set.size(); i++) {
		uint32_t j = random_value_0(g_free_set.size());
		std::swap(g_free_set[i], g_free_set[j]);
	}
}

//--------------------------------------------------------------------
// alloc whole device
//--------------------------------------------------------------------
void alloc_device(stm * pstm) {
	uint64_t capacity = g_data_capacity / g_factor;
	while(!pstm->is_full()) {
		pstm->alloc();
		static unsigned prev_usage = (unsigned)-1;
		unsigned usage = (unsigned)((double)pstm->used() / capacity * 100);
		if (usage != prev_usage && usage % 10 == 0) {
			printf("device usage %u %%\n", usage);
			prev_usage = usage;
		}
	}
}

//--------------------------------------------------------------------
// free whole device
//--------------------------------------------------------------------
void free_device(stm * pstm) {
	uint64_t capacity = g_data_capacity / g_factor;
	for (uint32_t i = 0; i < g_free_set.size(); i++) {
		pstm->free(g_free_set[i]);
		static unsigned prev_usage = (unsigned)-1;
		unsigned usage = (unsigned)((double)pstm->used() / capacity * 100);
		if (usage != prev_usage && usage % 10 == 0) {
			printf("device usage %u %%\n", usage);
			prev_usage = usage;
		}
	}
}

//--------------------------------------------------------------------
// runs test
//--------------------------------------------------------------------
void run_test() {
	stm * pstm = (stm *)whereis("stm");
	init_free_set();
	while (g_loops > 0) {
		alloc_device(pstm);
		shuffle_free_set();
		free_device(pstm);
		g_loops--;
	}
}

//--------------------------------------------------------------------
// main
//--------------------------------------------------------------------
int main() {
	// initialize random namber generator
	g_seed = time(NULL);
//	g_seed = 1268570804;
	printf("seed : %u\n", g_seed);
	g_rand.seed(g_seed);
	// skip first 10000 numbers to get better randomization
	for (int i = 0; i < 10000; i++) {
		random_value_0(100);
	}
	// run test
	start_devs();
	run_test();
	stop_devs();
	// report ok
	printf("ok\n");
	return  0;
}

