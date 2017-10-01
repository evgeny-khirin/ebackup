///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : btree_test_perf_1.cpp
/// Author  : Evgeny Khirin <>
/// Description : B-tree performance test on top of raw disk.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <list>
#include <set>
#include <string>

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
#include "btree.hpp"

#define FILES_LIST			"/var/ramdisk/files.lst"
#define BD_FILE				"/dev/sdb"
#define FILE_SIZE		(320072933376ULL)
#define LOG_SIZE		(256ULL * 1024 * 1024)
#define CP_LOG_SIZE (LOG_SIZE / 2)

//--------------------------------------------------------------------
// global variables
//--------------------------------------------------------------------
uint32_t								g_block_size = 64 * 1024;
uint64_t								g_capacity = FILE_SIZE / g_block_size;
uint64_t								g_log_capacity =  LOG_SIZE / g_block_size;
uint64_t								g_data_capacity = g_capacity - g_log_capacity;

//--------------------------------------------------------------------
// starts device tree
//--------------------------------------------------------------------
void start_devs() {
	printf("\nstarting devices\n");
	// start bd_file device
	bd_file::start("bd_file", g_block_size, g_capacity, BD_FILE);
	// start log partition
	bd_part::start("log_part", "bd_file", 0, g_log_capacity);
	// start check sum device
	bd_crc::start("log_crc", "log_part", 0);
	// start log_cache
	bd_cache::start("log_cache", "log_crc", 2);
	// format log device
	if (!ld_bd::is_formatted("log_cache", "BTREETESTPERF1")){
		ld_bd::format("log_cache", "BTREETESTPERF1");
	}
	// ld_bd device
	ld_bd::start("ld_bd", "log_cache");
	// ld_pp device
	ld_pp::start("ld_pp", "ld_bd");
	// start logger
	blogger::start("logger", "ld_pp");
	// start transaction manager
	trans_mgr::start("tm", "logger", "bm", CP_LOG_SIZE);
	// start data partition
	bd_part::start("data_part", "bd_file", g_log_capacity, g_data_capacity);
	// start data_crc device
	ld_bd * pld_bd = (ld_bd *)whereis("ld_bd");
	printf("SYSTEM GENEARTION: %llu\n", pld_bd->system_generation());
	bd_crc::start("data_crc", "data_part", pld_bd->system_generation());
	// start buffer manager
	buffer_mgr::start("bm", "data_crc", "tm", 512);
	// start STM
	stm::start("stm", "tm", "bm");
	// start B-tree resource manager
	btree_rm::start("btree_rm", "tm", "bm");
	// recover TM
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	printf("\nrecovery started, log size %llu MB\n", tm->log_size() / 1024 / 1024);
	struct timeval tv;
	double start, end;
	gettimeofday(&tv, NULL);
	start = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	tm->recover();
	gettimeofday(&tv, NULL);
	end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	printf("recovery finished, time %f\n\n", end - start);
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
	// stop TM
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	tm->stop();
	// stop BM
	buffer_mgr * bm = (buffer_mgr *)whereis("bm");
	bm->stop();
	// stop B-tree RM
	btree_rm * pbtree_rm = (btree_rm *)whereis("btree_rm");
	pbtree_rm->stop();
	// stop STM
	stm * pstm = (stm *)whereis("stm");
	pstm->stop();
	// Delete TM
	tm->del_underlaying();
	delete tm;
	// Delete BM
	bm->del_underlaying();
	delete bm;
	// Delete B-tree RM
	pbtree_rm->del_underlaying();
	delete pbtree_rm;
	// Delete STM
	pstm->del_underlaying();
	delete pstm;
}

//--------------------------------------------------------------------
// data types
//--------------------------------------------------------------------
typedef std::set<std::string> str_set;

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool is_member(std::string const & s, str_set const & l) {
	for (str_set::const_iterator i = l.begin(); i != l.end(); i++) {
		if (s == *i) {
			return true;
		}
	}
	return false;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool is_dir(std::string const & s) {
	struct stat st;
	if (stat(s.c_str(), &st) != 0) {
		return false;
	}
	return S_ISDIR(st.st_mode);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void list_dir(std::string const & s, str_set & result) {
	DIR * dir =	opendir(s.c_str());
	if (dir == NULL) {
		return;
	}
	dirent * entry;
	while ((entry = readdir(dir)) != NULL) {
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
			continue;
		}
		if (s[s.size() - 1] == '/') {
			result.insert(s + entry->d_name);
		} else {
			result.insert(s + '/' + entry->d_name);
		}
	}
	closedir(dir);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void build_files_list_rec(FILE * f, str_set const & sources,
													str_set const & excludes, str_set & result) {
	for (str_set::const_iterator i = sources.begin(); i != sources.end(); i++) {
		std::string const & s = *i;
		if (is_member(s, excludes)) {
			continue;
		}
		fprintf(f, "%s\n", s.c_str());
		result.insert(s);
		if (is_dir(s)) {
			str_set dir;
			list_dir(s,dir);
			build_files_list_rec(f, dir,excludes,result);
		}
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void build_files_list(str_set const & sources, str_set const & excludes, str_set & result) {
	FILE * f = fopen(FILES_LIST, "w");
	build_files_list_rec(f, sources, excludes, result);
	fclose(f);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void read_files_list(str_set & result) {
	char buf[1024];
	FILE * f = fopen(FILES_LIST, "r");
	while (fgets(buf, sizeof(buf), f) != NULL) {
		std::string s(buf, strlen(buf) - 1);
		result.insert(s);
	}
	fclose(f);
}

//--------------------------------------------------------------------
// Creates integer term
//--------------------------------------------------------------------
term_ptr encode_term(int32_t value) {
	char buf[5];
	buf[0] = 98;
	*((int32_t *)(buf + 1)) = htonl(value);
	term * t = new term(buf, 5);
	return term_ptr(t);
}

//--------------------------------------------------------------------
// Decodes integer term
//--------------------------------------------------------------------
int32_t decode_term(const term_ptr & v) {
	const unsigned char * buf = (const unsigned char *)v->data();
	if (buf[0] != 98) {
		printf("ERROR: invalid term signature\n");
		throw test_failed();
	}
	return ntohl(*((int32_t *)(buf + 1)));
}

//--------------------------------------------------------------------
// Creates string term
//--------------------------------------------------------------------
term_ptr encode_term(const std::string & value) {
	unsigned char buf[1024];
	if (value.length() > sizeof(buf) - 3) {
		throw test_failed();
	}
	buf[0] = 109;	// Encode strings as binary for better performance
	*((int32_t *)(buf + 1)) = htonl(value.length());
	memcpy(buf + 5, value.c_str(), value.length());
	term * t = new term(buf, value.length() + 5);
	return term_ptr(t);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool lookup(btree * tree, std::string const & k, int32_t * pv) {
	term_ptr v;
	result res = tree->lookup(encode_term(k), v);
	if (res == not_found) {
		return false;
	}
	*pv = decode_term(v);
	return true;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void insert(btree * tree, std::string const & k, int32_t v) {
	tree->insert(encode_term(k), encode_term(v));
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void insert(btree * tree, int32_t k, std::string const & v) {
	tree->insert(encode_term(k), encode_term(v));
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void remove(btree * tree, std::string const & k) {
	tree->remove(encode_term(k));
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void remove(btree * tree, int32_t k) {
	tree->remove(encode_term(k));
}

//--------------------------------------------------------------------
// runs test
//--------------------------------------------------------------------
void run_test() {
	str_set files;
	if (access(FILES_LIST, F_OK) == 0) {
		read_files_list(files);
	} else {
		str_set sources;
		str_set excludes;
		sources.insert("/");
		excludes.insert("/sys");
		excludes.insert("/proc");
		excludes.insert("/dev");
		build_files_list(sources,excludes,files);
	}
	printf("files %d\n", files.size());

	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	std::auto_ptr<btree> tree(btree::create(tm, bm, pstm, pbtree_rm));
	printf("================================= btree root %llu\n", tree->root());
	struct timeval tv;
	double global_start;
	double start, end;
	int loop;

	gettimeofday(&tv, NULL);
	global_start = start = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	loop = 0;
	for (str_set::const_iterator i = files.begin(); i != files.end(); i++, loop++) {
		std::string const & s = *i;
		int32_t id;
		if (lookup(tree.get(), s, &id)) {
			printf("unexpected found %s, id %d\n", s.c_str(), id);
			throw test_failed();
		}
		if (!lookup(tree.get(), "free", &id)) {
			id = 0;
		}
		insert(tree.get(), "free", id + 1);
		insert(tree.get(), s, id);
		insert(tree.get(), id, s);
		if (loop % 10000 == 0) {
			gettimeofday(&tv, NULL);
			end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
			printf("insert %d %f, id %d\n", loop, end - start, id);
			fflush(stdout);
			start = end;
		}
	}
	gettimeofday(&tv, NULL);
	end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	printf("===================================== insert time %f\n", end - global_start);

	gettimeofday(&tv, NULL);
	global_start = start = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	loop = 0;
	for (str_set::const_iterator i = files.begin(); i != files.end(); i++, loop++) {
		std::string const & s = *i;
		int32_t id;
		if (!lookup(tree.get(), s, &id)) {
			printf("lookup failed for %s\n", s.c_str());
			throw test_failed();
		}
		if (id != loop) {
			printf("found wrong id %d for %s\n", id, s.c_str());
			throw test_failed();
		}
		if (loop % 10000 == 0) {
			gettimeofday(&tv, NULL);
			end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
			printf("lookup %d %f, id %d\n", loop, end - start, id);
			fflush(stdout);
			start = end;
		}
	}
	gettimeofday(&tv, NULL);
	end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	printf("===================================== lookup time %f\n", end - global_start);

	gettimeofday(&tv, NULL);
	global_start = start = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	loop = 0;
	uint32_t id = tm->begin_transaction(read_only);
	for (btree::iterator i = tree->begin(); i.has_more(); i++, loop++) {
		if (loop % 10000 == 0) {
			gettimeofday(&tv, NULL);
			end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
			printf("cursor scan %d %f\n", loop, end - start);
			fflush(stdout);
			start = end;
		}
	}
	tm->commit(id);
	gettimeofday(&tv, NULL);
	end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	printf("===================================== cursor scan time %f\n", end - global_start);

	gettimeofday(&tv, NULL);
	global_start = start = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	remove(tree.get(), "free");
	loop = 0;
	for (str_set::const_iterator i = files.begin(); i != files.end(); i++, loop++) {
		std::string const & s = *i;
		remove(tree.get(), s);
		remove(tree.get(), loop);
		if (loop % 10000 == 0) {
			gettimeofday(&tv, NULL);
			end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
			printf("remove %d %f\n", loop, end - start);
			fflush(stdout);
			start = end;
		}
	}
	gettimeofday(&tv, NULL);
	end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	printf("===================================== remove time %f\n", end - global_start);
}

//--------------------------------------------------------------------
// main
//--------------------------------------------------------------------
int main() {
	// run test
	start_devs();
	run_test();
	stop_devs();
	// report ok
	printf("ok\n");
	return  0;
}

