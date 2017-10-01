///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : btree_test_1.cpp
/// Author  : Evgeny Khirin <>
/// Description : Unit test for B-tree. Test checks correct program behaivior
/// with many devices in case of power failures and does not measure performance.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#ifdef _WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#include <sys/time.h>
#endif
#include <boost/random/linear_congruential.hpp>
#include <map>
#include <set>

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
#include "ld_pp.hpp"
#include "logger.hpp"
#include "tm.hpp"
#include "bm.hpp"
#include "stm.hpp"
#include "btree.hpp"
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
#define MIN_CAPACITY	20000

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
uint64_t						g_log_devs_capacity[MAX_DEVS];	// capacity of each device
uint64_t						g_data_devs_capacity[MAX_DEVS];	// capacity of each device
top_level_device		g_log_dev_type;							// log device type
top_level_device		g_data_dev_type;						// data device type
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
	switch (g_log_dev_type) {
	case bd_pool_device:
		bd_pool::start("log_bd_device", devs_list, 0);
		break;
	case bd_raid0_device:
		bd_raid0::start("log_bd_device", devs_list);
		break;
	default:
		printf("ERROR: unknown top level device type\n");
		throw test_failed();
	}
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
	if (!ld_bd::is_formatted("log_bd_cache", "BTREE_TEST_1")) {
		ld_bd::format("log_bd_cache", "BTREE_TEST_1");
	}
	ld_bd::start("ld_bd", "log_bd_cache");
	// start ld_pp device
	ld_pp::start("ld_pp", "ld_bd");
	// start logger
	blogger::start("logger", "ld_pp");
	// start transaction manager
	uint64_t cp_log_size = MIN_CAPACITY / 16 * g_log_block_size;
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
	switch (g_data_dev_type) {
	case bd_pool_device:
		bd_pool::start("data_bd_device", devs_list, 0);
		break;
	case bd_raid0_device:
		bd_raid0::start("data_bd_device", devs_list);
		break;
	default:
		printf("ERROR: unknown top level device type\n");
		throw test_failed();
	}
	// check that block size and capacity correctly reported by compound devices.
	pdevice = (block_device *)whereis("data_bd_device");
	if (g_data_capacity / g_data_factor != pdevice->capacity()) {
		printf("ERROR: wrong capacity %llu reported by data_bd_device, expected %llu\n",
					 pdevice->capacity(), g_data_capacity / g_data_factor );
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
	// start B-tree resource manager
	btree_rm::start("btree_rm", "tm", "bm");
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
	// stop TM
	trans_mgr * tm = (trans_mgr *)whereis("tm");
	if (do_stop && tm != NULL) {
		tm->stop();
	}
	// Stop BM
	buffer_mgr * bm = (buffer_mgr *)whereis("bm");
	if (do_stop && bm != NULL) {
		bm->stop();
	}
	// stop B-tree RM
	btree_rm * pbtree_rm = (btree_rm *)whereis("btree_rm");
	if (pbtree_rm != NULL) {
		if (do_stop) {
			pbtree_rm->stop();
		}
		pbtree_rm->del_underlaying();
		delete pbtree_rm;
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
#define MAX_KEY				5000
struct descr_t {
	uint32_t	m_key;
	uint32_t	m_key_len;
	uint32_t	m_val;
	uint32_t	m_val_len;
};
typedef std::map<uint32_t, descr_t>	map_t;
map_t			g_map;
uint64_t	g_root = UINT64_MAX;
uint64_t	g_used;

//--------------------------------------------------------------------
// returns max term len
//--------------------------------------------------------------------
uint32_t term_len(uint32_t max_chunk_size) {
	return 6 + random_value_0(max_chunk_size / 2 - 5 - 6);
}

//--------------------------------------------------------------------
// performs insert operation
//--------------------------------------------------------------------
void test_create() {
	if (g_root != UINT64_MAX) {
		return;
	}
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	uint64_t			used = pstm->used();
	uint32_t			id = tm->begin_transaction(hard);
	std::auto_ptr<btree> tree(btree::create(tm, bm, pstm, pbtree_rm));
	tm->rollback(id);
	if (used != pstm->used()) {
		printf("ERROR: storage leak %lld blocks\n", pstm->used() - used);
		throw test_failed();
	}
	id = tm->begin_transaction(hard);
	tree.reset(btree::create(tm, bm, pstm, pbtree_rm));
	tm->commit(id);
	g_root = tree->root();
}

//--------------------------------------------------------------------
// Creates integer term
//--------------------------------------------------------------------
term_ptr encode_term(int32_t value, uint32_t len) {
	char buf[4096];
	if (len >= sizeof(buf) - 5 || len < 6) {
		printf("INTERNAL TEST ERROR: invalid term length\n");
		throw test_failed();
	}
	buf[0] = 109;
	*((int32_t *)(buf + 1)) = htonl(len);
	memset(buf + 5, 0, len);
	sprintf(buf + 5, "%5u", value);
	term * t = new term(buf, len + 5);
	return term_ptr(t);
}

//--------------------------------------------------------------------
// Decodes integer term
//--------------------------------------------------------------------
uint32_t decode_term(const term_ptr & v, uint32_t & len) {
	const char * buf = v->data();
	if (buf[0] != 109) {
		printf("ERROR: invalid term signature\n");
		throw test_failed();
	}
	len = ntohl(*((int32_t *)(buf + 1)));
	uint32_t value;
	sscanf(buf + 5, "%5u", &value);
	return value;
}

//--------------------------------------------------------------------
// performs insert operation
//--------------------------------------------------------------------
void test_insert(descr_t & last_descr) {
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	uint32_t			id = tm->begin_transaction(hard);
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	descr_t				descr;
	descr.m_key = random_value_0(MAX_KEY);
	descr.m_val = random_value_0(MAX_KEY);
	descr.m_val_len = term_len(tree->max_chunk_size());
	map_t::iterator i = g_map.find(descr.m_key);
	if (i == g_map.end()) {
		descr.m_key_len = term_len(tree->max_chunk_size());
	} else {
		descr.m_key_len = i->second.m_key_len;
	}
//	printf("^^^^^^^^^^^^^^ test_insert: {%u, %u}\n", descr.m_key, descr.m_key_len);
	tree->insert(encode_term(descr.m_key, descr.m_key_len),
							 encode_term(descr.m_val, descr.m_val_len));
	if (random_value_1(10) == 1) {
//		printf("^^^^^^^^^^^^^^ test_insert: rollback started\n");
		tm->rollback(id);
//		printf("^^^^^^^^^^^^^^ test_insert: rollback finished\n");
		return;
	}
	// Commit is not atomic. It is guaranteed that if commit succeeded, than data are in place.
	// If commit is failed, than data may be in place as well. So save it for latter correction.
	last_descr = descr;
	tm->commit(id);
//	printf("^^^^^^^^^^^^^^ test_insert: commited\n");
	last_descr.m_key = UINT32_MAX;
	g_map[descr.m_key] = descr;
}

//--------------------------------------------------------------------
// performs lookup operation
//--------------------------------------------------------------------
void test_lookup() {
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	uint32_t 			k = random_value_0(MAX_KEY);
	map_t::iterator i = g_map.find(k);
	uint32_t			k_len;
	if (i == g_map.end()) {
		k_len = term_len(tree->max_chunk_size());
	} else {
		k_len = i->second.m_key_len;
	}
	term_ptr			v;
	result				res = tree->lookup(encode_term(k, k_len), v);
	switch (res) {
	case not_found:
		if (i != g_map.end()) {
			printf("ERROR: existing key {%u, %u} not found\n", k, k_len);
			throw test_failed();
		}
		break;
	case ok:
		{
			if (i == g_map.end()) {
				printf("ERROR: found non-existing key\n");
				throw test_failed();
			}
			const descr_t & descr = i->second;
			uint32_t v_len;
			if (descr.m_val != decode_term(v, v_len)) {
				printf("ERROR: found key with invalid value\n");
				throw test_failed();
			}
			if (v_len != descr.m_val_len) {
				printf("ERROR: found key with invalid value length\n");
				throw test_failed();
			}
		}
		break;
	default:
		throw test_failed();
	}
}

//--------------------------------------------------------------------
// performs seek operation
//--------------------------------------------------------------------
void test_seek_prev() {
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	uint32_t 			k = random_value_0(MAX_KEY);
	uint32_t			k_len;
	if (g_map.find(k) == g_map.end()) {
		k_len = term_len(tree->max_chunk_size());
	} else {
		k_len = g_map[k].m_key_len;
	}
	map_t::iterator map_it = g_map.lower_bound(k);
	if (map_it == g_map.begin()) {
		map_it = g_map.end();
	} else {
		map_it--;
		assert(map_it->first < k);
	}
	btree::iterator tree_it = tree->seek(encode_term(k, k_len), prev);
	if (map_it == g_map.end()) {
		if (tree_it.has_more()) {
			printf("ERROR: found non existing prev key for {%u, %u}\n", k, k_len);
			throw test_failed();
		}
		return;
	}
	if (!tree_it.has_more()) {
		printf("ERROR: not found prev key for {%u, %u}\n", k, k_len);
		throw test_failed();
	}
	const descr_t & descr = map_it->second;
	const term_ptr & tree_k = tree_it->first;
	const term_ptr & tree_v = (*tree_it).second;
	uint32_t len;
	uint32_t val = decode_term(tree_k, len);
	if (descr.m_key != val || descr.m_key_len != len) {
		printf("ERROR: found invalid prev key {%u, %u} instead {%u, %u}\n",
					 val, len, descr.m_key, descr.m_key_len);
		throw test_failed();
	}
	val = decode_term(tree_v, len);
	if (descr.m_val != val || descr.m_val_len != len) {
		printf("ERROR: found invalid prev value {%u, %u} instead {%u, %u}\n",
					 val, len, descr.m_val, descr.m_val_len);
		throw test_failed();
	}
}

//--------------------------------------------------------------------
// performs seek operation
//--------------------------------------------------------------------
void test_seek_next() {
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	uint32_t 			k = random_value_0(MAX_KEY);
	uint32_t			k_len;
	if (g_map.find(k) == g_map.end()) {
		k_len = term_len(tree->max_chunk_size());
	} else {
		k_len = g_map[k].m_key_len;
	}
	map_t::iterator map_it = g_map.upper_bound(k);
	btree::iterator tree_it = tree->seek(encode_term(k, k_len), next);
	if (map_it == g_map.end()) {
		if (tree_it.has_more()) {
			printf("ERROR: found non existing next key for {%u, %u}\n", k, k_len);
			throw test_failed();
		}
		return;
	}
	assert(map_it->first > k);
	if (!tree_it.has_more()) {
		printf("ERROR: not found next key for {%u, %u}\n", k, k_len);
		throw test_failed();
	}
	const descr_t & descr = map_it->second;
	const term_ptr & tree_k = tree_it->first;
	const term_ptr & tree_v = (*tree_it).second;
	uint32_t len;
	uint32_t val = decode_term(tree_k, len);
	if (descr.m_key != val || descr.m_key_len != len) {
		printf("ERROR: found invalid next key {%u, %u} instead {%u, %u}\n",
					 val, len, descr.m_key, descr.m_key_len);
		throw test_failed();
	}
	val = decode_term(tree_v, len);
	if (descr.m_val != val || descr.m_val_len != len) {
		printf("ERROR: found invalid next value {%u, %u} instead {%u, %u}\n",
					 val, len, descr.m_val, descr.m_val_len);
		throw test_failed();
	}
}

//--------------------------------------------------------------------
// performs seek operation
//--------------------------------------------------------------------
void test_seek_exact() {
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	uint32_t 			k = random_value_0(MAX_KEY);
	uint32_t			k_len;
	if (g_map.find(k) == g_map.end()) {
		k_len = term_len(tree->max_chunk_size());
	} else {
		k_len = g_map[k].m_key_len;
	}
	map_t::iterator map_it = g_map.find(k);
	btree::iterator tree_it = tree->seek(encode_term(k, k_len), exact);
	if (map_it == g_map.end()) {
		if (tree_it.has_more()) {
			printf("ERROR: found non existing exact key for {%u, %u}\n", k, k_len);
			throw test_failed();
		}
		return;
	}
	assert(map_it->first == k);
	if (!tree_it.has_more()) {
		printf("ERROR: not found exact key for {%u, %u}\n", k, k_len);
		throw test_failed();
	}
	const descr_t & descr = map_it->second;
	const term_ptr & tree_k = tree_it->first;
	const term_ptr & tree_v = (*tree_it).second;
	uint32_t len;
	uint32_t val = decode_term(tree_k, len);
	if (descr.m_key != val || descr.m_key_len != len) {
		printf("ERROR: found invalid exact key {%u, %u} instead {%u, %u}\n",
					 val, len, descr.m_key, descr.m_key_len);
		throw test_failed();
	}
	val = decode_term(tree_v, len);
	if (descr.m_val != val || descr.m_val_len != len) {
		printf("ERROR: found invalid exact value {%u, %u} instead {%u, %u}\n",
					 val, len, descr.m_val, descr.m_val_len);
		throw test_failed();
	}
}

//--------------------------------------------------------------------
// performs remove operation
//--------------------------------------------------------------------
void test_remove(descr_t & last_descr) {
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	uint32_t			id = tm->begin_transaction(hard);
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	int32_t 			k = random_value_0(MAX_KEY);
	map_t::iterator i = g_map.find(k);
	uint32_t			k_len;
	if (i == g_map.end()) {
		k_len = term_len(tree->max_chunk_size());
	} else {
		k_len = i->second.m_key_len;
	}
//	printf("^^^^^^^^^^^^^^ test_remove: {%u, %u}\n", k, k_len);
	tree->remove(encode_term(k, k_len));
	term_ptr			v;
	result				res = tree->lookup(encode_term(k, k_len), v);
	if (res != not_found) {
		printf("ERROR: remove does not work\n");
		throw test_failed();
	}
	if (random_value_1(10) == 1) {
//		printf("^^^^^^^^^^^^^^ test_remove: rollback started\n");
		tm->rollback(id);
//		printf("^^^^^^^^^^^^^^ test_remove: rollback finished\n");
		return;
	}
	if (i != g_map.end()) {
		// Commit is not atomic. It is guaranteed that if commit succeeded, than data are in place.
		// If commit is failed, than data may be in place as well. So save it for latter correction.
		last_descr.m_key = k;
		last_descr.m_key_len = k_len;
	}
	tm->commit(id);
//	printf("^^^^^^^^^^^^^^ test_remove: commited\n");
	last_descr.m_key = UINT32_MAX;
	g_map.erase(k);
}

//--------------------------------------------------------------------
// clears tree in random order
//--------------------------------------------------------------------
void clear_tree() {
	static map_t::iterator	map_array[MAX_KEY];
	uint32_t			size = 0;
	// copy keys
	for (map_t::iterator i = g_map.begin(); i != g_map.end(); i++) {
		map_array[size++] = i;
	}
	// shuffle keys
	for (uint32_t i = 0; i < size; i++) {
		std::swap(map_array[i], map_array[random_value_0(size)]);
	}
	// delete all keys
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	uint32_t			id = tm->begin_transaction(hard);
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	for (uint32_t i = 0; i < size; i++) {
		const descr_t descr = map_array[i]->second;
		term_ptr k = encode_term(descr.m_key, descr.m_key_len);
		term_ptr v;
		result res = tree->lookup(k, v);
		if (res != ok) {
			printf("ERROR: existing key not found\n");
			throw test_failed();
		}
		uint32_t v_len;
		if (descr.m_val != decode_term(v, v_len)) {
			printf("ERROR: found key with invalid value\n");
			throw test_failed();
		}
		if (descr.m_val_len != v_len) {
			printf("ERROR: found key with invalid value length\n");
			throw test_failed();
		}
		tree->remove(k);
		res = tree->lookup(k, v);
		if (res != not_found) {
			printf("ERROR: remove does not work\n");
			throw test_failed();
		}
	}
	if (g_used + 1 != pstm->used()) {
		printf("ERROR: storage leak %lld blocks\n", pstm->used() - g_used - 1);
		throw test_failed();
	}
	tm->commit(id);
}

//--------------------------------------------------------------------
// scans whole tree
//--------------------------------------------------------------------
void scan_tree() {
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	for (map_t::iterator i = g_map.begin(); i != g_map.end(); i++) {
		const descr_t & descr = i->second;
		term_ptr k = encode_term(descr.m_key, descr.m_key_len);
		term_ptr			v;
		result				res = tree->lookup(k, v);
		switch (res) {
		case not_found:
			printf("ERROR: existing key {%u, %u} not found\n",
						 descr.m_key, descr.m_key_len);
			throw test_failed();
		case ok:
			{
				uint32_t v_len;
				if (descr.m_val != decode_term(v, v_len)) {
					printf("ERROR: found key {%u, %u} with invalid value\n",
								 descr.m_key, descr.m_key_len);
					throw test_failed();
				}
				if (descr.m_val_len != v_len) {
					printf("ERROR: found key {%u, %u} with invalid value length\n",
								 descr.m_key, descr.m_key_len);
					throw test_failed();
				}
			}
			break;
		default:
			throw test_failed();
		}
	}
}

//--------------------------------------------------------------------
// scans whole tree with iterator
//--------------------------------------------------------------------
void scan_tree_it() {
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	uint32_t			id = tm->begin_transaction(read_only);
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	btree::iterator tree_it = tree->begin();
	map_t::iterator map_it = g_map.begin();
	for (; map_it != g_map.end() && tree_it.has_more(); map_it++, tree_it++) {
		const descr_t & descr = map_it->second;
		const term_ptr & k = tree_it->first;
		const term_ptr & v = (*tree_it).second;
		uint32_t len;
		uint32_t val = decode_term(k, len);
		if (descr.m_key != val || descr.m_key_len != len) {
			printf("ERROR: found invalid key {%u, %u} instead {%u, %u}\n",
						 val, len, descr.m_key, descr.m_key_len);
			throw test_failed();
		}
		val = decode_term(v, len);
		if (descr.m_val != val || descr.m_val_len != len) {
			printf("ERROR: found invalid value {%u, %u} instead {%u, %u}\n",
						 val, len, descr.m_val, descr.m_val_len);
			throw test_failed();
		}
	}
	if (map_it != g_map.end()) {
		printf("ERROR: tree iterator is not complete\n");
		throw test_failed();
	}
	if (tree_it.has_more()) {
		printf("ERROR: tree iterator is too long\n");
		throw test_failed();
	}
	tm->commit(id);
	return;
}

//--------------------------------------------------------------------
// runs test
//--------------------------------------------------------------------
static void run_test() {
	static descr_t last_descr;
	static int last_action = 0;
	trans_mgr * 	tm = (trans_mgr *)whereis("tm");
	buffer_mgr *	bm = (buffer_mgr *)whereis("bm");
	stm *					pstm = (stm *)whereis("stm");
	btree_rm *		pbtree_rm = (btree_rm *)whereis("btree_rm");
	printf("TEST: ============ loops left %d, B-tree size %u, device usage %f%%\n",
				 g_loops, g_map.size(),
				 (double)pstm->used() / (g_data_capacity / g_data_factor) * 100);
	test_create();
	bool fisrt_time = true;
	static bool finish_clear = false;
	while (g_loops > 0) {
		if (g_loops % 10000 == 0) {
			printf("loops left %d, B-tree size %u, device usage %f%%\n",
						 g_loops, g_map.size(),
						 (double)pstm->used() / (g_data_capacity / g_data_factor) * 100);
		}
		if (finish_clear) {
			printf("++++++++++++++ finish fast clean B-tree started\n");
			std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
			tree->clear();
			uint32_t 	id = tm->begin_transaction(hard);
			tm->commit(id);
			finish_clear = false;
			if (g_used + 1 != pstm->used()) {
				printf("ERROR: storage leak %lld blocks\n", pstm->used() - g_used - 1);
				throw test_failed();
			}
			printf("++++++++++++++ fast clean B-tree finished\n");
		}
		if (fisrt_time) {
			fisrt_time = false;
			// Commit is not atomic. It is guaranteed that if commit succeeded, than data are in place.
			// If commit is failed, than data may be in place as well. So use saved to to fix test.
			if (last_action == 1 && last_descr.m_key != UINT32_MAX) {
				// last action was insert with power failed commit.
				std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
				term_ptr			v;
				if (tree->lookup(encode_term(last_descr.m_key, last_descr.m_key_len), v) == ok) {
					uint32_t v_len;
					if (decode_term(v, v_len) == last_descr.m_val && v_len == last_descr.m_val_len) {
						g_map[last_descr.m_key] = last_descr;
					}
				}
			}
			if (last_action == 2 && last_descr.m_key != UINT32_MAX) {
				// last action was remove with power failed commit.
				std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
				term_ptr			v;
				if (tree->lookup(encode_term(last_descr.m_key, last_descr.m_key_len), v) == not_found) {
					g_map.erase(last_descr.m_key);
				}
			}
		}
		if (random_value_1(5000) == 1) {
			printf("++++++++++++++ fast clean B-tree started\n");
			finish_clear = true;
			g_map.clear();
			std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
			tree->clear();
			uint32_t 	id = tm->begin_transaction(hard);
			tm->commit(id);
			finish_clear = false;
			if (g_used + 1 != pstm->used()) {
				printf("ERROR: storage leak %lld blocks\n", pstm->used() - g_used - 1);
				throw test_failed();
			}
			printf("++++++++++++++ fast B-tree clean finished\n");
		}
		if (random_value_1(5000) == 1) {
			uint32_t 	id = tm->begin_transaction(hard);
			printf("++++++++++++++ start slow B-tree clean\n");
			clear_tree();
			if (random_value_1(3) == 1) {
				tm->rollback(id);
				printf("++++++++++++++ slow B-tree clean rolled back\n");
			} else {
				tm->commit(id);
				printf("++++++++++++++ slow B-tree clean commited\n");
				g_map.clear();
			}
		}
		if (random_value_1(5000) == 1) {
			printf("++++++++++++++ B-tree scan started\n");
			scan_tree();
			printf("++++++++++++++ B-tree scan finished\n");
		}
		if (random_value_1(5000) == 1) {
			printf("++++++++++++++ B-tree scan iterator started\n");
			scan_tree_it();
			printf("++++++++++++++ B-tree scan iterator finished\n");
		}
		last_descr.m_key = UINT32_MAX;
		last_action = 1;
		test_insert(last_descr);
		last_action = 0;
		test_lookup();
		last_action = 2;
		test_remove(last_descr);
		last_action = 0;
		test_seek_prev();
		test_seek_next();
		test_seek_exact();
		g_loops--;
	}

	// clean tree finally
	printf("++++++++++++++ fast clean B-tree started\n");
	std::auto_ptr<btree> tree(btree::open(tm, bm, pstm, pbtree_rm, g_root));
	tree->clear();
	printf("++++++++++++++ fast clean B-tree finished\n");

	// drop table
	uint32_t id = tm->begin_transaction(hard);
	tree->drop();
	if (g_used != pstm->used()) {
		printf("ERROR: storage leak %lld blocks\n", pstm->used() - g_used);
		throw test_failed();
	}
	tm->commit(id);
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
		msleep(my_rand() % 5000);
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
//	g_seed = 1273832770;
	printf("seed : %u\n", g_seed);
	g_rand.seed(g_seed);
	// skip first 10000 numbers to get better randomization
	for (int i = 0; i < 10000; i++) {
		random_value_0(100);
	}
	// initialize test parameters
	g_log_block_size = 1024 + random_value_1(1024);
	g_data_block_size = g_log_block_size;
	g_loops = random_value_1(1000000);
	g_data_capacity = MIN_CAPACITY + random_value_1(10000);
	g_log_capacity = g_data_capacity * 4;
	g_log_devs_count = random_value_1(MAX_DEVS);
	g_data_devs_count = random_value_1(MAX_DEVS);
	g_log_factor = random_value_1(4);
	g_data_factor = random_value_1(4);
	// init log devices
	switch (random_value_1(2)) {
	case 1:
		g_log_dev_type = bd_pool_device;
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
		break;
	case 2:
		g_log_dev_type = bd_raid0_device;
		uint64_t dev_capacity = g_log_capacity / g_log_devs_count / g_log_factor * g_log_factor;
		g_log_capacity = dev_capacity * g_log_devs_count;
		for (uint32_t i = 0; i < g_log_devs_count; i++) {
			g_log_devs_capacity[i] = dev_capacity;
		}
		break;
	}
	// init data devices
	switch (random_value_1(2)) {
	case 1:
		g_data_dev_type = bd_pool_device;
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
		break;
	case 2:
		g_data_dev_type = bd_raid0_device;
		uint64_t dev_capacity = g_data_capacity / g_data_devs_count / g_data_factor * g_data_factor;
		g_data_capacity = dev_capacity * g_data_devs_count;
		for (uint32_t i = 0; i < g_data_devs_count; i++) {
			g_data_devs_capacity[i] = dev_capacity;
		}
		break;
	}
	printf("loops %u\n", g_loops);
	printf("log block size %u, log capacity %llu, log device size %f MB\n",
				 g_log_block_size, g_log_capacity,
				 g_log_capacity * g_log_block_size / 1024.0 / 1024.0);
	printf("data block size %u, data capacity %llu, data device size %f MB\n",
				 g_data_block_size, g_data_capacity,
				 g_data_capacity * g_data_block_size / 1024.0 / 1024.0);
	// save used storage space
	start_devs();
	stm *					pstm = (stm *)whereis("stm");
	g_used = pstm->used();
	stop_devs(true);
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

