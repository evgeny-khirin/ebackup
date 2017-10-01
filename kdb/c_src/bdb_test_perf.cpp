///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : bdb_test_perf.cpp
/// Author  : Evgeny Khirin <>
/// Description : Berkeley DB B-tree performance test. Test has same
/// logic and options as B-tree performance test (btree_test_perf_2.cpp).
///-------------------------------------------------------------------
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/time.h>
#include <db.h>
#include <list>
#include <set>
#include <string>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>

#include "erl_interface.h"

#define FILES_LIST			"/var/ramdisk/files.lst"
#define DB_HOME "/tmp/bdb_test"
#define DATABASE "btree.db"

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
void build_files_list_rec(FILE * f, str_set const & sources, str_set const & excludes, str_set & result) {
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
uint32_t encode_term(unsigned char * buf, int32_t value) {
	buf[0] = 98;
	*((int32_t *)(buf + 1)) = htonl(value);
	return 5;
}

//--------------------------------------------------------------------
// Decodes integer term
//--------------------------------------------------------------------
int32_t decode_term(unsigned char * buf) {
	if (buf[0] != 98) {
		printf("ERROR: invalid term signature\n");
		exit(1);
	}
	return ntohl(*((int32_t *)(buf + 1)));
}

//--------------------------------------------------------------------
// Creates string term
//--------------------------------------------------------------------
uint32_t encode_term(unsigned char * buf, const std::string & value) {
	buf[0] = 109;	// Encode strings as binary for better performance
	*((int32_t *)(buf + 1)) = htonl(value.length());
	memcpy(buf + 5, value.c_str(), value.length());
	return value.length() + 5;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool lookup(DB *dbp, std::string const & k, int * v) {
	unsigned char k_buf[1024];
	if (k.length() > sizeof(k_buf) - 3) {
		printf("ERROR: buffer too small\n");
		exit(1);
	}
	uint32_t k_len = encode_term(k_buf, k);
	DBT key, data;
	int ret;
	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	key.data = k_buf;
	key.size = k_len;
	unsigned char v_buf[5];
	data.data = v_buf;
	data.ulen = sizeof(v_buf);
	data.flags = DB_DBT_USERMEM;
	ret = dbp->get(dbp, NULL, &key, &data, 0);
	if (ret == DB_NOTFOUND) {
		return false;
	}
	if (ret != 0) {
		dbp->err(dbp, ret, "DB->get");
		exit(ret);
	}
	*v = decode_term(v_buf);
	return true;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void insert(DB *dbp, std::string const & k, int v) {
	unsigned char k_buf[1024];
	if (k.length() > sizeof(k_buf) - 3) {
		printf("ERROR: buffer too small\n");
		exit(1);
	}
	uint32_t k_len = encode_term(k_buf, k);
	unsigned char v_buf[5];
	uint32_t v_len = encode_term(v_buf, v);
	DBT key, data;
	int ret;
	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	key.data = k_buf;
	key.size = k_len;
	data.data = v_buf;
	data.size = v_len;
	ret = dbp->put(dbp, NULL, &key, &data, 0);
	if (ret != 0) {
		dbp->err(dbp, ret, "DB->put");
		exit(ret);
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void insert(DB *dbp, int k, std::string const & v) {
	unsigned char k_buf[5];
	uint32_t k_len = encode_term(k_buf, k);
	unsigned char v_buf[1024];
	if (v.length() > sizeof(v_buf) - 3) {
		printf("ERROR: buffer too small\n");
		exit(1);
	}
	uint32_t v_len = encode_term(v_buf, v);
	DBT key, data;
	int ret;
	memset(&key, 0, sizeof(key));
	memset(&data, 0, sizeof(data));
	key.data = k_buf;
	key.size = k_len;
	data.data = v_buf;
	data.size = v_len;
	ret = dbp->put(dbp, NULL, &key, &data, 0);
	if (ret != 0) {
		dbp->err(dbp, ret, "DB->put");
		exit(ret);
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void remove(DB *dbp, std::string const & k) {
	unsigned char k_buf[1024];
	if (k.length() > sizeof(k_buf) - 3) {
		printf("ERROR: buffer too small\n");
		exit(1);
	}
	uint32_t k_len = encode_term(k_buf, k);
	DBT key;
	int ret;
	memset(&key, 0, sizeof(key));
	key.data = k_buf;
	key.size = k_len;
	ret = dbp->del(dbp, NULL, &key, 0);
	if (ret != 0) {
		dbp->err(dbp, ret, "dbp->del");
		exit(ret);
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void remove(DB *dbp, int k) {
	unsigned char k_buf[5];
	uint32_t k_len = encode_term(k_buf, k);
	DBT key;
	int ret;
	memset(&key, 0, sizeof(key));
	key.data = k_buf;
	key.size = k_len;
	ret = dbp->del(dbp, NULL, &key, 0);
	if (ret != 0) {
		dbp->err(dbp, ret, "DB->del");
		exit(ret);
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
int bt_compare_fcn(DB *db, const DBT *dbt1, const DBT *dbt2) {
	return erl_compare_ext((unsigned char *)dbt1->data,
												 (unsigned char *)dbt2->data);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
int main() {
	erl_init(NULL, 0);

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

	DB_ENV *envp;
	int ret;
	struct timeval tv;
	double global_start;
	double start, end;
	const char * free_key = "free";

	system("rm -rf " DB_HOME);
	system("mkdir " DB_HOME);

	ret = db_env_create(&envp, 0);
	if (ret != 0) {
		fprintf(stderr, "db_env_create: %s\n", db_strerror(ret));
		exit (1);
	}

	if ((ret = envp->set_cachesize(envp, 0, 4096 * 8 * 4096, 0)) != 0) {
		envp->err(envp, ret, "%s", "envp->set_cachesize");
		exit(ret);
	}

	uint32_t env_flags =
		DB_CREATE     |  /* Create the environment if it does not exist */
		DB_RECOVER    |  /* Run normal recovery. */
		DB_INIT_LOCK  |  /* Initialize the locking subsystem */
		DB_INIT_LOG   |  /* Initialize the logging subsystem */
		DB_INIT_TXN   |  /* Initialize the transactional subsystem. This */
										 /* also turns on logging. */
		DB_INIT_MPOOL |  /* Initialize the memory pool (in-memory cache) */
		DB_THREAD;       /* Cause the environment to be free-threaded */

	if ((ret = envp->open(envp, DB_HOME, env_flags, 0)) != 0) {
		envp->err(envp, ret, "%s", "envp->open");
		exit (1);
	}

	if ((ret = envp->set_flags(envp, DB_AUTO_COMMIT | DB_TXN_NOSYNC, 1)) != 0) {
		envp->err(envp, ret, "%s", "envp->set_flags");
		exit (1);
	}

	DB *dbp;
	if ((ret = db_create(&dbp, envp, 0)) != 0) {
		fprintf(stderr, "db_create: %s\n", db_strerror(ret));
		exit (1);
	}

	if ((ret = dbp->set_bt_compare(dbp, bt_compare_fcn)) != 0) {
		dbp->err(dbp, ret, "%s", "dbp->set_bt_compare");
		exit(ret);
	}

	if ((ret = dbp->set_pagesize(dbp, 4096 * 8)) != 0) {
		dbp->err(dbp, ret, "%s", "dbp->set_pagesize");
		exit(ret);
	}

	if ((ret = dbp->open(dbp, NULL, DATABASE, NULL, DB_BTREE, DB_CREATE | DB_NOMMAP, 0664)) != 0) {
		dbp->err(dbp, ret, "%s", "dbp->open");
		exit(ret);
	}

	gettimeofday(&tv, NULL);
	global_start = start = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;

	int loop = 0;
	for (str_set::const_iterator i = files.begin(); i != files.end(); i++, loop++) {
		std::string const & s = *i;
		int id;
		if (lookup(dbp, s, &id)) {
			printf("unexpected found %s, id %d\n", s.c_str(), id);
			exit(1);
		}
		if (!lookup(dbp, free_key, &id)) {
			id = 0;
		}
		insert(dbp, free_key, id + 1);
		insert(dbp, s, id);
		insert(dbp, id, s);
		if (loop % 10000 == 0) {
			gettimeofday(&tv, NULL);
			end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
			printf("insert: %d, time %f, id %d\n", loop, end - start, id);
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
		if (!lookup(dbp, s, &id)) {
			printf("lookup failed for %s\n", s.c_str());
			exit(1);
		}
		if (id != loop) {
			printf("found wrong id %d, for %s\n", id, s.c_str());
			exit(1);
		}
		if (loop % 10000 == 0) {
			gettimeofday(&tv, NULL);
			end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
			printf("lookup: %d, time %f, id %d\n", loop, end - start, id);
			fflush(stdout);
			start = end;
		}
	}
	gettimeofday(&tv, NULL);
	end = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	printf("===================================== lookup time %f\n", end - global_start);

	gettimeofday(&tv, NULL);
	global_start = start = (tv.tv_sec * 1000.0 * 1000.0 + tv.tv_usec) / 1000.0 / 1000.0;
	remove(dbp, "free");
	loop = 0;
	for (str_set::const_iterator i = files.begin(); i != files.end(); i++, loop++) {
		std::string const & s = *i;
		remove(dbp, s);
		remove(dbp, loop);
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

	dbp->close(dbp, 0);
	envp->close(envp, 0);

	return 0;
}

