///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : driver.cpp
/// Author  : Evgeny Khirin <>
/// Description : Erlang KDB driver.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <boost/static_assert.hpp>
#ifndef _WIN32
#include <unistd.h>
#include <sys/vfs.h>
#include <linux/magic.h>
#endif

#include "drv_terms.hpp"
#include "bd_adler.hpp"
#include "bd_cache.hpp"
#include "bd_crc.hpp"
#include "bd_dwrite.hpp"
#include "bd_factor.hpp"
#include "bd_file.hpp"
#include "bd_part.hpp"
#include "bd_pool.hpp"
#include "bd_raid0.hpp"
#include "bm.hpp"
#include "btree.hpp"
#include "kdb.hpp"
#include "ld_bd.hpp"
#include "ld_pp.hpp"
#include "logger.hpp"
#include "stm.hpp"
#include "tm.hpp"

//--------------------------------------------------------------------
// Requirements
//--------------------------------------------------------------------
namespace {
   BOOST_STATIC_ASSERT(sizeof(void *) == sizeof(uint32_t));
}

//--------------------------------------------------------------------
// Driver specific exceptions
//--------------------------------------------------------------------
namespace {
class kdb_not_started;
class unknown_class_name;
class unknown_driver_command;

DEF_EXCEPTION(kdb_not_started)					// KDB server is not started.
DEF_EXCEPTION(unknown_class_name)				// class name is unknown
DEF_EXCEPTION(unknown_driver_command)		// unknow command passed to KDB driver
}

//--------------------------------------------------------------------
// Global variables
//--------------------------------------------------------------------
static erl_atom	g_ok("ok");
static erl_atom	g_error("error");
static erl_atom	g_not_found("not_found");
static erl_atom	g_true("true");
static erl_atom	g_false("false");
static erl_atom	g_eof("eof");
static erl_atom	g_remote("remote");
static erl_atom	g_local("local");
static erl_atom	g_undefined("undefined");
static kdb *		g_kdb = NULL;

//--------------------------------------------------------------------
// device startup descriptor
//--------------------------------------------------------------------
namespace {
	struct dev_start_descr {
		std::string		m_class;
		opt_map				m_options;
	};
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
static named_object * start_object(const dev_start_descr & descr) {
	named_object * obj = NULL;
	if (descr.m_class == "bd_adler") {
		obj = new bd_adler;
	} else if (descr.m_class == "bd_cache") {
		obj = new bd_cache;
	} else if (descr.m_class == "bd_crc") {
		obj = new bd_crc;
	} else if (descr.m_class == "bd_dwrite") {
		obj = new bd_dwrite;
	} else if (descr.m_class == "bd_factor") {
		obj = new bd_factor;
	} else if (descr.m_class == "bd_file") {
		obj = new bd_file;
	} else if (descr.m_class == "bd_part") {
		obj = new bd_part;
	} else if (descr.m_class == "bd_pool") {
		obj = new bd_pool;
	} else if (descr.m_class == "bd_raid0") {
		obj = new bd_raid0;
	} else if (descr.m_class == "buffer_mgr") {
		obj = new buffer_mgr;
	} else if (descr.m_class == "kdb") {
		g_kdb = new kdb;
		obj = g_kdb;
	} else if (descr.m_class == "ld_bd") {
		obj = new ld_bd;
	} else if (descr.m_class == "ld_pp") {
		obj = new ld_pp;
	} else if (descr.m_class == "blogger") {
		obj = new blogger;
	} else if (descr.m_class == "stm") {
		obj = new stm;
	} else if (descr.m_class == "trans_mgr") {
		obj = new trans_mgr;
	} else {
		throw unknown_class_name();
	}
	obj->start(descr.m_options);
	return obj;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
static trans_type_t translate_trans_type(const std::string & type) {
	if (type == "hard") {
		return hard;
	}
	if (type == "soft") {
		return soft;
	}
	if (type == "read_only") {
		return read_only;
	}
	throw inv_param();
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
static position translate_position(const std::string & type) {
	if (type == "next") {
		return next;
	}
	if (type == "exact_next") {
		return exact_next;
	}
	if (type == "prev") {
		return prev;
	}
	if (type == "exact_prev") {
		return exact_prev;
	}
	if (type == "exact") {
		return exact;
	}
	throw inv_param();
}

//--------------------------------------------------------------------
// internal function
// A trailing backslash is required on Windows. For example, you
// specify \\MyServer\MyShare as "\\MyServer\MyShare\", or the C drive as "C:\".
//--------------------------------------------------------------------
static erl_atom & get_drive_type(const std::string & path) {
#ifdef _WIN32
	if (path[path.length() - 1] != '\\') {
		throw inv_param();
	}
	wchar_t wpath[MAX_PATH];
	MultiByteToWideChar(CP_UTF8, 0, path.c_str(), path.length() + 1, wpath, MAX_PATH);
	switch (GetDriveTypeW(wpath)) {
		case DRIVE_REMOTE:
			return g_remote;
		case DRIVE_REMOVABLE:
		case DRIVE_FIXED:
		case DRIVE_CDROM:
		case DRIVE_RAMDISK:
			return g_local;
		case DRIVE_UNKNOWN:
		case DRIVE_NO_ROOT_DIR:
		default:
			return g_undefined;
	}
#else
	struct statfs s;
	if (statfs(path.c_str(), &s) != 0) {
		return g_local;
	}
	switch (s.f_type) {
	case CODA_SUPER_MAGIC:
	case NCP_SUPER_MAGIC:
	case NFS_SUPER_MAGIC:
	case SMB_SUPER_MAGIC:
		return g_remote;
	}
	return g_local;
#endif
}

//--------------------------------------------------------------------
// driver commands
//--------------------------------------------------------------------
#define START_OBJECT						1
#define FIND_OBJECT							2
#define STOP_OBJECT							3
#define LD_BD_IS_FORMATTED			4
#define LD_BD_FORMAT						5
#define LD_BD_SYSTEM_GENERATION	6
#define RECOVER									7
#define BEGIN_TRANSACTION				8
#define COMMIT									9
#define ROLLBACK								10
#define OPEN_TABLE							11
#define CLOSE_TABLE							12
#define DROP_TABLE							13
#define INSERT									14
#define LOOKUP									15
#define REMOVE									16
#define BEGIN										17
#define SEEK										18
#define NEXT										19
#define CLOSE_ITER							20
#define USED										21
#define ROOT_BEGIN							22
#define GET_DRIVE_TYPE					23

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
static void process(uint32_t command, serial_buffer & in_buf, serial_buffer & out_buf) {
	try {
		switch (command) {
		case START_OBJECT:
			{
				dev_start_descr descr;
				uint32_t size;
				in_buf.get(size);
				const char * p = (const char *)in_buf.get_ptr(size);
				descr.m_class.assign(p, size);
				while (!in_buf.eob()) {
					in_buf.get(size);
					p = (const char *)in_buf.get_ptr(size);
					std::string name(p, size);
					in_buf.get(size);
					p = (const char *)in_buf.get_ptr(size);
					std::string value(p, size);
					descr.m_options[name] = value;
				}
				named_object * obj = start_object(descr);
				// encode reply
				erl_integer obj_ptr((uint32_t)obj);
				erl_tuple reply;
				reply.push_back(g_ok);
				reply.push_back(obj_ptr);
				erl_reply::serialize(out_buf, reply);
			}
			break;
		case FIND_OBJECT:
			{
				uint32_t size;
				in_buf.get(size);
				const char * p = (const char *)in_buf.get_ptr(size);
				assert(in_buf.eob());
				named_object * obj = whereis(std::string(p, size));
				// encode reply
				if (obj == NULL) {
					erl_reply::serialize(out_buf, g_not_found);
				} else {
					erl_integer ptr((uint32_t)obj);
					erl_tuple reply;
					reply.push_back(g_ok);
					reply.push_back(ptr);
					erl_reply::serialize(out_buf, reply);
				}
			}
			break;
		case STOP_OBJECT:
			{
				uint32_t size;
				in_buf.get(size);
				const char * p = (const char *)in_buf.get_ptr(size);
				assert(in_buf.eob());
				std::string name(p, size);
				named_object * obj = whereis(name);
				if (obj != NULL) {
					obj->stop();
					obj->del_underlaying();
					delete obj;
					if (obj == g_kdb) {
						g_kdb = NULL;
					}
				}
				// encode reply
				erl_reply::serialize(out_buf, g_ok);
			}
			break;
		case LD_BD_IS_FORMATTED:
			{
				block_device * dev;
				in_buf.get(dev);
				uint32_t size;
				in_buf.get(size);
				const char * p = (const char *)in_buf.get_ptr(size);
				assert(in_buf.eob());
				std::string app_name(p, size);
				// encode reply
				if (ld_bd::is_formatted(dev, app_name)) {
					erl_reply::serialize(out_buf, g_true);
				} else {
					erl_reply::serialize(out_buf, g_false);
				}
			}
			break;
		case LD_BD_FORMAT:
			{
				block_device * dev;
				in_buf.get(dev);
				uint32_t size;
				in_buf.get(size);
				const char * p = (const char *)in_buf.get_ptr(size);
				assert(in_buf.eob());
				std::string app_name(p, size);
				ld_bd::format(dev, app_name);
			}
			break;
		case LD_BD_SYSTEM_GENERATION:
			{
				ld_bd * dev;
				in_buf.get(dev);
				assert(in_buf.eob());
				// encode reply
				erl_uint64 gen(dev->system_generation());
				erl_tuple reply;
				reply.push_back(g_ok);
				reply.push_back(gen);
				erl_reply::serialize(out_buf, reply);
			}
			break;
		case RECOVER:
			if (g_kdb == NULL) {
				throw kdb_not_started();
			}
			g_kdb->recover();
			// encode reply
			erl_reply::serialize(out_buf, g_ok);
			break;
		case BEGIN_TRANSACTION:
			{
				if (g_kdb == NULL) {
					throw kdb_not_started();
				}
				uint32_t size;
				in_buf.get(size);
				const char * p = (const char *)in_buf.get_ptr(size);
				assert(in_buf.eob());
				std::string type(p, size);
				// encode reply
				erl_integer id(g_kdb->begin_transaction(translate_trans_type(type)));
				erl_tuple reply;
				reply.push_back(g_ok);
				reply.push_back(id);
				erl_reply::serialize(out_buf, reply);
			}
			break;
		case COMMIT:
			{
				if (g_kdb == NULL) {
					throw kdb_not_started();
				}
				uint32_t id;
				in_buf.get(id);
				assert(in_buf.eob());
				g_kdb->commit(id);
			}
			break;
		case ROLLBACK:
			{
				if (g_kdb == NULL) {
					throw kdb_not_started();
				}
				uint32_t id;
				in_buf.get(id);
				assert(in_buf.eob());
				g_kdb->rollback(id);
			}
			break;
		case OPEN_TABLE:
			{
				if (g_kdb == NULL) {
					throw kdb_not_started();
				}
				uint32_t size;
				in_buf.get(size);
				const void * p = in_buf.get_ptr(size);
				assert(in_buf.eob());
				// encode reply
				term_ptr name(new term(p, size));
				erl_integer tree_ptr((uint32_t)g_kdb->open(name));
				erl_tuple reply;
				reply.push_back(g_ok);
				reply.push_back(tree_ptr);
				erl_reply::serialize(out_buf, reply);
			}
			break;
		case CLOSE_TABLE:
			{
				if (g_kdb == NULL) {
					throw kdb_not_started();
				}
				btree * tree;
				in_buf.get(tree);
				assert(in_buf.eob());
				g_kdb->close(tree);
			}
			break;
		case DROP_TABLE:
			{
				if (g_kdb == NULL) {
					throw kdb_not_started();
				}
				uint32_t size;
				in_buf.get(size);
				const void * p = in_buf.get_ptr(size);
				assert(in_buf.eob());
				term_ptr name(new term(p, size));
				g_kdb->drop(name);
			}
			break;
		case INSERT:
			{
				btree * tree;
				in_buf.get(tree);
				uint32_t size;
				in_buf.get(size);
				const void * p = in_buf.get_ptr(size);
				term_ptr k(new term(p, size));
				in_buf.get(size);
				p = in_buf.get_ptr(size);
				term_ptr v(new term(p, size));
				assert(in_buf.eob());
				tree->insert(k, v);
			}
			break;
		case LOOKUP:
			{
				btree * tree;
				in_buf.get(tree);
				uint32_t size;
				in_buf.get(size);
				const void * p = in_buf.get_ptr(size);
				term_ptr k(new term(p, size));
				assert(in_buf.eob());
				term_ptr v;
				// encode reply
				switch (tree->lookup(k, v)) {
				case ok:
					{
						erl_binary rv(v->data(), v->size());
						erl_tuple reply;
						reply.push_back(g_ok);
						reply.push_back(rv);
						erl_reply::serialize(out_buf, reply);
					}
					break;
				case not_found:
					erl_reply::serialize(out_buf, g_not_found);
					break;
				default:
					assert(false);
				}
			}
			break;
		case REMOVE:
			{
				btree * tree;
				in_buf.get(tree);
				uint32_t size;
				in_buf.get(size);
				const void * p = in_buf.get_ptr(size);
				term_ptr k(new term(p, size));
				assert(in_buf.eob());
				tree->remove(k);
			}
			break;
		case BEGIN:
			{
				btree * tree;
				in_buf.get(tree);
				assert(in_buf.eob());
				btree::iterator * it = new btree::iterator;
				*it = tree->begin();
				// encode reply
				erl_integer iter_ptr((uint32_t)it);
				erl_tuple reply;
				reply.push_back(g_ok);
				reply.push_back(iter_ptr);
				erl_reply::serialize(out_buf, reply);
			}
			break;
		case SEEK:
			{
				btree * tree;
				in_buf.get(tree);
				uint32_t size;
				in_buf.get(size);
				const void * p = in_buf.get_ptr(size);
				term_ptr k(new term(p, size));
				in_buf.get(size);
				p = in_buf.get_ptr(size);
				std::string pos((const char *)p, size);
				assert(in_buf.eob());
				btree::iterator * it = new btree::iterator;
				*it = tree->seek(k, translate_position(pos));
				// encode reply
				erl_integer iter_ptr((uint32_t)it);
				erl_tuple reply;
				reply.push_back(g_ok);
				reply.push_back(iter_ptr);
				erl_reply::serialize(out_buf, reply);
			}
			break;
		case NEXT:
			{
				btree::iterator * it;
				in_buf.get(it);
				assert(in_buf.eob());
				// encode reply
				if (!it->has_more()) {
					erl_reply::serialize(out_buf, g_eof);
				} else {
					erl_binary k((*it)->first->data(), (*it)->first->size());
					erl_binary v((*it)->second->data(), (*it)->second->size());
					erl_tuple reply;
					reply.push_back(g_ok);
					reply.push_back(k);
					reply.push_back(v);
					erl_reply::serialize(out_buf, reply);
					(*it)++;
				}
			}
			break;
		case CLOSE_ITER:
			{
				btree::iterator * it;
				in_buf.get(it);
				assert(in_buf.eob());
				delete it;
			}
			break;
		case USED:
			{
				if (g_kdb == NULL) {
					throw kdb_not_started();
				}
				// encode reply
				erl_uint64 used(g_kdb->used());
				erl_tuple reply;
				reply.push_back(g_ok);
				reply.push_back(used);
				erl_reply::serialize(out_buf, reply);
			}
			break;
		case ROOT_BEGIN:
			{
				if (g_kdb == NULL) {
					throw kdb_not_started();
				}
				btree::iterator * it = new btree::iterator;
				*it = g_kdb->root_begin();
				// encode reply
				erl_integer iter_ptr((uint32_t)it);
				erl_tuple reply;
				reply.push_back(g_ok);
				reply.push_back(iter_ptr);
				erl_reply::serialize(out_buf, reply);
			}
			break;
		case GET_DRIVE_TYPE:
			{
				uint32_t size;
				in_buf.get(size);
				const char * p = (const char *)in_buf.get_ptr(size);
				assert(in_buf.eob());
				std::string path(p, size);
				// encode reply
				erl_reply::serialize(out_buf, get_drive_type(path));
			}
			break;
		default:
			throw unknown_driver_command();
		}
	} catch (std::exception & e) {
		erl_atom reason(e.what());
		erl_tuple reply;
		reply.push_back(g_error);
		reply.push_back(reason);
		erl_reply::serialize(out_buf, reply);
	}
}

//--------------------------------------------------------------------
// reads exact amount of bytes from stdin
//--------------------------------------------------------------------
#ifdef _WIN32
static HANDLE gstdin;
result read_exact(temp_buffer & buf, uint32_t size) {
	buf.alloc(size);
	uint32_t offset = 0;
	while (offset != size) {
		DWORD count;
		if (!ReadFile(gstdin, buf.m_pdata + offset,
									size - offset, &count, NULL)) {
			return my_eof;
		}
		if (count <= 0) {
			return my_eof;
		}
		offset += count;
	}
	return ok;
}
#else
result read_exact(temp_buffer & buf, uint32_t size) {
	buf.alloc(size);
	uint32_t offset = 0;
	while (offset != size) {
		int32_t count = read(STDIN_FILENO, buf.m_pdata + offset, size - offset);
		if (count <= 0) {
			return my_eof;
		}
		offset += count;
	}
	return ok;
}
#endif

//--------------------------------------------------------------------
// writes exact amount of bytes to stdout
//--------------------------------------------------------------------
#ifdef _WIN32
static HANDLE gstdout;
result write_exact(const void * buf, uint32_t size) {
	const char * p = (const char *)buf;
	uint32_t offset = 0;
	while (offset != size) {
		DWORD count;
		if (!WriteFile(gstdout, p + offset,
									 size - offset, &count, NULL)) {
			return my_eof;
		}
		if (count <= 0) {
			return my_eof;
		}
		offset += count;
	}
	return ok;
}
#else
result write_exact(const void * buf, uint32_t size) {
	const char * p = (const char *)buf;
	uint32_t offset = 0;
	while (offset != size) {
		int32_t count = write(STDOUT_FILENO, p + offset, size - offset);
		if (count <= 0) {
			return my_eof;
		}
		offset += count;
	}
	return ok;
}
#endif

//--------------------------------------------------------------------
// translates command to its name
//--------------------------------------------------------------------
const char * command_name(uint32_t command) {
	switch (command) {
	case START_OBJECT:
		return "START_OBJECT";
	case FIND_OBJECT:
		return "FIND_OBJECT";
	case STOP_OBJECT:
		return "STOP_OBJECT";
	case LD_BD_IS_FORMATTED:
		return "LD_BD_IS_FORMATTED";
	case LD_BD_FORMAT:
		return "LD_BD_FORMAT";
	case LD_BD_SYSTEM_GENERATION:
		return "LD_BD_SYSTEM_GENERATION";
	case RECOVER:
		return "RECOVER";
	case BEGIN_TRANSACTION:
		return "BEGIN_TRANSACTION";
	case COMMIT:
		return "COMMIT";
	case ROLLBACK:
		return "ROLLBACK";
	case OPEN_TABLE:
		return "OPEN_TABLE";
	case CLOSE_TABLE:
		return "CLOSE_TABLE";
	case DROP_TABLE:
		return "DROP_TABLE";
	case INSERT:
		return "INSERT";
	case LOOKUP:
		return "LOOKUP";
	case REMOVE:
		return "REMOVE";
	case BEGIN:
		return "BEGIN";
	case SEEK:
		return "SEEK";
	case NEXT:
		return "NEXT";
	case CLOSE_ITER:
		return "CLOSE_ITER";
	case USED:
		return "USED";
	case ROOT_BEGIN:
		return "ROOT_BEGIN";
	case GET_DRIVE_TYPE:
		return "GET_DRIVE_TYPE";
	default:
		throw unknown_driver_command();
	}
}

//--------------------------------------------------------------------
// driver entry point
//--------------------------------------------------------------------
int main() {
#ifdef _WIN32
	SetErrorMode(
		SEM_FAILCRITICALERRORS | 
		SEM_NOGPFAULTERRORBOX | 
		SEM_NOALIGNMENTFAULTEXCEPT | 
		SEM_NOOPENFILEERRORBOX);
	gstdin = GetStdHandle(STD_INPUT_HANDLE);
	gstdout = GetStdHandle(STD_OUTPUT_HANDLE);
#endif
	temp_buffer read_buf;
	serial_buffer out_buf;
	while (read_exact(read_buf, 4) == ok) {
		uint32_t size = ntohl(*((uint32_t *)read_buf.m_pdata));
		if (read_exact(read_buf, size) != ok) {
			break;
		}
//		fprintf(stderr, "got %u bytes\n", size);
		serial_buffer in_buf(read_buf.m_pdata, size, true);
		uint32_t command;
		in_buf.get(command);
		out_buf.rewind();
//		fprintf(stderr, "command %s\n", command_name(command));
		process(command, in_buf, out_buf);
//		fprintf(stderr, "replying %u bytes\n", out_buf.size());
//		for (uint32_t i = 0; i < out_buf.size(); i++) {
//			fprintf(stderr, "%u, ", (uint32_t)*((uint8_t *)out_buf.data() + i));
//		}
//		fprintf(stderr, "\n");
		if (out_buf.size() > 0) {
			size = htonl(out_buf.size());
			if (write_exact(&size, 4) != ok) {
				break;
			}
			if (write_exact(out_buf.data(), out_buf.size()) != ok) {
				break;
			}
		}
	}
	if (g_kdb != NULL) {
		g_kdb->stop();
		g_kdb->del_underlaying();
		delete g_kdb;
	}
	return 0;
}

