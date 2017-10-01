///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : btree.cpp
/// Author  : Evgeny Khirin <>
/// Description : Implements B+-tree with following propreties:
///   1. Any erlang term can be key.
///   2. Any erlang term can be value.
///   3. No practical limit on size of key-value pair: max key size 2G and
///      max value size is 2G. First implementation does not support this feature.
///      Max size of key-value pair must not exceed max chunk size and depends
///      on block size.
///   4. Delete algorithm is lazy, without merging. So there are possible empty
///      inner nodes with single pointer only. Tree is rolled up when root node
///      becomes empty (single pointer only).
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#define __STDC_CONSTANT_MACROS
#include <stdint.h>
#include <assert.h>
#include <memory>

#include "btree.hpp"

//--------------------------------------------------------------------
// initialize erl_interface
//--------------------------------------------------------------------
namespace {
	static class init_erl_interface {
		public:
			init_erl_interface() {
				erl_init(NULL, 0);
			}
		} g_init_erl_interface;
}

//--------------------------------------------------------------------
// undo/redo signatures
//--------------------------------------------------------------------
#define NEW_NODE						1
#define SPLIT_NODE					2
#define UPDATE_LEAF					3
#define INSERT_LEAF					4
#define REMOVE_LEAF					5
#define INSERT_INNER				6
#define REMOVE_INNER				7
#define SET_NEXT						8
#define SET_PREV						9
#define FREE_NODE						10
#define ROLLUP_ROOT					13
#define SET_LAST						14

//--------------------------------------------------------------------
// undo/redo protocol
//--------------------------------------------------------------------
namespace {
	//--------------------------------------------------------------------
	// log record
	//--------------------------------------------------------------------
	struct log_rec {
		char	m_sign;
	};

	//--------------------------------------------------------------------
	// new node record
	//--------------------------------------------------------------------
	struct new_node_rec: public log_rec {
		// Fieals are valid after deserialization only;
		decoded_block_ptr		m_pb;

		new_node_rec() {m_sign = NEW_NODE;}

		void serialize(serial_buffer & buf, btree::node_t & node) {
			buf.put(m_sign);
			buf.put(node.m_block);
			node.serialize(buf);
		}

		void deserialize(serial_buffer & buf) {
			buf.get(m_sign);
			uint64_t n;
			buf.get(n);
			m_pb = btree::node_decoder(n, buf);
		}
	};

	//--------------------------------------------------------------------
	// split node record
	//--------------------------------------------------------------------
	struct split_node_rec: public new_node_rec {
		split_node_rec() {m_sign = SPLIT_NODE;}
	};

	//--------------------------------------------------------------------
	// update leaf record
	//--------------------------------------------------------------------
	struct update_leaf_rec: public log_rec {
		// Fieals are valid after deserialization only;
		term_ptr	m_key;
		term_ptr	m_old_value;
		term_ptr	m_new_value;

		update_leaf_rec() {m_sign = UPDATE_LEAF;}

		void serialize(serial_buffer & buf, const term_ptr & k, const term_ptr & old_v,
									 const term_ptr & new_v) {
			buf.put(m_sign);
			k->serialize(buf);
			old_v->serialize(buf);
			new_v->serialize(buf);
		}

		void deserialize(serial_buffer & buf) {
			buf.get(m_sign);
			m_key = term::deserialize(buf);
			m_old_value = term::deserialize(buf);
			m_new_value = term::deserialize(buf);
		}
	};

	//--------------------------------------------------------------------
	// insert leaf record
	//--------------------------------------------------------------------
	struct insert_leaf_rec: public log_rec {
		// Fieals are valid after deserialization only;
		term_ptr	m_key;
		term_ptr	m_value;

		insert_leaf_rec() {m_sign = INSERT_LEAF;}

		void serialize(serial_buffer & buf, const term_ptr & k, const term_ptr & v) {
			buf.put(m_sign);
			k->serialize(buf);
			v->serialize(buf);
		}

		void deserialize(serial_buffer & buf) {
			buf.get(m_sign);
			m_key = term::deserialize(buf);
			m_value = term::deserialize(buf);
		}
	};

	//--------------------------------------------------------------------
	// remove leaf record
	//--------------------------------------------------------------------
	struct remove_leaf_rec: public insert_leaf_rec {
		remove_leaf_rec() {m_sign = REMOVE_LEAF;}
	};

	//--------------------------------------------------------------------
	// insert inner record
	//--------------------------------------------------------------------
	struct insert_inner_rec: public log_rec {
		// Fieals are valid after deserialization only;
		term_ptr	m_term;
		uint64_t	m_less_ptr;
		uint64_t	m_old_last;
		uint64_t	m_new_last;

		insert_inner_rec() {m_sign = INSERT_INNER;}

		void serialize(serial_buffer & buf, const term_ptr & t, uint64_t less_ptr,
									 uint64_t old_last, uint64_t new_last) {
			buf.put(m_sign);
			t->serialize(buf);
			buf.put(less_ptr);
			buf.put(old_last);
			buf.put(new_last);
		}

		void deserialize(serial_buffer & buf) {
			buf.get(m_sign);
			m_term = term::deserialize(buf);
			buf.get(m_less_ptr);
			buf.get(m_old_last);
			buf.get(m_new_last);
		}
	};

	//--------------------------------------------------------------------
	// remove inner record
	//--------------------------------------------------------------------
	struct remove_inner_rec: public insert_inner_rec {
		remove_inner_rec() {m_sign = REMOVE_INNER;}
	};

	//--------------------------------------------------------------------
	// set next record
	//--------------------------------------------------------------------
	struct set_next_rec: public log_rec {
		// Fieals are valid after deserialization only;
		uint64_t	m_old_value;
		uint64_t	m_new_value;

		set_next_rec() {m_sign = SET_NEXT;}

		void serialize(serial_buffer & buf, uint64_t old_value, uint64_t new_value) {
			buf.put(m_sign);
			buf.put(old_value);
			buf.put(new_value);
		}

		void deserialize(serial_buffer & buf) {
			buf.get(m_sign);
			buf.get(m_old_value);
			buf.get(m_new_value);
		}
	};

	//--------------------------------------------------------------------
	// set prev record
	//--------------------------------------------------------------------
	struct set_prev_rec: public set_next_rec {
		set_prev_rec() {m_sign = SET_PREV;}
	};

	//--------------------------------------------------------------------
	// free node record
	//--------------------------------------------------------------------
	struct free_node_rec: public new_node_rec {
		free_node_rec() {m_sign = FREE_NODE;}
	};

	//--------------------------------------------------------------------
	// roll up root record
	//--------------------------------------------------------------------
	struct rollup_root_rec: public new_node_rec {
		// Fieals are valid after deserialization only;
		uint64_t	m_last;

		rollup_root_rec() {m_sign = ROLLUP_ROOT;}

		void serialize(serial_buffer & buf, btree::node_t & node, uint64_t last) {
			new_node_rec::serialize(buf, node);
			buf.put(last);
		}

		void deserialize(serial_buffer & buf) {
			new_node_rec::deserialize(buf);
			buf.get(m_last);
		}
	};

	//--------------------------------------------------------------------
	// set last record
	//--------------------------------------------------------------------
	struct set_last_rec: public set_next_rec {
		set_last_rec() {m_sign = SET_LAST;}
	};
} // namespace

//--------------------------------------------------------------------
// public function
// Supported options:
//   {transaction_manager, Name} - transaction manager.
//   {buffer_manager, Name} - buffer manager.
//--------------------------------------------------------------------
void btree_rm::start(const opt_map & options) {
	resource_mgr::start(options);

	opt_map::const_iterator it;
	if ((it = options.find("transaction_manager")) == options.end()) {
		throw missed_param();
	}
	m_tm = (trans_mgr *)whereis(it->second);
	if (m_tm == NULL) {
		throw not_registered();
	}
	m_tm->register_rm(this);

	if ((it = options.find("buffer_manager")) == options.end()) {
		throw missed_param();
	}
	m_bm = (buffer_mgr *)whereis(it->second);
	if (m_bm == NULL) {
		throw not_registered();
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree_rm::start(const std::string & name, const std::string & trans_mgr,
										 const std::string & buffer_mgr) {
	printf("btree_rm: name %s, trans_mgr %s, buffer_mgr %s\n",
				 name.c_str(), trans_mgr.c_str(), buffer_mgr.c_str());
	opt_map	opts;
	opts["name"] = name;
	opts["transaction_manager"] = trans_mgr;
	opts["buffer_manager"] = buffer_mgr;
	btree_rm * pbtree_rm = new btree_rm;
	pbtree_rm->start(opts);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree_rm::stop() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree_rm::del_underlaying() {
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree_rm::stats(no_stats_list & list) {
}

//--------------------------------------------------------------------
// Decodes integer term
//--------------------------------------------------------------------
/*
static uint32_t decode_term(const term_ptr & v, uint32_t & len) {
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
*/

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree_rm::undo(uint64_t n, const void * data, uint32_t size, void * opaque) {
	if (m_ignore_undo == n) {
//		printf("btree_rm::undo: %llu ignored because m_ignore_undo %llu\n", n, m_ignore_undo);
		m_ignore_undo = UINT64_MAX;
		return;
	}
	switch (*((char *)data)) {
	case NEW_NODE:
		// nothing to undo
//		printf("btree_rm::undo: NEW_NODE, block %llu\n", n);
		return;
	case SPLIT_NODE:
		{
			split_node_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
//			printf("btree_rm::undo: SPLIT_NODE, block %llu stored in m_splitted\n", n);
			m_splitted = rec.m_pb;
		}
		return;
	case UPDATE_LEAF:
		{
			update_leaf_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
//			{
//				uint32_t k_len;
//				uint32_t k_val = decode_term(rec.m_key, k_len);
//				printf("btree_rm::undo: UPDATE_LEAF, block %llu, key {%u, %u}\n", n, k_val, k_len);
//			}
			// undo operation
			decoded_block_ptr pb;
			uint64_t redo_lsn = 0;
			if (m_splitted.get() != NULL) {
				pb = m_splitted;
			} else {
				// write compensation record BEFORE real node change, because if node is dirty,
				// it can be flushed by buffer manager on log_update, if transaction manager decide
				// to take checkpoint.
				update_leaf_rec crec;
				serial_buffer cbuf;
				crec.serialize(cbuf, rec.m_key, rec.m_new_value, rec.m_old_value);
				redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
				// read node
				pb = m_bm->read(n, btree::node_decoder);
			}
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			assert(node.m_block == n);
			assert(node.m_map.find(rec.m_key) != node.m_map.end());
			node.m_used = node.m_used - rec.m_new_value->size() + rec.m_old_value->size();
			node.m_map[rec.m_key] = rec.m_old_value;
//			node.check_used();
			if (m_splitted.get() != NULL) {
				m_splitted = decoded_block_ptr();
				// write compensation record for splitted node undo
				new_node_rec crec;
				serial_buffer cbuf;
				crec.serialize(cbuf, node);
				redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			}
			// save node
			assert(node.m_used <= m_bm->block_size());
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case INSERT_LEAF:
		{
			insert_leaf_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_key, k_len);
// 				printf("btree_rm::undo: INSERT_LEAF, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// undo operation
			decoded_block_ptr pb;
			uint64_t redo_lsn = 0;
			if (m_splitted.get() != NULL) {
				pb = m_splitted;
			} else {
				// write compensation record BEFORE real node change, because if node is dirty,
				// it can be flushed by buffer manager on log_update, if transaction manager decide
				// to take checkpoint.
				remove_leaf_rec crec;
				serial_buffer cbuf;
				crec.serialize(cbuf, rec.m_key, rec.m_value);
				redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
				// read node
				pb = m_bm->read(n, btree::node_decoder);
			}
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			assert(node.m_block == n);
			assert(node.m_map.find(rec.m_key) != node.m_map.end());
			node.m_used = node.m_used - rec.m_key->size() - rec.m_value->size() - 4 - 4;
			node.m_map.erase(rec.m_key);
//			node.check_used();
			if (m_splitted.get() != NULL) {
				m_splitted = decoded_block_ptr();
				// write compensation record for splitted node undo
				new_node_rec crec;
				serial_buffer cbuf;
				crec.serialize(cbuf, node);
				redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			}
			// save node
			assert(node.m_used <= m_bm->block_size());
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case REMOVE_LEAF:
		{
			remove_leaf_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_key, k_len);
// 				printf("btree_rm::undo: REMOVE_LEAF, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			insert_leaf_rec crec;
			serial_buffer cbuf;
			crec.serialize(cbuf, rec.m_key, rec.m_value);
			uint64_t redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			// undo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			assert(node.m_map.find(rec.m_key) == node.m_map.end());
			node.m_used = node.m_used + rec.m_key->size() + rec.m_value->size() + 4 + 4;
			node.m_map[rec.m_key] = rec.m_value;
//			node.check_used();
			// save node
			assert(node.m_used <= m_bm->block_size());
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case INSERT_INNER:
		{
			insert_inner_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_term, k_len);
// 				printf("btree_rm::undo: INSERT_INNER, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// undo operation
			decoded_block_ptr pb;
			uint64_t redo_lsn = 0;
			if (m_splitted.get() != NULL) {
				pb = m_splitted;
			} else {
				// write compensation record BEFORE real node change, because if node is dirty,
				// it can be flushed by buffer manager on log_update, if transaction manager decide
				// to take checkpoint.
				remove_inner_rec crec;
				serial_buffer cbuf;
				crec.serialize(cbuf, rec.m_term, rec.m_less_ptr, rec.m_new_last, rec.m_old_last);
				redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
				// read node
				pb = m_bm->read(n, btree::node_decoder);
			}
			btree::inner_node & node = (btree::inner_node &)*pb;
			assert(node.m_block == n);
			assert(node.m_map.find(rec.m_term) != node.m_map.end());
			node.m_used = node.m_used - rec.m_term->size() - 4 - 8;
			node.m_map.erase(rec.m_term);
			node.m_last = rec.m_old_last;
//			node.check_used();
			if (m_splitted.get() != NULL) {
				m_splitted = decoded_block_ptr();
				// write compensation record for splitted node undo
				new_node_rec crec;
				serial_buffer cbuf;
				crec.serialize(cbuf, node);
				redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			}
			// save node
			assert(node.m_used <= m_bm->block_size());
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case REMOVE_INNER:
		{
			remove_inner_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_term, k_len);
// 				printf("btree_rm::undo: REMOVE_INNER, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			insert_inner_rec crec;
			serial_buffer cbuf;
			crec.serialize(cbuf, rec.m_term, rec.m_less_ptr, rec.m_new_last, rec.m_old_last);
			uint64_t redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			// undo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::inner_node & node = (btree::inner_node &)*pb;
			assert(node.m_map.find(rec.m_term) == node.m_map.end());
			node.m_used = node.m_used + rec.m_term->size() + 4 + 8;
			node.m_map[rec.m_term] = rec.m_less_ptr;
			node.m_last = rec.m_old_last;
//			node.check_used();
			// save node
			assert(node.m_used <= m_bm->block_size());
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case SET_NEXT:
		{
			set_next_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::undo: SET_NEXT, block %llu\n", n);
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			set_next_rec crec;
			serial_buffer cbuf;
			crec.serialize(cbuf, UINT64_MAX, rec.m_old_value);
			uint64_t redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			// undo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			assert(node.m_next == rec.m_new_value);
			node.m_next = rec.m_old_value;
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case SET_PREV:
		{
			set_prev_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::undo: SET_PREV, block %llu\n", n);
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			set_prev_rec crec;
			serial_buffer cbuf;
			crec.serialize(cbuf, UINT64_MAX, rec.m_old_value);
			uint64_t redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			// undo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			assert(node.m_prev == rec.m_new_value);
			node.m_prev = rec.m_old_value;
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case FREE_NODE:
		{
			free_node_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::undo: FREE_NODE, block %llu\n", n);
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			new_node_rec crec;
			serial_buffer cbuf;
			crec.serialize(cbuf, (btree::node_t &)*rec.m_pb);
			uint64_t redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			// save node
			m_bm->write(n, rec.m_pb, redo_lsn);
		}
		return;
	case ROLLUP_ROOT:
		{
			rollup_root_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::undo: ROLLUP_ROOT, block %llu\n", n);
			// undo operation
			btree::inner_node * node = new btree::inner_node(n);
			decoded_block_ptr pb(node);
			node->m_last = rec.m_last;
			node->m_used += 8;
			// write compensation record
			new_node_rec crec;
			serial_buffer cbuf;
			crec.serialize(cbuf, *node);
			uint64_t redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case SET_LAST:
		{
			set_last_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::undo: SET_LAST, block %llu\n", n);
			// write compensation record BEFORE real node change, because if node is dirty,
			// it can be flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			set_last_rec crec;
			serial_buffer cbuf;
			crec.serialize(cbuf, UINT64_MAX, rec.m_old_value);
			uint64_t redo_lsn = m_tm->log_compensate(this, n, cbuf.data(), cbuf.size(), opaque);
			// undo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::inner_node & node = (btree::inner_node &)*pb;
			assert(node.m_last == rec.m_new_value);
			node.m_last = rec.m_old_value;
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	default:
		assert(false);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree_rm::redo(uint64_t n, bool state_only, const void * data, uint32_t size, uint64_t redo_lsn) {
	m_ignore_undo = UINT64_MAX;
	if (state_only) {
		return;
	}
	switch (*((char *)data)) {
	case NEW_NODE:
	case ROLLUP_ROOT:
		{
			new_node_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::redo: %s, block %llu\n",
// 						 *((char *)data) == NEW_NODE ? "NEW_NODE" : "ROLLUP_ROOT", n);
			// save node
			assert(((btree::node_t &)*rec.m_pb).m_used <= m_bm->block_size());
			m_bm->write(n, rec.m_pb, redo_lsn);
		}
		return;
	case SPLIT_NODE:
		// nothing to redo
// 		printf("btree_rm::redo: SPLIT_NODE, block %llu\n", n);
		return;
	case UPDATE_LEAF:
		{
			update_leaf_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_key, k_len);
// 				printf("btree_rm::redo: UPDATE_LEAF, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// redo operation only, if it will not cause split
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			uint32_t new_used = node.m_used - rec.m_old_value->size() + rec.m_new_value->size();
			if (new_used > m_bm->block_size()) {
				m_ignore_undo = n;
			} else {
				assert(node.m_map.find(rec.m_key) != node.m_map.end());
				node.m_used = new_used;
				node.m_map[rec.m_key] = rec.m_new_value;
//				node.check_used();
				// save node
				m_bm->write(n, pb, redo_lsn);
			}
		}
		return;
	case INSERT_LEAF:
		{
			insert_leaf_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_key, k_len);
// 				printf("btree_rm::redo: INSERT_LEAF, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// redo operation only, if it will not cause split
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			uint32_t new_used = node.m_used + rec.m_key->size() + rec.m_value->size() + 4 + 4;
			if (new_used > m_bm->block_size()) {
				m_ignore_undo = n;
			} else {
				assert(node.m_map.find(rec.m_key) == node.m_map.end());
				node.m_used = new_used;
				node.m_map[rec.m_key] = rec.m_value;
//				node.check_used();
				// save node
				m_bm->write(n, pb, redo_lsn);
			}
		}
		return;
	case REMOVE_LEAF:
		{
			remove_leaf_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_key, k_len);
// 				printf("btree_rm::redo: REMOVE_LEAF, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// redo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			assert(node.m_map.find(rec.m_key) != node.m_map.end());
			node.m_used = node.m_used - rec.m_key->size() - rec.m_value->size() - 4 - 4;
			node.m_map.erase(rec.m_key);
//			node.check_used();
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case INSERT_INNER:
		{
			insert_inner_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_term, k_len);
// 				printf("btree_rm::redo: INSERT_INNER, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// redo operation only, if it will not cause split
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::inner_node & node = (btree::inner_node &)*pb;
			uint32_t new_used = node.m_used + rec.m_term->size() + 4 + 8;
			if (new_used > m_bm->block_size()) {
				m_ignore_undo = n;
			} else {
				assert(node.m_map.find(rec.m_term) == node.m_map.end());
				node.m_used = new_used;
				node.m_map[rec.m_term] = rec.m_less_ptr;
				node.m_last = rec.m_new_last;
//				node.check_used();
				// save node
				m_bm->write(n, pb, redo_lsn);
			}
		}
		return;
	case REMOVE_INNER:
		{
			remove_inner_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			{
// 				uint32_t k_len;
// 				uint32_t k_val = decode_term(rec.m_term, k_len);
// 				printf("btree_rm::redo: REMOVE_INNER, block %llu, key {%u, %u}\n", n, k_val, k_len);
// 			}
			// redo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::inner_node & node = (btree::inner_node &)*pb;
			assert(node.m_map.find(rec.m_term) != node.m_map.end());
			node.m_used = node.m_used - rec.m_term->size() - 4 - 8;
			node.m_map.erase(rec.m_term);
			node.m_last = rec.m_new_last;
//			node.check_used();
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case SET_NEXT:
		{
			set_next_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::redo: SET_NEXT, block %llu\n", n);
			// redo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			assert(node.m_next != rec.m_new_value);
			node.m_next = rec.m_new_value;
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case SET_PREV:
		{
			set_prev_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::redo: SET_PREV, block %llu\n", n);
			// redo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::leaf_node & node = (btree::leaf_node &)*pb;
			assert(node.m_prev != rec.m_new_value);
			node.m_prev = rec.m_new_value;
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	case FREE_NODE:
		// nothing to redo
// 		printf("btree_rm::redo: FREE_NODE, block %llu\n", n);
		return;
	case SET_LAST:
		{
			set_last_rec rec;
			serial_buffer buf(data, size, true);
			rec.deserialize(buf);
// 			printf("btree_rm::redo: SET_LAST, block %llu\n", n);
			// redo operation
			decoded_block_ptr pb = m_bm->read(n, btree::node_decoder);
			btree::inner_node & node = (btree::inner_node &)*pb;
			assert(node.m_last != rec.m_new_value);
			node.m_last = rec.m_new_value;
			// save node
			m_bm->write(n, pb, redo_lsn);
		}
		return;
	default:
		assert(false);
	}
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree_rm::recover_finished() {
	m_ignore_undo = UINT64_MAX;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
btree * btree::create(trans_mgr * tm, buffer_mgr * bm, stm * stm, btree_rm * rm) {
	uint32_t id = tm->begin_transaction(soft);
	uint64_t root_n = stm->alloc();
	std::auto_ptr<btree> tree(open(tm, bm, stm, rm, root_n));
	leaf_node * root = new leaf_node(root_n);
	decoded_block_ptr pb(root);
	new_node_rec rec;
	serial_buffer buf;
	rec.serialize(buf, *root);
	uint64_t redo_lsn = tm->log_update(rm, root_n, buf.data(), buf.size());
	bm->write(root_n, pb, redo_lsn);
	tm->commit(id);
	return tree.release();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
btree * btree::open(trans_mgr * tm, buffer_mgr * bm, stm * stm, btree_rm * rm, uint64_t root) {
	std::auto_ptr<btree> tree(new btree());
	tree->m_root = root;
	tree->m_tm = tm;
	tree->m_bm = bm;
	tree->m_stm = stm;
	tree->m_rm = rm;
	// Use inner node scheme for calculating of max chunk size, because leaf node stores
	// key-value pairs only and pointers to next and prev node are less than 3 pointers
	// used max chunk size calculation.
	tree->m_max_chunk_size = (bm->block_size() - 4 - 8 * 3) / 2;
	// Make sure that max chunk size allows to store two encoded indirect pointers of
	// key-value pair. Max space required for such pointer is 12 bytes:
	// <<IsIndirect:1, Size:31, Pointer:64>>.
	if (tree->m_max_chunk_size < 12 * 2) {
		throw block_too_small();
	}
	return tree.release();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree::insert(const term_ptr & k, const term_ptr & v) {
	// first version does not support arbitrary size of key-value pair
	if (k->size() + v->size() + 4 + 4 > m_max_chunk_size) {
		throw key_value_pair_too_big();
	}
	uint32_t id = m_tm->begin_transaction(soft);
	decoded_block_ptr pb = m_bm->read(m_root, node_decoder);
	node_t & root = (node_t &)*pb;
	uint64_t redo_lsn = root.insert(this, k, v);
	if (redo_lsn != 0) {
		if (root.m_used <= m_bm->block_size()) {
			m_bm->write(m_root, pb, redo_lsn);
		} else {
			// lock too big node in cache and prevent it from being
			// flushed by buffer manager
			m_bm->lock(m_root);
			// log update BEFORE real node change, because if node is dirty, it can be
			// flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			split_node_rec split_rec;
			serial_buffer buf;
			split_rec.serialize(buf, root);
			redo_lsn = m_tm->log_update(m_rm, m_root, buf.data(), buf.size());
			// split root node such that it remains in place
			uint64_t left = m_stm->alloc();
			uint64_t right = m_stm->alloc();
			// split old root
			term_ptr term = root.split(this, m_bm, left, right);
			// create new root
			inner_node * new_root = new inner_node(m_root);
			decoded_block_ptr new_pb(new_root);
			new_root->m_used += term->size() + 4 + 8 + 8;
			new_root->m_last = right;
			new_root->m_map[term] = left;
//			new_root->check_used();
			// log new root creation
			new_node_rec new_rec;
			buf.rewind();
			new_rec.serialize(buf, *new_root);
			redo_lsn = m_tm->log_update(m_rm, m_root, buf.data(), buf.size());
			m_bm->write(m_root, new_pb, redo_lsn);
		}
	}
	m_tm->commit(id);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
uint64_t btree::leaf_node::insert(const btree * tree, const term_ptr & k,
																	const term_ptr & v) {
// 	{
// 		uint32_t k_len;
// 		uint32_t k_val = decode_term(k, k_len);
// 		printf("btree::leaf_node::insert: key {%u, %u}, block %llu\n", k_val, k_len, m_block);
// 	}
	map_t::iterator it = m_map.find(k);
	if (it != m_map.end()) {
		// update existing key
		term_ptr & old_v = it->second;
		// log update BEFORE real node change, because if node is dirty, it can be
		// flushed by buffer manager on log_update, if transaction manager decide
		// to take checkpoint.
		update_leaf_rec rec;
		serial_buffer buf;
		rec.serialize(buf, k, old_v, v);
		uint64_t redo_lsn = tree->m_tm->log_update(tree->m_rm, m_block, buf.data(), buf.size());
		// perform update
		m_used = m_used - old_v->size() + v->size();
		it->second = v;
//		check_used();
		return redo_lsn;
	}
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	insert_leaf_rec rec;
	serial_buffer buf;
	rec.serialize(buf, k, v);
	uint64_t redo_lsn = tree->m_tm->log_update(tree->m_rm, m_block, buf.data(), buf.size());
	// insert new key
	m_used += k->size() + v->size() + 4 + 4;
	m_map[k] = v;
//	check_used();
	return redo_lsn;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
uint64_t btree::inner_node::insert(const btree * tree, const term_ptr & k,
															 const term_ptr & v) {
	map_t::iterator i = m_map.upper_bound(k);
	uint64_t n;
	if (i == m_map.end()) {
		// new key is bigger than all node keys.
		n = m_last;
	} else {
		n = i->second;
	}
	decoded_block_ptr pb = tree->m_bm->read(n, node_decoder);
	node_t & node = (node_t &)*pb;
	uint64_t redo_lsn = node.insert(tree, k, v);
	if (redo_lsn == 0) {
		// nothing to do
		return 0;
	}
	if (node.m_used <= tree->m_bm->block_size()) {
		// just save dirty node
		tree->m_bm->write(n, pb, redo_lsn);
		return 0;
	}
	// node too big, split it.
	// lock too big node in cache and prevent it from being
	// flushed by buffer manager
	tree->m_bm->lock(n);
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	split_node_rec split_rec;
	serial_buffer buf;
	split_rec.serialize(buf, node);
	redo_lsn = tree->m_tm->log_update(tree->m_rm, n, buf.data(), buf.size());
	if (n == m_last) {
		// split biggest node
		// split biggest node
		uint64_t right = tree->m_stm->alloc();
		// perform split
		term_ptr term = node.split(tree, tree->m_bm, n, right);
		// log update BEFORE real node change, because if node is dirty, it can be
		// flushed by buffer manager on log_update, if transaction manager decide
		// to take checkpoint.
		insert_inner_rec ins_rec;
		buf.rewind();
		ins_rec.serialize(buf, term, n, m_last, right);
		redo_lsn = tree->m_tm->log_update(tree->m_rm, m_block, buf.data(), buf.size());
		// perform update
		m_map[term] = n;
		m_last = right;
		m_used += term->size() + 4 + 8;
//		check_used();
		return redo_lsn;
	}
	// split middle node, n points on smaller node.
	uint64_t left = tree->m_stm->alloc();
	// perform split
	term_ptr term = node.split(tree, tree->m_bm, left, n);
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	insert_inner_rec ins_rec;
	buf.rewind();
	ins_rec.serialize(buf, term, left, m_last, m_last);
	redo_lsn = tree->m_tm->log_update(tree->m_rm, m_block, buf.data(), buf.size());
	// perform update
	m_map[term] = left;
	m_used += term->size() + 4 + 8;
//	check_used();
	return redo_lsn;
}

//--------------------------------------------------------------------
// internal function
// Split leaf node.
//--------------------------------------------------------------------
term_ptr btree::leaf_node::split(const btree * tree, buffer_mgr * bm,
																 uint64_t left_n, uint64_t right_n) {
//	check_used();
	assert(m_used > bm->block_size());
	assert(m_map.size() >= 3);
	// create left node
	leaf_node * left = new leaf_node(left_n);
	decoded_block_ptr left_ptr(left);
	left->m_next = right_n;
	left->m_prev = m_prev;
	serial_buffer buf;
	if (right_n == m_block && m_prev != UINT64_MAX) {
		// fix next pointer in previos node
		decoded_block_ptr pb = bm->read(m_prev, node_decoder);
		leaf_node & prev = (leaf_node &)*pb;
		assert(prev.m_next == m_block);
		// log update BEFORE real node change, because if node is dirty, it can be
		// flushed by buffer manager on log_update, if transaction manager decide
		// to take checkpoint.
		set_next_rec rec;
		buf.rewind();
		rec.serialize(buf, m_block, left_n);
		uint64_t redo_lsn = tree->m_tm->log_update(tree->m_rm, m_prev, buf.data(), buf.size());
		// fix pointer
		prev.m_next = left_n;
		bm->write(m_prev, pb, redo_lsn);
	}
	// create right node
	leaf_node * right = new leaf_node(right_n);
	decoded_block_ptr right_ptr(right);
	right->m_next = m_next;
	right->m_prev = left_n;
	if (left_n == m_block && m_next != UINT64_MAX) {
		// fix previos pointer in next node
		decoded_block_ptr pb = bm->read(m_next, node_decoder);
		leaf_node & next = (leaf_node &)*pb;
		assert(next.m_prev == m_block);
		// log update BEFORE real node change, because if node is dirty, it can be
		// flushed by buffer manager on log_update, if transaction manager decide
		// to take checkpoint.
		set_prev_rec rec;
		buf.rewind();
		rec.serialize(buf, m_block, right_n);
		uint64_t redo_lsn = tree->m_tm->log_update(tree->m_rm, m_next, buf.data(), buf.size());
		// fix pointer
		next.m_prev = right_n;
		bm->write(m_next, pb, redo_lsn);
	}
	// fill left node
	uint32_t boundary = bm->block_size() / 2;
	assert(boundary > 0);
	map_t::const_iterator i = m_map.begin();
	while (left->m_used < boundary) {
		const term_ptr & k = i->first;
		const term_ptr & v = i->second;
// 		{
// 			uint32_t k_len;
// 			uint32_t k_val = decode_term(k, k_len);
// 			printf("btree::leaf_node::split(%llu): key {%u, %u} goes to block %llu\n", m_block, k_val, k_len, left_n);
// 		}
		left->m_map[k] = v;
		left->m_used += k->size() + v->size() + 4 + 4;
		i++;
	}
	// make sure that left node will not be empty
	assert(left->m_map.size() > 0);
	// get pop up term
	term_ptr term = i->first;
	// fill right node
	while (i != m_map.end()) {
		const term_ptr & k = i->first;
		const term_ptr & v = i->second;
// 		{
// 			uint32_t k_len;
// 			uint32_t k_val = decode_term(k, k_len);
// 			printf("btree::leaf_node::split(%llu): key {%u, %u} goes to block %llu\n", m_block, k_val, k_len, right_n);
// 		}
		right->m_map[k] = v;
		right->m_used += k->size() + v->size() + 4 + 4;
		i++;
	}
	// make sure that right node will not be empty
	assert(right->m_map.size() > 0);
	// save new nodes: split log record must be written before calling
	// this function.
//	left->check_used();
//	right->check_used();
	// log left node creation
	new_node_rec rec;
	buf.rewind();
	rec.serialize(buf, *left);
	uint64_t redo_lsn = tree->m_tm->log_update(tree->m_rm, left_n, buf.data(), buf.size());
	// save left node
	bm->write(left_n, left_ptr, redo_lsn);
	// log right node creation
	buf.rewind();
	rec.serialize(buf, *right);
	redo_lsn = tree->m_tm->log_update(tree->m_rm, right_n, buf.data(), buf.size());
	bm->write(right_n, right_ptr, redo_lsn);
	return term;
}

//--------------------------------------------------------------------
// internal function
// Split inner node.
//--------------------------------------------------------------------
term_ptr btree::inner_node::split(const btree * tree, buffer_mgr * bm, uint64_t left_n, uint64_t right_n) {
//	check_used();
	assert(m_used > bm->block_size());
	assert(m_map.size() >= 3);
	// create left node
	inner_node * left = new inner_node(left_n);
	decoded_block_ptr left_ptr(left);
	// create right node
	inner_node * right = new inner_node(right_n);
	decoded_block_ptr right_ptr(right);
	// fill left node
	uint32_t boundary = bm->block_size() / 2;
	assert(boundary > 0);
	map_t::const_iterator i = m_map.begin();
	while (left->m_used < boundary && left->m_map.size() < m_map.size() - 2) {
		const term_ptr & k = i->first;
		uint64_t less_ptr = i->second;
		left->m_map[k] = less_ptr;
		left->m_used += k->size() + 4 + 8;
		i++;
	}
	// make sure that left node is not empty
	assert(left->m_map.size() > 0);
	// get pop up term and set last ptr in left node
	term_ptr term = i->first;
	left->m_last = i->second;
	left->m_used += 8;
	i++;
	// fill right node
	while (i != m_map.end()) {
		const term_ptr & k = i->first;
		uint64_t less_ptr = i->second;
		right->m_map[k] = less_ptr;
		right->m_used += k->size() + 4 + 8;
		i++;
	}
	// make sure that right node is not empty
	assert(right->m_map.size() > 0);
	// set last ptr in right node
	right->m_last = m_last;
	right->m_used += 8;
	// save new nodes: split log record must be written before calling
	// this function.
//	left->check_used();
//	right->check_used();
	new_node_rec rec;
	serial_buffer buf;
	rec.serialize(buf, *left);
	uint64_t redo_lsn = tree->m_tm->log_update(tree->m_rm, left_n, buf.data(), buf.size());
	bm->write(left_n, left_ptr, redo_lsn);
	buf.rewind();
	rec.serialize(buf, *right);
	redo_lsn = tree->m_tm->log_update(tree->m_rm, right_n, buf.data(), buf.size());
	bm->write(right_n, right_ptr, redo_lsn);
	return term;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
decoded_block_ptr btree::node_decoder(uint64_t n, serial_buffer & buf) {
	uint32_t keys = *((uint32_t *)buf.curr_get_data());
	node_t * node;
	// create node
	if ((keys & 0x80000000) == 0) {
		// inner node
		node = new inner_node(n);
	} else {
		// leaf node
		node = new leaf_node(n);
	}
	decoded_block_ptr pb(node);
	node->deserialize(buf);
	return pb;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void btree::leaf_node::serialize(serial_buffer & buf) {
	uint32_t keys = 0x80000000 | m_map.size();
	buf.put(keys);
	buf.put(m_prev);
	buf.put(m_next);
	for (map_t::iterator i = m_map.begin(); i != m_map.end(); i++) {
		const term_ptr & k = i->first;
		buf.put(k->size());
		buf.put(k->data(), k->size());
		const term_ptr & v = i->second;
		buf.put(v->size());
		buf.put(v->data(), v->size());
	}
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void btree::leaf_node::deserialize(serial_buffer & buf) {
	m_used = buf.curr_get_offset();
	uint32_t keys;
	buf.get(keys);
	keys ^= 0x80000000;
	buf.get(m_prev);
	buf.get(m_next);
	for (uint32_t i = 0; i < keys; i++) {
		uint32_t size;
		buf.get(size);
		const void * data = buf.get_ptr(size);
		term_ptr k(new term(data, size));
		buf.get(size);
		data = buf.get_ptr(size);
		term_ptr v(new term(data, size));
		m_map[k] = v;
	}
	m_used = buf.curr_get_offset() - m_used;
//	check_used();
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void btree::inner_node::serialize(serial_buffer & buf) {
	buf.put((uint32_t)m_map.size());
	for (map_t::iterator i = m_map.begin(); i != m_map.end(); i++) {
		uint64_t less_ptr = i->second;
		buf.put(less_ptr);
		const term_ptr & k = i->first;
		buf.put(k->size());
		buf.put(k->data(), k->size());
	}
	buf.put(m_last);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
void btree::inner_node::deserialize(serial_buffer & buf) {
	m_used = buf.curr_get_offset();
	uint32_t keys;
	buf.get(keys);
	for (uint32_t i = 0; i < keys; i++) {
		uint64_t less_ptr;
		buf.get(less_ptr);
		uint32_t size;
		buf.get(size);
		const void * data = buf.get_ptr(size);
		term_ptr k(new term(data, size));
		m_map[k] = less_ptr;
	}
	buf.get(m_last);
	m_used = buf.curr_get_offset() - m_used;
//	check_used();
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
result btree::lookup(const term_ptr & k, term_ptr & v) {
	uint32_t id = m_tm->begin_transaction(read_only);
	decoded_block_ptr pb = m_bm->read(m_root, node_decoder);
	node_t & root = (node_t &)*pb;
	result res = root.lookup(m_bm, k, v);
	m_tm->commit(id);
	return res;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
result btree::leaf_node::lookup(buffer_mgr * bm, const term_ptr & k, term_ptr & v) {
	map_t::iterator it = m_map.find(k);
	if (it == m_map.end()) {
		return not_found;
	}
	v = it->second;
	return ok;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
result btree::inner_node::lookup(buffer_mgr * bm, const term_ptr & k, term_ptr & v) {
	map_t::iterator i = m_map.upper_bound(k);
	uint64_t n;
	if (i == m_map.end()) {
		// key is bigger than all node keys.
		n = m_last;
	} else {
		n = i->second;
	}
	decoded_block_ptr pb = bm->read(n, node_decoder);
	node_t & node = (node_t &)*pb;
	return node.lookup(bm, k, v);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree::remove(const term_ptr & k) {
	uint32_t id = m_tm->begin_transaction(soft);
	decoded_block_ptr pb = m_bm->read(m_root, node_decoder);
	node_t * root = (node_t *)pb.get();
	uint64_t redo_lsn = root->remove(this, k);
	if (redo_lsn > 0) {
		// save dirty root
		m_bm->write(m_root, pb, redo_lsn);
		// roll up root
		uint64_t last;
		serial_buffer buf;
		while (root->can_roll_up(last)) {
			pb = m_bm->read(last, node_decoder);
			root = (node_t *)pb.get();
			// log update BEFORE real node change, because if node is dirty, it can be
			// flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			free_node_rec free_rec;
			buf.rewind();
			free_rec.serialize(buf, *root);
			m_tm->log_update(m_rm, last, buf.data(), buf.size());
			// save read block as new root, copy node before modifing, because it is not
			// COW pointer and original block in cache will be modified.
			root = root->copy();
			pb = decoded_block_ptr(root);
			root->m_block = m_root;
			// log update BEFORE real node change, because if node is dirty, it can be
			// flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			rollup_root_rec roll_rec;
			buf.rewind();
			roll_rec.serialize(buf, *root, last);
			redo_lsn = m_tm->log_update(m_rm, m_root, buf.data(), buf.size());
			// save new root and free rolled up node
			m_bm->write(m_root, pb, redo_lsn);
			m_stm->free(last);
		}
	}
	m_tm->commit(id);
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool btree::leaf_node::can_roll_up(uint64_t & last) {
	return false;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool btree::inner_node::can_roll_up(uint64_t & last) {
	if (m_map.empty()) {
		last = m_last;
		return true;
	}
	return false;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
uint64_t btree::leaf_node::remove(const btree * tree, const term_ptr & k) {
	map_t::iterator it = m_map.find(k);
	if (it == m_map.end()) {
		// nothing to delete
		return 0;
	}
// 	{
// 		uint32_t k_len;
// 		uint32_t k_val = decode_term(k, k_len);
// 		printf("btree::leaf_node::remove: key {%u, %u}, block %llu\n", k_val, k_len, m_block);
// 	}
	// delete key
	const term_ptr & stored_k = it->first;
	const term_ptr & v = it->second;
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	remove_leaf_rec rec;
	serial_buffer buf;
	rec.serialize(buf, stored_k, v);
	uint64_t redo_lsn = tree->m_tm->log_update(tree->m_rm, m_block, buf.data(), buf.size());
	// perform update
	m_used -= stored_k->size() + v->size() + 4 + 4;
	m_map.erase(it);
	if (tree != NULL && m_map.empty()) {
		// remove node from linked list of nodes
		if (m_prev != UINT64_MAX) {
			// update previos node
			decoded_block_ptr pb = tree->m_bm->read(m_prev, node_decoder);
			leaf_node & prev = (leaf_node &)*pb;
			assert(prev.m_next == m_block);
			// log update BEFORE real node change, because if node is dirty, it can be
			// flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			set_next_rec rec;
			buf.rewind();
			rec.serialize(buf, m_block, m_next);
			uint64_t redo_lsn_1 = tree->m_tm->log_update(tree->m_rm, m_prev, buf.data(), buf.size());
			// fix pointer
			prev.m_next = m_next;
			tree->m_bm->write(m_prev, pb, redo_lsn_1);
		}
		if (m_next != UINT64_MAX) {
			// update next node
			decoded_block_ptr pb = tree->m_bm->read(m_next, node_decoder);
			leaf_node & next = (leaf_node &)*pb;
			assert(next.m_prev == m_block);
			// log update BEFORE real node change, because if node is dirty, it can be
			// flushed by buffer manager on log_update, if transaction manager decide
			// to take checkpoint.
			set_prev_rec rec;
			buf.rewind();
			rec.serialize(buf, m_block, m_prev);
			uint64_t redo_lsn_1 = tree->m_tm->log_update(tree->m_rm, m_next, buf.data(), buf.size());
			// fix pointer
			next.m_prev = m_prev;
			tree->m_bm->write(m_next, pb, redo_lsn_1);
		}
	}
//	check_used();
	return redo_lsn;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
uint64_t btree::inner_node::remove(const btree * tree, const term_ptr & k) {
	map_t::iterator i = m_map.upper_bound(k);
	uint64_t n;
	if (i == m_map.end()) {
		// new key is bigger than all node keys.
		n = m_last;
	} else {
		n = i->second;
	}
	decoded_block_ptr pb = tree->m_bm->read(n, node_decoder);
	node_t & node = (node_t &)*pb;
	uint64_t redo_lsn = node.remove(tree, k);
	if (redo_lsn == 0) {
		// nothing to do
		return 0;
	}
	// save dirty node
	tree->m_bm->write(n, pb, redo_lsn);
	if (!node.can_free()) {
		return 0;
	}
	// log free operation
	free_node_rec free_rec;
	serial_buffer buf;
	free_rec.serialize(buf, node);
	tree->m_tm->log_update(tree->m_rm, n, buf.data(), buf.size());
	// free node
	tree->m_stm->free(n);
	// update inner node
	if (m_map.empty()) {
		// node is empty and last pointer can be deleted
		// log update BEFORE real node change, because if node is dirty, it can be
		// flushed by buffer manager on log_update, if transaction manager decide
		// to take checkpoint.
		set_last_rec rec;
		buf.rewind();
		rec.serialize(buf, m_last, UINT64_MAX);
		redo_lsn = tree->m_tm->log_update(tree->m_rm, m_block, buf.data(), buf.size());
		// perform update
		m_last = UINT64_MAX;
		return redo_lsn;
	}
	// node is not empty
	uint64_t old_last;
	uint64_t new_last;
	uint64_t less_ptr;
	if (n == m_last) {
		old_last = m_last;
		i--;
		new_last = i->second;
		less_ptr = i->second;
	} else {
		old_last = new_last = m_last;
		less_ptr = n;
	}
	// remove pointer on freed node
	const term_ptr & t = i->first;
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	remove_inner_rec rm_rec;
	buf.rewind();
	rm_rec.serialize(buf, t, less_ptr, old_last, new_last);
	redo_lsn = tree->m_tm->log_update(tree->m_rm, m_block, buf.data(), buf.size());
	// perform update
	m_last = new_last;
	m_used = m_used - t->size() - 4 - 8;
	m_map.erase(i);
//	check_used();
	return redo_lsn;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool btree::leaf_node::can_free() {
	return m_map.empty();
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool btree::inner_node::can_free() {
	return m_map.empty() && m_last == UINT64_MAX;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
btree::node_t * btree::leaf_node::copy() {
	leaf_node * node = new leaf_node(m_block);
	*node = *this;
	node->reset();
	return node;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
btree::node_t * btree::inner_node::copy() {
	inner_node * node = new inner_node(m_block);
	*node = *this;
	node->reset();
	return node;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool btree::leaf_node::is_leaf() {
	return true;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
bool btree::inner_node::is_leaf() {
	return false;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
btree::iterator btree::begin() {
	uint32_t id = m_tm->begin_transaction(read_only);
	iterator it;
	it.m_bm = m_bm;
	leaf_node * node = NULL;
	decoded_block_ptr pb;
	uint64_t n = m_root;
	// find first leaf node
	do {
		pb = m_bm->read(n, node_decoder);
		node_t * tmp = (node_t *)pb.get();
		if (tmp->is_leaf()) {
			node = (leaf_node *)tmp;
		} else {
			inner_node * inner = (inner_node *)tmp;
			if (inner->m_map.empty()) {
				n = inner->m_last;
			} else {
				n = inner->m_map.begin()->second;
			}
		}
	} while (node == NULL);
	// init iterator
	if (node->m_map.empty()) {
		it.m_pb = decoded_block_ptr();
	} else {
		it.m_pb = pb;
		it.m_map_it = node->m_map.begin();
		it.m_curr = *it.m_map_it;
	}
	m_tm->commit(id);
	return it;
}

//--------------------------------------------------------------------
// internal function
//--------------------------------------------------------------------
decoded_block_ptr btree::lookup(const term_ptr &k) {
	leaf_node * node = NULL;
	decoded_block_ptr pb;
	uint64_t n = m_root;
	do {
		pb = m_bm->read(n, node_decoder);
		node_t * tmp = (node_t *)pb.get();
		if (tmp->is_leaf()) {
			node = (leaf_node *)tmp;
		} else {
			inner_node * inner = (inner_node *)tmp;
			inner_node::map_t::iterator i = inner->m_map.upper_bound(k);
			if (i == inner->m_map.end()) {
				// key is bigger than all node keys.
				n = inner->m_last;
			} else {
				n = i->second;
			}
		}
	} while (node == NULL);
	return pb;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
btree::iterator btree::seek(const term_ptr & k, position how) {
	uint32_t id = m_tm->begin_transaction(read_only);
	btree::iterator it;
	it.m_bm = m_bm;
	switch (how) {
	case prev:
		{
			decoded_block_ptr pb = lookup(k);
			leaf_node * node = (leaf_node *)pb.get();
			leaf_node::map_t::iterator map_it = node->m_map.lower_bound(k);
			if (map_it == node->m_map.begin()) {
				uint64_t n = node->m_prev;
				if (n == UINT64_MAX) {
					map_it = node->m_map.end();
				} else {
					pb = m_bm->read(n, node_decoder);
					node = (leaf_node *)pb.get();
					map_it = node->m_map.end();
					map_it--;
				}
			} else {
				map_it--;
			}
			if (map_it == node->m_map.end()) {
				it.m_pb = decoded_block_ptr();
			} else {
				it.m_pb = pb;
				it.m_map_it = map_it;
				it.m_curr = *map_it;
			}
		}
		break;
	case next:
		{
			decoded_block_ptr pb = lookup(k);
			leaf_node * node = (leaf_node *)pb.get();
			leaf_node::map_t::iterator map_it = node->m_map.upper_bound(k);
			if (map_it == node->m_map.end()) {
				uint64_t n = node->m_next;
				if (n != UINT64_MAX) {
					pb = m_bm->read(n, node_decoder);
					node = (leaf_node *)pb.get();
					map_it = node->m_map.begin();
				}
			}
			if (map_it == node->m_map.end()) {
				it.m_pb = decoded_block_ptr();
			} else {
				it.m_pb = pb;
				it.m_map_it = map_it;
				it.m_curr = *map_it;
			}
		}
		break;
	case exact:
		{
			decoded_block_ptr pb = lookup(k);
			leaf_node & node = (leaf_node &)*pb;
			leaf_node::map_t::iterator map_it = node.m_map.find(k);
			if (map_it == node.m_map.end()) {
				it.m_pb = decoded_block_ptr();
			} else {
				it.m_pb = pb;
				it.m_map_it = map_it;
				it.m_curr = *map_it;
			}
		}
		break;
	case exact_prev:
		{
			it = seek(k, exact);
			if (!it.has_more()) {
				it = seek(k, prev);
			}
		}
		break;
	case exact_next:
		{
			it = seek(k, exact);
			if (!it.has_more()) {
				it = seek(k, next);
			}
		}
		break;
	default:
		assert(false);
	}
	m_tm->commit(id);
	return it;
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree::drop() {
	uint32_t id = m_tm->begin_transaction(soft);
	decoded_block_ptr pb = m_bm->read(m_root, node_decoder);
	leaf_node & root = (leaf_node &)*pb;
	assert(root.is_leaf());
	assert(root.m_map.empty());
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	free_node_rec free_rec;
	serial_buffer buf;
	free_rec.serialize(buf, root);
	m_tm->log_update(m_rm, m_root, buf.data(), buf.size());
	m_stm->free(m_root);
	m_tm->commit(id);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree::clear() {
	uint32_t id = m_tm->begin_transaction(read_only);
	decoded_block_ptr pb = m_bm->read(m_root, node_decoder);
	node_t & root = (node_t &)*pb;
	m_tm->commit(id);
	if (!root.is_leaf()) {
		// root is inner node. Inner node handles transactions by itself.
		inner_node & inner = (inner_node &)root;
		inner.clear(*this, pb);
	}
	// finish clean up
	id = m_tm->begin_transaction(soft);
	// log update BEFORE real node change, because if node is dirty, it can be
	// flushed by buffer manager on log_update, if transaction manager decide
	// to take checkpoint.
	new_node_rec rec;
	serial_buffer buf;
	leaf_node * new_root = new leaf_node(m_root);
	pb = decoded_block_ptr(new_root);
	rec.serialize(buf, *new_root);
	uint64_t redo_lsn = m_tm->log_update(m_rm, m_root, buf.data(), buf.size());
	m_bm->write(m_root, pb, redo_lsn);
	m_tm->commit(id);
}

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void btree::inner_node::clear(btree & tree, decoded_block_ptr & my_pb) {
	if (m_last == UINT64_MAX) {
		// node already cleaned up
		return;
	}
	trans_mgr * tm = tree.m_tm;
	buffer_mgr * bm = tree.m_bm;
	stm * pstm = tree.m_stm;
	btree_rm * rm = tree.m_rm;
	uint32_t id = tm->begin_transaction(read_only);
	// read one node
	decoded_block_ptr pb = bm->read(m_last, node_decoder);
	node_t & node = (node_t &)*pb;
	tm->commit(id);
	if (node.is_leaf()) {
		leaf_node dummy_leaf(UINT64_MAX);	// dummy empty leaf
		serial_buffer buf;
		// free all leaf nodes in map
		for (map_t::iterator i = m_map.begin(); i != m_map.end();) {
			id = tm->begin_transaction(soft);
			term_ptr t = i->first;	// not reference
			uint64_t n = i->second;
			// log node freeing
			free_node_rec free_rec;
			buf.rewind();
			dummy_leaf.m_block = n;
			free_rec.serialize(buf, dummy_leaf);
			tm->log_update(rm, n, buf.data(), buf.size());
			// free leaf node
			pstm->free(n);
			// log removing
			remove_inner_rec remove_rec;
			buf.rewind();
			remove_rec.serialize(buf, t, n, m_last, m_last);
			uint64_t redo_lsn = tm->log_update(rm, m_block, buf.data(), buf.size());
			// remove term from map
			m_used = m_used - t->size() - 4 - 8;
			map_t::iterator j = i;
			i++;
			m_map.erase(j);
			// save node
//			check_used();
			bm->write(m_block, my_pb, redo_lsn);
			tm->commit(id);
		}
		// finish cleanup
		id = tm->begin_transaction(soft);
		// free last node
		free_node_rec free_rec;
		buf.rewind();
		dummy_leaf.m_block = m_last;
		free_rec.serialize(buf, dummy_leaf);
		tm->log_update(rm, m_last, buf.data(), buf.size());
		pstm->free(m_last);
		// log last ptr change
		set_last_rec last_rec;
		buf.rewind();
		last_rec.serialize(buf, m_last, UINT64_MAX);
		uint64_t redo_lsn = tm->log_update(rm, m_block, buf.data(), buf.size());
		// save node
		bm->write(m_block, my_pb, redo_lsn);
		tm->commit(id);
	} else {
		inner_node dummy_inner(UINT64_MAX);	// dummy empty inner
		serial_buffer buf;
		// all nodes below are inner nodes
		for (map_t::iterator i = m_map.begin(); i != m_map.end();) {
			term_ptr t = i->first;	// not reference
			uint64_t n = i->second;
			// read inner node
			id = tm->begin_transaction(read_only);
			decoded_block_ptr inner_pb = bm->read(n, node_decoder);
			inner_node & inner = (inner_node &)*inner_pb;
			tm->commit(id);
			// clear inner node
			inner.clear(tree, inner_pb);
			// remove clean node from map and save this node
			id = tm->begin_transaction(soft);
			// log node freeing
			free_node_rec free_rec;
			buf.rewind();
			dummy_inner.m_block = n;
			free_rec.serialize(buf, dummy_inner);
			tm->log_update(rm, n, buf.data(), buf.size());
			pstm->free(n);
			// log removing
			remove_inner_rec remove_rec;
			buf.rewind();
			remove_rec.serialize(buf, t, n, m_last, m_last);
			uint64_t redo_lsn = tm->log_update(rm, m_block, buf.data(), buf.size());
			// remove term from map
			m_used = m_used - t->size() - 4 - 8;
			map_t::iterator j = i;
			i++;
			m_map.erase(j);
			// save node
//			check_used();
			bm->write(m_block, my_pb, redo_lsn);
			tm->commit(id);
		}
		// clean last node
		inner_node & inner = (inner_node &)node;
		inner.clear(tree, pb);
		// finish cleanup
		id = tm->begin_transaction(soft);
		// free last node
		free_node_rec free_rec;
		buf.rewind();
		dummy_inner.m_block = m_last;
		free_rec.serialize(buf, dummy_inner);
		tm->log_update(rm, m_last, buf.data(), buf.size());
		pstm->free(m_last);
		// log last ptr change
		set_last_rec last_rec;
		buf.rewind();
		last_rec.serialize(buf, m_last, UINT64_MAX);
		uint64_t redo_lsn = tm->log_update(rm, m_block, buf.data(), buf.size());
		// save node
		bm->write(m_block, my_pb, redo_lsn);
		tm->commit(id);
	}
}

