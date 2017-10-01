///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : btree.hpp
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
#ifndef __btree_hpp__
#define __btree_hpp__

#include <map>

#include "stm.hpp"
#include "term.hpp"
#include "pointers.hpp"

//--------------------------------------------------------------------
// btree resource manager.
//--------------------------------------------------------------------
class btree_rm: public resource_mgr {
private:
	trans_mgr *					m_tm;
	buffer_mgr *				m_bm;
	uint64_t						m_ignore_undo;
	decoded_block_ptr		m_splitted;

public:
	//--------------------------------------------------------------------
	// Function: btree_rm().
	// Description: Constructor.
	//--------------------------------------------------------------------
	btree_rm() {
		m_tm = NULL;
		m_bm = NULL;
		m_ignore_undo = UINT64_MAX;
	}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the transaction manager.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {transaction_manager, Name} - transaction manager.
	//   {buffer_manager, Name} - buffer manager.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & trans_mgr,
										const std::string & buffer_mgr);

	// see base class
	virtual void stop();

	// see base class
	virtual void del_underlaying();

	// see base class
	virtual void stats(no_stats_list & list);

	// see base class
	virtual void undo(uint64_t n, const void * data, uint32_t size, void * opaque);

	// see base class
	virtual void redo(uint64_t n, bool state_only, const void * data, uint32_t size, uint64_t redo_lsn);

	// see base class
	virtual void recover_finished();
};

//--------------------------------------------------------------------
// position of B-tree seek operations
//--------------------------------------------------------------------
enum position {
	prev,						// set position before key
	next,						// set position after key
	exact,					// set position exact on key
	exact_prev,			// set exact position and if it is not possible, than
									// set prev position.
	exact_next			// set exact position and if it is not possible, than
									// set next position.
};

//--------------------------------------------------------------------
// btree class.
//--------------------------------------------------------------------
class btree {
	friend class btree_rm;

public:
	struct node_t;
	struct leaf_node;
	struct inner_node;
	class iterator;

private:
	uint64_t			m_root;								// block number of root node. Root node
																			// always stays in same block.
	trans_mgr *		m_tm;									// transaction manager
	buffer_mgr *	m_bm;									// buffer manager
	stm *					m_stm;								// storage manager
	btree_rm *		m_rm;									// B-tree resource manager
	uint32_t			m_max_chunk_size;			// Max size of binary encoded {K,V} pair
																			// stored immediately in node. Indirect
																			// storage used, if size of chunk exceeds
																			// the limit. The max chunk size is chosen
																			// according to following criterias:
																			// 1. Node is splitted when it can not fit
																			// a block.
																			// 2. In order to avoid creation of empty
																			// nodes on split, inner node must contain at
																			// least three keys at split point and
																			// leaf node - two keys.
																			// 3. So any two keys must not cause node
																			// split. Space for 3 pointers
																			// surrounding two keys in inner nodes
																			// must be reserved too.

	//--------------------------------------------------------------------
	// Function: btree().
	// Description: Constructor.
	//--------------------------------------------------------------------
	btree() {
		m_root = 0;
		m_tm = NULL;
		m_bm = NULL;
		m_stm = NULL;
		m_rm = NULL;
		m_max_chunk_size = 0;
	}

	//--------------------------------------------------------------------
	// Function: lookup(const term_ptr & k) -> decoded_block_ptr
	// Description: Returns leaf node where key can be.
	//--------------------------------------------------------------------
	decoded_block_ptr lookup(const term_ptr & k);

public:
	//--------------------------------------------------------------------
	// Function: ~btree().
	// Description: Destructor.
	//--------------------------------------------------------------------
	~btree() {}

	//--------------------------------------------------------------------
	// Function: create(trans_mgr * tm, buffer_mgr * bm, stm * stm, , btree_rm * rm) -> btree *
	// Description: Creates new B-tree. Use delete operator to close the tree.
	//--------------------------------------------------------------------
	static btree * create(trans_mgr * tm, buffer_mgr * bm, stm * stm, btree_rm * rm);

	//--------------------------------------------------------------------
	// Function: open(trans_mgr * tm, buffer_mgr * bm, stm * stm, btree_rm * rm, uint64_t root) -> btree *
	// Description: Opens existing B-tree with given root. Use delete operator to close the tree.
	//--------------------------------------------------------------------
	static btree * open(trans_mgr * tm, buffer_mgr * bm, stm * stm, btree_rm * rm, uint64_t root);

	//--------------------------------------------------------------------
	// Function: node_decoder(uint64_t n, serial_buffer & buf) -> decoded_block_ptr
	// Description: Decodes node from raw buffer.
	//--------------------------------------------------------------------
	static decoded_block_ptr node_decoder(uint64_t n, serial_buffer & buf);

	//--------------------------------------------------------------------
	// Function: root() -> uint64_t.
	// Description: Returns block number of root node.
	//--------------------------------------------------------------------
	uint64_t root() {return m_root;}

	//--------------------------------------------------------------------
	// Function: insert(const term_ptr & k, const term_ptr & v)
	// Description: Inserts Key-Value pair into B-tree. If Key already presented,
	// its value is overwritten.
	//--------------------------------------------------------------------
	void insert(const term_ptr & k, const term_ptr & v);

	//--------------------------------------------------------------------
	// Function: lookup(const term_ptr & k, term_ptr & v) -> ok | not_found
	// Description: Searches B-tree for key.
	//--------------------------------------------------------------------
	result lookup(const term_ptr & k, term_ptr & v);

	//--------------------------------------------------------------------
	// Function: remove(const term_ptr & k)
	// Description: Removes key from B-tree.
	//--------------------------------------------------------------------
	void remove(const term_ptr & k);

	//--------------------------------------------------------------------
	// Function: begin() -> iterator
	// Description: Returns an iterator that can be used for traversing the entries
	// of B-tree with iterator++ operator. Iterator position is first tree element.
	// The function must be called inside of transaction.
	//--------------------------------------------------------------------
	iterator begin();

	//--------------------------------------------------------------------
	// Function: seek(const term_ptr & k, position how) -> iterator
	// Description: Returns iterator according to required position. If position
	// can not be set, than end iterator is returned.
	// The function must be called inside of transaction.
	//--------------------------------------------------------------------
	iterator seek(const term_ptr & k, position how);

	//--------------------------------------------------------------------
	// Function: max_chunk_size() -> uint32_t
	// Description: Returns max chunk size of key value pair.
	//--------------------------------------------------------------------
	uint32_t max_chunk_size() {return m_max_chunk_size - 4 - 4;}

	//--------------------------------------------------------------------
	// Function: clear()
	// Description: Clears B-tree in fast way by freeing all blocks and
	// leaving only empty root block. DON'T CALL THIS FUNCTION INSIDE
	// TRANSACTION, because large trees can cause log overflow. Short
	// transactions are handled inside of this function. Since operation
	// consists multiple non-nested transactions and original blocks are
	// not logged before freeing, it can not be rolled back and B-tree
	// becomes unusable. Automatic restart of interrupted operation is
	// not supported too. And interrupted operation must be restarted manually.
	// Caller of this function MUST take care that NO OTHRER THREADS can
	// access the B-tree untill cleanup finished completely.
	//--------------------------------------------------------------------
	void clear();

	//--------------------------------------------------------------------
	// Function: drop()
	// Description: Removes empty B-tree, call clear before drop. Drop
	// function can be called inside transaction.
	//--------------------------------------------------------------------
	void drop();
};

//--------------------------------------------------------------------
// base node type
//--------------------------------------------------------------------
struct btree::node_t: public decoded_block {
	uint64_t		m_block;			// block number
	uint32_t		m_used;				// number of bytes used by node data

private:
	node_t();

public:
	node_t(uint64_t n) {
		m_block = n;
		m_used = 0;
	}

	// deserialization routine
	virtual void deserialize(serial_buffer & buf) = 0;

	// Inserts key-value pair into node. Returns redo LSN or 0 if node is not
	// modified.
	virtual uint64_t insert(const btree * tree, const term_ptr & k, const term_ptr & v) = 0;

	// Splits node
	virtual term_ptr split(const btree * tree, buffer_mgr * bm, uint64_t left, uint64_t right) = 0;

	// Searches node for key
	virtual result lookup(buffer_mgr * bm, const term_ptr & k, term_ptr & v) = 0;

	// Deletes key from node. Returns redo LSN or 0 if node is not
	// modified.
	virtual uint64_t remove(const btree * tree, const term_ptr & k) = 0;

	// Returns true if node can be rolled up, in order to reduce tree height
	virtual bool can_roll_up(uint64_t & last) = 0;

	// Returns true if node can be freed. Only leaf nodes may return true. Inner nodes
	// are freed by roll up procedure.
	virtual bool can_free() = 0;

	// Makes copy of block
	virtual node_t * copy() = 0;

	// Returns true, if node is leaf
	virtual bool is_leaf() = 0;
};

//--------------------------------------------------------------------
// Leaf node. Stores {Key, Value} pairs (erlang binary format):
//    <<Key/binary,Value/binary>>.
// Both Key and Value has following format:
// <<Indirect:1, Size:31, Data/binary>>
// If Indirect flag is 0, than Data is:
//    <<Term:Size/binary>>.
// If Indirect flag is 1, than Data is:
//    <<Ptr:64>>.
// Ptr is block number of head of list a blocks containing actual data.
// Each block except last has 64-bits pointer on next block. Last block
// is used completely.
//--------------------------------------------------------------------
struct btree::leaf_node: public btree::node_t {
	// leaf map: Key => Value
	typedef std::map<term_ptr, term_ptr, term::less>	map_t;

	uint64_t	m_prev;			// pointer to prev block
	uint64_t	m_next;			// pointer to next block
	map_t			m_map;			// keys map

private:
	leaf_node();

public:
	leaf_node(uint64_t n): node_t(n) {
		m_prev = m_next = UINT64_MAX;
		// Space reserved in each leaf node:
		//   4 - number of keys in node, encoded as <<IsLeaf:1, Keys:31>>.
		//   8 - pointer to prev node
		//   8 - pointer to next node
		m_used = 4 + 8 + 8;
	}

	// see base class
	virtual void serialize(serial_buffer & buf);

	// see base class
	virtual void deserialize(serial_buffer & buf);

	// see base class
	virtual uint64_t insert(const btree * tree, const term_ptr & k, const term_ptr & v);

	// see base class
	virtual term_ptr split(const btree * tree, buffer_mgr * bm, uint64_t left, uint64_t right);

	// see base class
	virtual result lookup(buffer_mgr * bm, const term_ptr & k, term_ptr & v);

	// see base class
	virtual uint64_t remove(const btree * tree, const term_ptr & k);

	// see base class
	virtual bool can_roll_up(uint64_t & last);

	// see base class
	virtual bool can_free();

	// see base class
	virtual node_t * copy();

	// see base class
	virtual bool is_leaf();

/*
	void check_used() {
		uint32_t used = 4 + 8 + 8;
		for (map_t::iterator i = m_map.begin(); i != m_map.end(); i++) {
			const term_ptr & k = i->first;
			const term_ptr & v = i->second;
			used += k->size() + v->size() + 4 + 4;
		}
		assert(used == m_used);
	}
*/
};

//--------------------------------------------------------------------
// Inner node. Stores {P, Key} pairs in chunks (erlang binary format):
//    <<SmallPtr:64, Key/binary, BigPtr:64>>.
// SmallPtr is pointer on node where all keys are less than Key.
// BigPtr is pointer to node where all keys are equal or greater than Key.
// Key has same format as leaf node.
//--------------------------------------------------------------------
struct btree::inner_node: public btree::node_t {
	// inner map: Key => LessPtr where
	//    LessPtr - pointer on node with keys less than Key
	typedef std::map<term_ptr, uint64_t, term::less>	map_t;

	uint64_t	m_last;			// last pointer - pointer to node, where all
												// keys are bigger than keys in node.
	map_t			m_map;

private:
	inner_node();

public:
	inner_node(uint64_t n): node_t(n) {
		m_last = UINT64_MAX;
		// Space reserved in each inner node:
		//   4 - number of keys in node, encoded as <<IsLeaf:1, Keys:31>>.
		m_used = 4;
	}

	// see base class
	virtual void serialize(serial_buffer & buf);

	// see base class
	virtual void deserialize(serial_buffer & buf);

	// see base class
	virtual uint64_t insert(const btree * tree, const term_ptr & k, const term_ptr & v);

	// see base class
	virtual term_ptr split(const btree * tree, buffer_mgr * bm, uint64_t left, uint64_t right);

	// see base class
	virtual result lookup(buffer_mgr * bm, const term_ptr & k, term_ptr & v);

	// see base class
	virtual uint64_t remove(const btree * tree, const term_ptr & k);

	// see base class
	virtual bool can_roll_up(uint64_t & last);

	// see base class
	virtual bool can_free();

	// see base class
	virtual node_t * copy();

	// see base class
	virtual bool is_leaf();

	// clears inner node, see btree::clear for details
	void clear(btree & tree, decoded_block_ptr & my_pb);

/*
	void check_used() {
		uint32_t used = 4;
		for (map_t::iterator i = m_map.begin(); i != m_map.end(); i++) {
			const term_ptr & k = i->first;
			used += k->size() + 4 + 8;
		}
		used += 8;
		assert(used == m_used);
	}
*/
};

//--------------------------------------------------------------------
// B-tree iterator. Iterator is constant, nor insert or delete opeartions
// can be performed when iterator is active.
//--------------------------------------------------------------------
class btree::iterator {
	friend class btree;

private:
	leaf_node::map_t::iterator 	m_map_it;
	decoded_block_ptr						m_pb;
	buffer_mgr *								m_bm;
	std::pair<term_ptr, term_ptr>		m_curr;

	void increment() {
		leaf_node * node = (leaf_node *)m_pb.get();
		m_map_it++;
		if (m_map_it != node->m_map.end()) {
			m_curr = *m_map_it;
		} else {
			if (node->m_next == UINT64_MAX) {
				m_pb = decoded_block_ptr();
				m_curr = std::pair<term_ptr, term_ptr>();
			} else {
				m_pb = m_bm->read(node->m_next, btree::node_decoder);
				leaf_node * node = (leaf_node *)m_pb.get();
				m_map_it = node->m_map.begin();
				m_curr = *m_map_it;
			}
		}
	}

public:
	iterator() {m_bm = NULL;}

	~iterator() {}

	const std::pair<term_ptr, term_ptr> & operator * () {
		return m_curr;
	}

	const std::pair<term_ptr, term_ptr> * operator -> () {
		return &m_curr;
	}

	iterator & operator ++ () {
		increment();
		return *this;
	}

	iterator & operator ++ (int) {
		increment();
		return *this;
	}

	bool has_more() {
		return m_pb.get() != NULL;
	}
};

#endif // __btree_hpp__

