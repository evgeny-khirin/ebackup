///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : ld_bd.hpp
/// Author  : Evgeny Khirin <>
/// Description : Log device based on block device. Most tricks in this
/// module deal with devices, which flushes and writes blocks in disorder.
///-------------------------------------------------------------------
#ifndef __ld_bd_hpp__
#define __ld_bd_hpp__

#include "ld.hpp"

//--------------------------------------------------------------------
// Terminal log device based on block device.
//--------------------------------------------------------------------
class ld_bd: public log_device {
private:
	enum header_version {
		version_1 = 1,
		current_version = version_1
	};

	struct header_t {
		char  		m_signature[4];				// signature 'LDBD'.
		uint32_t	m_version;						// header version
		uint64_t	m_head_lsn;						// head lsn.
		uint64_t	m_tail_lsn;						// tail lsn.
		uint64_t	m_capacity;						// device capacity.
		uint64_t	m_head_phys;					// head's fhysical block number
		uint64_t	m_head_logical;				// head's logical block number
		uint64_t	m_generation;					// header generation. Generation
																		// is constantly increasing on
																		// each header update.
		uint64_t	m_session;						// Used to distinguish old blocks
																		// after set_tail is called.
																		// Session is increased on each
																		// set_tail call.
		uint64_t	m_first_session_block;	// First block number, for which
																		// new session is effective.
																		// Blocks with higher or equal
																		// numbers and older sessions are
																		// considered as invalid.
		uint64_t 	m_checkpoint_lsn;			// transaction manager checkpoint LSN
		uint64_t	m_sys_gen;						// System generation increased on
																		// each reformat of raw disk. Used
																		// as trailler for CRC device in order
																		// to distinguish (invalidate) old
																		// written data.
		char			m_app_name[16];				// application name

	private:
		header_t();

	public:
		header_t(bool do_init) {
			if (do_init) {
				m_signature[0] = 'L';
				m_signature[1] = 'D';
				m_signature[2] = 'B';
				m_signature[3] = 'D';
				m_version = current_version;
				m_capacity = 0;
				m_head_phys = 2;
				m_head_logical = 0;
				m_generation = 1;
				m_session = 1;
				m_first_session_block = 0;
				m_head_lsn = m_tail_lsn = 0;
				m_checkpoint_lsn = 0;
				m_sys_gen = 0;
				memset(m_app_name, 0, sizeof(m_app_name));
			}
		};

		bool is_ok() const {
			if (this == NULL) {
				return false;
			}
			if (m_signature[0] == 'L' && m_signature[1] == 'D' &&
					m_signature[2] == 'B' && m_signature[3] == 'D' &&
					m_version > 0 && m_version <= current_version) {
				return true;
			}
			return false;
		}
	};

private:
	block_device *	m_pdevice;			// pointer to underlaying device.
	header_t				m_header;				// header
	uint32_t				m_block_size;		// effective block size. Each
																	// block reserves 16 bytes for
																	// logical block number and
																	// session number.
	int 						m_syncs;				// number of sync calls - tail is
																	// saved every 100 calls.
	uint64_t				m_max_block_n;	// max logical block number
																	// written on device. Used in increase
																	// capacity.

	//--------------------------------------------------------------------
	// Function: read_header(block_device * pdev, header_t & header).
	// Description: Reads header from block device.
	//--------------------------------------------------------------------
	static void read_header(block_device * pdev, header_t & header);

	//--------------------------------------------------------------------
	// Function: write_header(block_device * pdev, header_t & header).
	// Description: Writes header to block device.
	//--------------------------------------------------------------------
	static void write_header(block_device * pdev, header_t & header);

	//--------------------------------------------------------------------
	// Function: uint64_t translate_block_n(uint64_t logical_n).
	// Description: Translates logical block number to physical. Ensures that
	// translated number is within log boundaries.
	//--------------------------------------------------------------------
	uint64_t translate_block_n(uint64_t logical_n);

	//--------------------------------------------------------------------
	// Function: check_capacity()
	// Description: This function checks if capacity of underlaying device
	// is increased and ld_bd must upgrade itself to use new capacity. ld_bd can
	// switch to new capacity only when log data reside continuously.
	//--------------------------------------------------------------------
	void check_capacity();

public:
	//--------------------------------------------------------------------
	// Function: ld_bd().
	// Description: Constructor.
	//--------------------------------------------------------------------
	ld_bd() : m_header(true)
	{m_pdevice = NULL; m_block_size = 0; m_syncs = 0; m_max_block_n = 0;}

	//--------------------------------------------------------------------
	// Function: start(const opt_map & options).
	// Description: Starts the device. Options are device specific.
	// Parameters:
	//    Options = [{Key, Value}]
	//    Key = Value = string
	// Supported options:
	//   {name, Name} - registered object name. Named objects significantly
	//      reduce complexity of program.
	//   {block_device, Name} - name of underlaying device.
	//--------------------------------------------------------------------
	virtual void start(const opt_map & options);

	//--------------------------------------------------------------------
	// Function: start(...).
	// Description: Start function with unfolded parameters. Used in unit
	// tests.
	//--------------------------------------------------------------------
	static void start(const std::string & name, const std::string & block_device);

	// see base class.
	virtual void stop();

	// see base class.
	virtual void stats(no_stats_list & list);

	// see base class.
	virtual void del_underlaying();

	// see base class.
	virtual uint64_t get_head();

	// see base class.
	virtual uint64_t get_checkpoint();

	// see base class.
	virtual void set_head(uint64_t lsn, uint64_t head_block, uint64_t checkpoint_lsn);

	// see base class.
	virtual uint64_t get_tail();

	// see base class.
	virtual void set_tail(uint64_t lsn, uint64_t tail_block);

	// see base class.
	virtual block_ptr read(uint64_t n);

	// see base class.
	virtual void write(uint64_t n, block_ptr & pb);

	// see base class.
	virtual void sync(uint64_t tail_lsn);

	// see base class.
	virtual uint32_t block_size();

	// see base class.
	virtual block_ptr alloc();

	//--------------------------------------------------------------------
	// Function: format(block_device * dev, const std::string & app_name).
	// Description: Formats block device. Application name must not exceed
	// 16 characters.
	//--------------------------------------------------------------------
	static void format(const std::string & dev_name, const std::string & app_name);
	static void format(block_device * dev, const std::string & app_name);

	//--------------------------------------------------------------------
	// Function: is_formatted(block_device * dev, const std::string & app_name) -> bool
	// Description: Checks that block device is properly formated. Application
	// name must not exceed 16 characters.
	//--------------------------------------------------------------------
	static bool is_formatted(const std::string & dev_name, const std::string & app_name);
	static bool is_formatted(block_device * dev, const std::string & app_name);

	//--------------------------------------------------------------------
	// Function: system_generation() -> uint64_t
	// Description: Returns system generation for raw devices.
	//--------------------------------------------------------------------
	uint64_t system_generation() {
		return m_header.m_sys_gen;
	}
};

#endif // __ld_bd_hpp__

