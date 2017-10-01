///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : nlogger.cpp
/// Author  : Evgeny Khirin <>
/// Description : null logger.
///-------------------------------------------------------------------
#define __STDC_LIMIT_MACROS
#include <stdio.h>

#include "nlogger.hpp"

//--------------------------------------------------------------------
// public function
//--------------------------------------------------------------------
void nlogger::start(const std::string & name) {
	printf("nlogger: name %s\n", name.c_str());
	opt_map	opts;
	opts["name"] = name;
	logger * plogger = new nlogger;
	plogger->start(opts);
}


