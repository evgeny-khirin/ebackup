///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : eb_pers_win.cpp
/// Author  : Evgeny Khirin <>
/// Description : Erlios Erlang starter for Windows. Starts console starter
/// with detached console.
///-------------------------------------------------------------------
#define WIN32_LEAN_AND_MEAN
#define STRICT
#include <windows.h>
#include <string.h>
#include <stdlib.h>
#include <string>
#include <boost/filesystem.hpp>

//--------------------------------------------------------------------
// main function
//--------------------------------------------------------------------
int CALLBACK WinMain(HINSTANCE hInstance, HINSTANCE hPrevInstance,
										 LPSTR lpCmdLine, int nCmdShow) {
	SetErrorMode(
		SEM_FAILCRITICALERRORS | 
		SEM_NOGPFAULTERRORBOX | 
		SEM_NOALIGNMENTFAULTEXCEPT | 
		SEM_NOOPENFILEERRORBOX);
	// Get executable full path
	char buffer[4096];
	GetModuleFileNameA(GetModuleHandle(NULL), buffer, sizeof(buffer));
	boost::filesystem::path executable(buffer);
	// installation directory
	boost::filesystem::path inst_dir(executable.parent_path());
	// ini file
	boost::filesystem::path console_starter(inst_dir);
	console_starter /= "/eb_pers.exe";

	STARTUPINFOA				si;
	PROCESS_INFORMATION	pi;

	// Initialize the STARTUPINFOA structure.
	memset(&si, 0, sizeof(STARTUPINFOA));
	si.cb = sizeof(STARTUPINFOA);

	// fix command line
	strcpy_s(buffer, sizeof(buffer), "eb_pers.exe ");
	int len = strlen(lpCmdLine);
	if (len >= 2 && lpCmdLine[len - 1] == '"' && lpCmdLine[len - 2] == '\\') {
		strncat_s(buffer, sizeof(buffer), lpCmdLine, len - 1);
		strcat_s(buffer, sizeof(buffer), "\\\"");
	} else {
		strcat_s(buffer, sizeof(buffer), lpCmdLine);
	}

	// Create starter process with detached console
	CreateProcessA(
		console_starter.string().c_str(),			// pointer to name of executable module
		buffer,					// pointer to command line string
		NULL,									// pointer to process security attributes
		NULL,								  // pointer to thread security attributes
		TRUE,									// handle inheritance flag
		DETACHED_PROCESS | GetPriorityClass(GetCurrentProcess()),
													// creation flags
		NULL,									// pointer to new environment block
		NULL,									// pointer to current directory name
		&si,									// pointer to STARTUPINFOA
		&pi										// pointer to PROCESS_INFORMATION
		);

	return 0;
}
