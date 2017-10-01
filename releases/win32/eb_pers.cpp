///-------------------------------------------------------------------
/// Copyright (c) 2009-2010 by Evgeny Khirin.
///-------------------------------------------------------------------
///-------------------------------------------------------------------
/// File    : start_erlios.cpp
/// Author  : Evgeny Khirin <>
/// Description : Erlios Erlang starter for Windows. Allows port drivers
/// to run in services.
///-------------------------------------------------------------------
#define WIN32_LEAN_AND_MEAN
#define STRICT
#include <windows.h>
#include <shellapi.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <io.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <boost/filesystem.hpp>
#include <direct.h>


#include "eb_pers.hpp"

//--------------------------------------------------------------------
// Creates console if it is not presented
//--------------------------------------------------------------------
static void init_console() {
	if (!AllocConsole()) {
		// already have console
		return;
	}
	int hConHandle;
	long lStdHandle;
	FILE *fp;
	// redirect unbuffered STDOUT to the console
	lStdHandle = (long)GetStdHandle(STD_OUTPUT_HANDLE);
	hConHandle = _open_osfhandle(lStdHandle, _O_TEXT);
	fp = _fdopen(hConHandle, "w");
	*stdout = *fp;
	setvbuf(stdout, NULL, _IONBF, 0);
	// redirect unbuffered STDIN to the console
	lStdHandle = (long)GetStdHandle(STD_INPUT_HANDLE);
	hConHandle = _open_osfhandle(lStdHandle, _O_TEXT);
	fp = _fdopen(hConHandle, "r");
	*stdin = *fp;
	setvbuf(stdin, NULL, _IONBF, 0);
	// redirect unbuffered STDERR to the console
	lStdHandle = (long)GetStdHandle(STD_ERROR_HANDLE);
	hConHandle = _open_osfhandle(lStdHandle, _O_TEXT);
	fp = _fdopen( hConHandle, "w" );
	*stderr = *fp;
	setvbuf( stderr, NULL, _IONBF, 0 );
	// make cout, wcout, cin, wcin, wcerr, cerr, wclog and clog
	// point to console as well
	std::ios::sync_with_stdio();
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int run_service(const std::string & inst_dir, const std::string & erl_exe, const std::string & erl_cmd) {
	STARTUPINFOA				si;
	PROCESS_INFORMATION	pi;
	DWORD								dwExitCode;

	// Initialize console, if it is not presented for example in service
	init_console();

	// Initialize the STARTUPINFOA structure.
	memset(&si, 0, sizeof(STARTUPINFOA));
	si.cb = sizeof(STARTUPINFOA);
	si.lpTitle = NULL;
	si.dwFlags = STARTF_USESTDHANDLES;
	si.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
	si.hStdOutput = GetStdHandle(STD_OUTPUT_HANDLE);
	si.hStdError = GetStdHandle(STD_ERROR_HANDLE);

	// copy erl_cmd
	char * erl_cmd_copy = _strdup(erl_cmd.c_str());

	// Create the new Erlang process
	CreateProcessA(
		erl_exe.c_str(),			// pointer to name of executable module
		erl_cmd_copy,					// pointer to command line string
		NULL,									// pointer to process security attributes
		NULL,								  // pointer to thread security attributes
		TRUE,									// handle inheritance flag
		CREATE_NEW_CONSOLE | GetPriorityClass(GetCurrentProcess()),
													// creation flags
		NULL,									// pointer to new environment block
		inst_dir.c_str(),			// pointer to current directory name
		&si,									// pointer to STARTUPINFOA
		&pi										// pointer to PROCESS_INFORMATION
	);

	// Waiting for Erlang to terminate
	if (MsgWaitForMultipleObjects(1, &pi.hProcess, FALSE, INFINITE, QS_POSTMESSAGE) == WAIT_OBJECT_0 + 1) {
		if (PostThreadMessage(pi.dwThreadId, WM_USER, 0, 0)) {
			// Wait 25 seconds for erl process to die, else terminate it.
			// 25 seconds selected because erlsrv waits 30 seconds
			if(WaitForSingleObject(pi.hProcess, 25000) != WAIT_OBJECT_0){
				TerminateProcess(pi.hProcess, 0);
			}
		} else {
			TerminateProcess(pi.hProcess,0);
		}
	}
	GetExitCodeProcess(pi.hProcess, &dwExitCode);
	free(erl_cmd_copy);
	return dwExitCode;
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int start_user_app(const std::string & inst_dir, const std::string & erl_exe, std::string & erl_cmd) {
	STARTUPINFOA				si;
	PROCESS_INFORMATION	pi;
	// Initialize the STARTUPINFOA structure.
	memset(&si, 0, sizeof(STARTUPINFOA));
	si.cb = sizeof(STARTUPINFOA);
	// Add parameters to erl_cmd
	erl_cmd += " -sname eb_pers_server@localhost -noinput ";
	erl_cmd += "-sasl sasl_error_logger {file,\\\"log/eb_pers_server.log\\\"} -sasl errlog_type error ";
	erl_cmd += "-s eb_pers run_app win_user -s init stop";
	// copy erl_cmd
	char * erl_cmd_copy = _strdup(erl_cmd.c_str());
	// Create the new Erlang process
	CreateProcessA(
		erl_exe.c_str(),			// pointer to name of executable module
		erl_cmd_copy,					// pointer to command line string
		NULL,									// pointer to process security attributes
		NULL,								  // pointer to thread security attributes
		FALSE,								// handle inheritance flag
		DETACHED_PROCESS | GetPriorityClass(GetCurrentProcess()),
													// creation flags
		NULL,									// pointer to new environment block
		inst_dir.c_str(),			// pointer to current directory name
		&si,									// pointer to STARTUPINFOA
		&pi										// pointer to PROCESS_INFORMATION
	);
	free(erl_cmd_copy);
	return 0;
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static LRESULT CALLBACK TrayWindowProc(
  HWND hwnd,
  UINT uMsg,
  WPARAM wParam,
  LPARAM lParam)
{
	return DefWindowProc(hwnd, uMsg, wParam, lParam);
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int start_tray() {
//	MessageBoxA(NULL, "i'm here", "i'm here", MB_OK);
	HANDLE evnt = CreateEventA(NULL, TRUE, FALSE, "__eb_pers_tray_event__");
	if (evnt == NULL) {
//		MessageBoxA(NULL, "CreateEvent failed", "CreateEvent failed", MB_OK);
		return 0;
	}
	if (GetLastError() == ERROR_ALREADY_EXISTS) {
//		MessageBoxA(NULL, "event already exists", "event already exists", MB_OK);
		CloseHandle(evnt);
		return 0;
	}
	HICON icon = LoadIconA(GetModuleHandleA(NULL), MAKEINTRESOURCEA(EB_PERS_ICON_ID));
	WNDCLASSA wc;
	memset(&wc, 0, sizeof(wc));
	wc.lpfnWndProc = TrayWindowProc;
	wc.lpszClassName = "__eb_pers_tray_handler_class__";
	RegisterClassA(&wc);
	HWND wnd = CreateWindowA("__eb_pers_tray_handler_class__",
													 "__eb_pers_tray_handler_wnd__",
													 0, 0, 0, 0, 0, HWND_MESSAGE,
													 NULL, NULL, NULL);
	if (wnd == NULL) {
//		MessageBoxA(NULL, "CreateWindow failed", "CreateWindow failed", MB_OK);
		CloseHandle(evnt);
		return 0;
	}
	NOTIFYICONDATAA ni;
	memset(&ni, 0, sizeof(ni));
	ni.cbSize = NOTIFYICONDATAA_V1_SIZE;
	ni.hWnd = wnd;
	ni.uFlags = NIF_ICON | NIF_TIP;
	ni.hIcon = icon;
	strcpy(ni.szTip, "Protected by Erlios Backup");
	if (!Shell_NotifyIconA(NIM_ADD, &ni)) {
//		MessageBoxA(NULL, "Shell_NotifyIcon failed", "Shell_NotifyIcon failed", MB_OK);
		CloseHandle(evnt);
		return 0;
	}
	WaitForSingleObject(evnt, INFINITE);
	CloseHandle(evnt);
	Shell_NotifyIconA(NIM_DELETE, &ni);
	return 0;
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int stop_tray() {
	HANDLE evnt = CreateEventA(NULL, TRUE, FALSE, "__eb_pers_tray_event__");
	if (evnt == NULL) {
		return 0;
	}
	if (GetLastError() != ERROR_ALREADY_EXISTS) {
		CloseHandle(evnt);
		return 0;
	}
	SetEvent(evnt);
	CloseHandle(evnt);
	return 0;
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int restart_in_user_mode(const std::string & inst_dir, const std::string & erts_bin_dir, const std::string & erl_cmd) {
	// set working dir
	_chdir(erts_bin_dir.c_str());
	// disable service
	system("erlsrv disable Erlios_Backup_Personal");
	// stop service
	system("erlsrv stop Erlios_Backup_Personal");
	// ensure that service is stopped
	std::string my_cmd("erl ");
	my_cmd += erl_cmd;
	my_cmd += " -sname eb_pers_client@localhost -noinput ";
	my_cmd += "-eval \"rpc:call(eb_pers_server@localhost, erlang, halt, [])\" ";
	my_cmd += "-s init stop";
	system(my_cmd.c_str());
	return stop_tray();
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int restart_in_service_mode(const std::string & erts_bin_dir, const std::string & erl_cmd) {
	// set working dir
	_chdir(erts_bin_dir.c_str());
	// stop application
	std::string my_cmd("erl ");
	my_cmd += erl_cmd;
	my_cmd += " -sname eb_pers_client@localhost -noinput ";
	my_cmd += "-eval \"rpc:call(eb_pers_server@localhost, erlang, halt, [])\" ";
	my_cmd += "-s init stop";
	system(my_cmd.c_str());
	// enable service
	system("erlsrv enable Erlios_Backup_Personal");
	// start service
	system("erlsrv start Erlios_Backup_Personal");
	return 0;
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int stop_server(const std::string & erts_bin_dir, const std::string & erl_cmd) {
	// set working dir
	_chdir(erts_bin_dir.c_str());
	// stop application
	std::string my_cmd("erl ");
	my_cmd += erl_cmd;
	my_cmd += " -sname eb_pers_client@localhost -noinput ";
	my_cmd += "-eval \"rpc:call(eb_pers_server@localhost, erlang, halt, [])\" ";
	my_cmd += "-s init stop";
	system(my_cmd.c_str());
	return stop_tray();
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int uninstall(const std::string & erts_bin_dir, const std::string & erl_cmd) {
	// set working dir
	_chdir(erts_bin_dir.c_str());
	// stop application
	std::string my_cmd("erl ");
	my_cmd += erl_cmd;
	my_cmd += " -sname eb_pers_client@localhost -noinput -s eb_pers uninstall -s init stop";
	system(my_cmd.c_str());
	return stop_tray();
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int add_source(const std::string & inst_dir, const std::string & erl_exe, std::string & erl_cmd, std::string & path) {
	STARTUPINFOA				si;
	PROCESS_INFORMATION	pi;

	// Initialize the STARTUPINFOA structure.
	memset(&si, 0, sizeof(STARTUPINFOA));
	si.cb = sizeof(STARTUPINFOA);

	// fix path
	if (path.length() >= 1 && path[path.length() - 1] == '\\') {
		path += '\\';
	}
	// Add parameters to erl_cmd
	erl_cmd += " -sname eb_pers_client@localhost -noinput ";
	erl_cmd += "-sasl sasl_error_logger {file,\\\"log/eb_pers_client.log\\\"} -sasl errlog_type error ";
	erl_cmd += "-s eb_pers add_source \"";
	erl_cmd += path;
	erl_cmd += "\" -s init stop";

	// copy erl_cmd
	char * erl_cmd_copy = _strdup(erl_cmd.c_str());

	// Create the new Erlang process
	CreateProcessA(
		erl_exe.c_str(),			// pointer to name of executable module
		erl_cmd_copy,					// pointer to command line string
		NULL,									// pointer to process security attributes
		NULL,								  // pointer to thread security attributes
		FALSE,								// handle inheritance flag
		CREATE_NEW_CONSOLE | GetPriorityClass(GetCurrentProcess()),
													// creation flags
		NULL,									// pointer to new environment block
		inst_dir.c_str(),			// pointer to current directory name
		&si,									// pointer to STARTUPINFOA
		&pi										// pointer to PROCESS_INFORMATION
	);

	return 0;
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int add_exclude(const std::string & inst_dir, const std::string & erl_exe, std::string & erl_cmd, std::string & path) {
	STARTUPINFOA				si;
	PROCESS_INFORMATION	pi;

	// Initialize the STARTUPINFOA structure.
	memset(&si, 0, sizeof(STARTUPINFOA));
	si.cb = sizeof(STARTUPINFOA);

	// fix path
	if (path.length() >= 1 && path[path.length() - 1] == '\\') {
		path += '\\';
	}
	// Add parameters to erl_cmd
	erl_cmd += " -sname eb_pers_client@localhost -noinput ";
	erl_cmd += "-sasl sasl_error_logger {file,\\\"log/eb_pers_client.log\\\"} -sasl errlog_type error ";
	erl_cmd += "-s eb_pers add_exclude \"";
	erl_cmd += path;
	erl_cmd += "\" -s init stop";

	// copy erl_cmd
	char * erl_cmd_copy = _strdup(erl_cmd.c_str());

	// Create the new Erlang process
	CreateProcessA(
		erl_exe.c_str(),			// pointer to name of executable module
		erl_cmd_copy,					// pointer to command line string
		NULL,									// pointer to process security attributes
		NULL,								  // pointer to thread security attributes
		FALSE,								// handle inheritance flag
		CREATE_NEW_CONSOLE | GetPriorityClass(GetCurrentProcess()),
													// creation flags
		NULL,									// pointer to new environment block
		inst_dir.c_str(),			// pointer to current directory name
		&si,									// pointer to STARTUPINFOA
		&pi										// pointer to PROCESS_INFORMATION
	);

	return 0;
}

//--------------------------------------------------------------------
// Internal function
//--------------------------------------------------------------------
static int error_box(const std::string & msg) {
	MessageBoxA(NULL, msg.c_str(), "Error", MB_OK | MB_ICONERROR);
	return 0;
}

//--------------------------------------------------------------------
// Requirements.
// Program has following filesystem structure:
//	$INSTAL_DIR  - start_erlios.exe and start_erlios.ini are here
//						|
//						erts-$ERTS_VSN
//						|				|
//						|				bin - erl.exe is here
//						|
//						releases
//										|
//										$REL_VSN - start.boot is here
//
// start_erlios.ini:
//		[erlios]
//		erts=$ERTS_VSN
//		rel=$REL_VSN
//
// All command line arguments are passed to erlang as is.
//--------------------------------------------------------------------
int main(int argc, char *argv[])
{
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
	boost::filesystem::path ini_file(inst_dir);
	ini_file /= "/eb_pers.ini";
	// Get $ERTS_VSN
	GetPrivateProfileStringA("erlios", "erts", NULL, buffer, sizeof(buffer), ini_file.string().c_str());
	std::string erts_vsn(buffer);
	// Get $REL_VSN
	GetPrivateProfileStringA("erlios", "rel", NULL, buffer, sizeof(buffer), ini_file.string().c_str());
	std::string rel_vsn(buffer);
	// erlang executable path and working directory
	std::string erts_bin_dir(inst_dir.string());
	erts_bin_dir += "/erts-";
	erts_bin_dir += erts_vsn;
	erts_bin_dir += "/bin";
	// build erlang command line and my_params
	std::vector<std::string> my_params;
	std::string erl_cmd("erl.exe --boot \"");
	erl_cmd += inst_dir.string();
	erl_cmd += "/releases/";
	erl_cmd += rel_vsn;
	erl_cmd += "/start\"";
	bool erl_params = true;
	for (int i = 1; i < argc; i++) {
		if (erl_params && strcmp(argv[i], "++") == 0) {
			erl_params = false;
		} else if (erl_params) {
			erl_cmd += ' ';
			erl_cmd += argv[i];
		} else {
			my_params.push_back(argv[i]);
		}
	}
	erl_cmd += " -setcookie eb_pers_2_5_2";
	if (my_params.size() == 0) {
		return 0;
	}
	if (my_params[0] == "-service") {
		erts_bin_dir += "/erl.exe";
		return run_service(inst_dir.string(), erts_bin_dir, erl_cmd);
	}
	if (my_params[0] == "-user_app") {
		erts_bin_dir += "/erl.exe";
		return start_user_app(inst_dir.string(), erts_bin_dir, erl_cmd);
	}
	if (my_params[0] == "-restart_in_user_mode") {
		return restart_in_user_mode(inst_dir.string(), erts_bin_dir, erl_cmd);
	}
	if (my_params[0] == "-restart_in_service_mode") {
		return restart_in_service_mode(erts_bin_dir, erl_cmd);
	}
	if (my_params[0] == "-stop_server") {
		return stop_server(erts_bin_dir, erl_cmd);
	}
	if (my_params[0] == "-uninstall") {
		return uninstall(erts_bin_dir, erl_cmd);
	}
	if (my_params[0] == "-start_tray") {
		return start_tray();
	}
	if (my_params[0] == "-stop_tray") {
		return stop_tray();
	}
	if (my_params[0] == "-add_source") {
		erts_bin_dir += "/erl.exe";
		return add_source(inst_dir.string(), erts_bin_dir, erl_cmd, my_params[1]);
	}
	if (my_params[0] == "-add_exclude") {
		erts_bin_dir += "/erl.exe";
		return add_exclude(inst_dir.string(), erts_bin_dir, erl_cmd, my_params[1]);
	}
	if (my_params[0] == "-error_box") {
		return error_box(my_params[1]);
	}
//	MessageBoxA(NULL, my_params[0].c_str(), "Unknow command", MB_OK);
	return 0;
}
