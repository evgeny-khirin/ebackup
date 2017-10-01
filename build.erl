%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : build.erl
%%% Author  : Evgeny Khirin <>
%%% Description :
%%%
%%% Created : 22 Feb 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(build).

-include("eklib/include/debug.hrl").

%% API
-export([compile/0,
				 compile/1,
				 clean/0,
				 test/0,
				 test/1,
				 release/3,
				 release/0,
				 gen_keys/0
				]).

%% Keys structures
-include_lib("public_key/include/public_key.hrl").

%%--------------------------------------------------------------------
%% Applications list to build
%%--------------------------------------------------------------------
-define(ALL,
				[eklib,
				 kdb,
				 erlios,
				 releases,
				 eb_pers
				]).

%%--------------------------------------------------------------------
%% Compiler options
%%--------------------------------------------------------------------
-define(PRODUCTION_FLAGS, [verbose, report_errors, report_warnings]).
-define(SANDBOX_FLAGS, [{d, 'SANDBOX'} | ?PRODUCTION_FLAGS]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: compile([Mode]) -> ok | {error, Reason}
%% Description: Builds all applications in desired mode.
%%    Mode = debug | production. Default is debug
%%--------------------------------------------------------------------
compile() ->
		compile(sandbox).

compile(sandbox) ->
		random:seed(now()),
		compile(?SANDBOX_FLAGS);
compile(production) ->
		random:seed(now()),
		compile(?PRODUCTION_FLAGS);
compile(Flags) when is_list(Flags) ->
		apps(?ALL, Flags),
		case os:type() of
				{unix, linux} ->
						build_kdb();
				_ ->
						ok
		end.

%%--------------------------------------------------------------------
%% Function: clean() -> ok | {error, Reason}
%% Description: Deletes all beam files.
%%--------------------------------------------------------------------
clean() ->
		clean(?ALL).

clean([])->
		ok;
clean([H|T])->
		%% Get application's directories
		{_AppSrc, _AppInc, AppEbin} = app_dirs(H),
		case file:list_dir(AppEbin) of
				 {error,enoent} ->
						ok;
				{ok, DirList} ->
						%% Filter beam files
						Beams = lists:filter(fun(X) -> ".beam" =:= filename:extension(X) end,
																 DirList),
						%% Delete files
						lists:foldl(fun(X, Acc) ->
																File = filename:join(AppEbin, X),
																ok = file:delete(File),
																Acc
												end,
												ok,
												Beams),
						ok
		end,
		clean(T).

%%--------------------------------------------------------------------
%% Function: test() -> ok | {error, Reason}
%% Description: Runs unit tests once
%%--------------------------------------------------------------------
test() ->
		test(1).

%%--------------------------------------------------------------------
%% Function: test(Count) -> ok | {error, Reason}
%% Description: Runs erlang unit tests specified number of cycles. C tests
%% are run with run_tests.sh script.
%%--------------------------------------------------------------------
test(0) ->
		ok;
test(Count) ->
		?TRACE("++++++++++++++++++++++++++++++++++++++++++++++++++ left ~p", [Count]),
		?TRACE("date: ~p, time ~p", [date(), time()]),

		%% KDB tests
		?TRACE("running kdb_test, time ~p", [time()]),
		ok = kdb_test:run(),

		test(Count - 1).

%%--------------------------------------------------------------------
%% Function: release(System, Platform, Variant) -> ok | {error, Reason}
%% Description: Creates release of system.
%%    System = name of system without ".rel" or ".relup".
%%    Platform = linux | win32 | all
%%    Variant = sandbox | production
%%--------------------------------------------------------------------
release(System, Platform, Variant) ->
		ok = clean(),
		ok = compile(Variant),
		ok = target_system:create(System, Platform, Variant).

%%--------------------------------------------------------------------
%% Function: release() -> ok | {error, Reason}
%% Description: Creates release of all systems.
%%--------------------------------------------------------------------
release() ->
		%% Production
		?TRACE("releasing production code ...", []),
		ok = clean(),
		ok = compile(production),
		ok = target_system:create(eb_pers, win32, production),
		ok = target_system:create(eb_pers, linux, production),
		%% Sandbox
		?TRACE("releasing sandbox code ...", []),
		ok = clean(),
		ok = compile(sandbox),
		ok = target_system:create(eb_pers, win32, sandbox),
		ok = target_system:create(eb_pers, linux, sandbox).

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: apps(List, Options) -> ok | {error, Reason}
%% Description: Builds listed applications
%%--------------------------------------------------------------------
apps([], _Options) ->
		ok;
apps([H|T], Options) ->
		%% Get application's directories
		{AppSrc, _AppInc, AppEbin} = app_dirs(H),
		case file:list_dir(AppSrc) of
				 {error,enoent} ->
						ok;
				{ok, DirList} ->
						%% Filter erlang sources
						Sources = lists:filter(fun(X) -> ".erl" =:= filename:extension(X) end,
																	 DirList),
						%% Select output directory
						NewOptions = [{outdir, AppEbin} | Options],
						%% Make sure that application is in path
						code:add_path(AppEbin),
						%% Compile sources
						lists:foldl(fun(X, _Acc) ->
																ok = compile_if_changed(X, AppSrc, AppEbin, NewOptions)
												end,
												ok,
												Sources),
						ok
		end,
		apps(T, Options).

%%--------------------------------------------------------------------
%% 1. Lookups .d file in ebin directory.
%% 2. If .d file not found, than it is created. .d file lists dependencies.
%% 3. Checks that timestamp of target file is larger than timestamp of any of
%%    its dependenices. If timestamp of target is less than target must be recompiled.
%% 4. Recompilation of target produces new dependency list.
%%--------------------------------------------------------------------
compile_if_changed(SrcName, SrcDir, EbinDir, Options) ->
		BaseName = filename:basename(filename:rootname(SrcName)),
		SrcFile = filename:join(SrcDir, SrcName),
		DepFile = filename:join([EbinDir, BaseName ++ ".d"]),
		BeamFile = filename:join([EbinDir, BaseName ++ ".beam"]),
		MustCompile =
				case fs_object_exist(DepFile) andalso fs_object_exist(BeamFile) of
						false ->
								true;
						true ->
								check_erl_dependencies(BeamFile, DepFile)
				end,
		if
				MustCompile ->
						ok = compile_file(SrcFile, DepFile, Options);
				true ->
						ok
		end.

%%--------------------------------------------------------------------
%% Checks file dependencies. Returns true, if file must be recompiled.
%%--------------------------------------------------------------------
check_erl_dependencies(BeamFile, DepFile) ->
		{ok, [Depends]} = file:consult(DepFile),
		BeamLastModified = filelib:last_modified(BeamFile),
		DepLastModified = filelib:last_modified(DepFile),
		if
				DepLastModified > BeamLastModified ->
						true;
				true ->
						check_dependencies(BeamFile, Depends)
		end.

check_dependencies(Target, Depends) ->
		TargetLastModified = filelib:last_modified(Target),
		not lists:all(fun(File) ->
													FileLastModified = filelib:last_modified(File),
													case filelib:last_modified(File) of
															0 ->
																	false;
															FileLastModified ->
																	FileLastModified < TargetLastModified
													end
									end,
									Depends).

%%--------------------------------------------------------------------
%% compiles file and builds its dependencies file
%%--------------------------------------------------------------------
compile_file(SrcFile, DepFile, Options) ->
		Defines =
				lists:map(fun({d, Macro}) ->
													{Macro, true};
										 ({d, Macro, Value}) ->
													{Macro, Value}
									end,
									lists:filter(fun({d, _}) ->
																			 true;
																	({d, _, _}) ->
																			 true;
																	(_) ->
																			 false
															 end,
															 Options)),
		{ok, Form} = epp:parse_file(SrcFile, code:get_path(), Defines),
		Depends =
				sets:to_list(
					sets:from_list(
						lists:map(fun({attribute, _Line1, file, {File, _Line2}}) ->
															File
											end,
											lists:filter(fun({attribute, _Line1, file, {_File, _Line2}}) ->
																					 true;
																			(_) ->
																					 false
																	 end,
																	 Form)))),
		ok = file:write_file(DepFile, io_lib:format("~p.",[Depends])),
		case c:c(SrcFile, Options) of
				{ok, _Module} ->
						ok;
				Error ->
						io:format("~s: Failed: ~p\n", [SrcFile, Error])
		end,
		ok.

%%--------------------------------------------------------------------
%% Function: app_dirs(App) -> {AppSrc, AppInc, AppEbin}.
%% Types: App -> term().
%% Description: Returns OTP directory structure for application
%%--------------------------------------------------------------------
app_dirs(App) ->
		%% Build application's directory name
		{_Module, _Binary, Filename} = code:get_object_code(?MODULE),
		AppBase = filename:join(filename:dirname(Filename),
														atom_to_list(App)),
		AppInc = filename:join(AppBase, "include"),
		AppSrc = filename:join(AppBase, "src"),
		AppEbin = filename:join(AppBase, "ebin"),
		{AppSrc, AppInc, AppEbin}.

%%---------------------------------------------------------------------------
%% fs_object_exist - checks if there is object with such name in FS
%%---------------------------------------------------------------------------
fs_object_exist(Obj) ->
		case file:read_link_info(Obj) of
				{ok, _} ->
						true;
				{error, enoent} ->
						false;
				_ ->
						case file:read_file_info(Obj) of
								{error, enoent} ->
										false;
								_ ->
										true
						end
		end.

%%---------------------------------------------------------------------------
%% mkdir_ensure - builds whole directory hierarchy
%%---------------------------------------------------------------------------
mkdir_ensure(Dir) ->
		filelib:ensure_dir(Dir),
		file:make_dir(Dir),
		ok.

%%--------------------------------------------------------------------
%% Function: temp_filename(BaseDir) -> AbsName
%% Description: Generates temporary filename under BaseDir.
%%--------------------------------------------------------------------
temp_filename(BaseDir) ->
		N = random:uniform(1000000),
		BaseName = lists:concat(["tmp_", N, '.']),
		Prefix = filename:absname(filename:join(BaseDir, BaseName)),
		temp_filename(Prefix, 0).

temp_filename(Prefix, Suffix) ->
		Name = lists:concat([Prefix, Suffix]),
		case fs_object_exist(Name) of
				true ->
						temp_filename(Prefix, Suffix + 1);
				false ->
						Name
    end.

%%--------------------------------------------------------------------
%% Function: gen_dss_key_pair(OutFile, KeyPrefix, Bits) -> ok | {error, Error}
%% Description: Generates include files with RSA keys definitions.
%%--------------------------------------------------------------------
gen_dss_key_pair(OutFile, KeyPrefix, Bits) ->
		OutDir = filename:dirname(OutFile),
		PemFile = temp_filename(OutDir),
		os:cmd(io_lib:format("openssl dsaparam -out ~p -genkey ~p", [PemFile, Bits])),
		{ok, [KeyInfo]} = public_key:pem_to_der(PemFile),
		ok = file:delete(PemFile),
		{ok, CompoundKey} = public_key:decode_private_key(KeyInfo),
		P = crypto:mpint(CompoundKey#'DSAPrivateKey'.p),
		Q = crypto:mpint(CompoundKey#'DSAPrivateKey'.q),
		G = crypto:mpint(CompoundKey#'DSAPrivateKey'.g),
		Y = crypto:mpint(CompoundKey#'DSAPrivateKey'.y),
		X = crypto:mpint(CompoundKey#'DSAPrivateKey'.x),
		{ok, Fd} = file:open(OutFile, [write]),
		io:fwrite(Fd, "-ifdef(LM_SERVER).\n\n", []),
		io:fwrite(Fd, "-define(~s_DSS_PRIVATE_KEY,\n\t[~w,\n\t ~w,\n\t ~w,\n\t ~w]).\n\n",
							[KeyPrefix, P, Q, G, X]),
		io:fwrite(Fd, "-endif.\n\n", []),
		io:fwrite(Fd, "-define(~s_DSS_PUBLIC_KEY,\n\t[~w,\n\t ~w,\n\t ~w,\n\t ~w]).\n\n",
							[KeyPrefix, P, Q, G, Y]),
		ok = file:close(Fd).

%%--------------------------------------------------------------------
%% Function: gen_aes_128_key(OutFile, KeyPrefix) -> ok | {error, Error}
%% Description: Generates include files with AES-128 key definitions
%%--------------------------------------------------------------------
gen_aes_128_key(OutFile, KeyPrefix) ->
		ensure_app(crypto),
		Key = crypto:rand_bytes(16),
		{ok, Fd} = file:open(OutFile, [write]),
		io:fwrite(Fd, "-ifdef(LM_SERVER).\n\n", []),
		io:fwrite(Fd, "-define(~s_AES_128_KEY, ~w).\n\n", [KeyPrefix, Key]),
		io:fwrite(Fd, "-endif.\n\n", []),
		ok = file:close(Fd).

%%--------------------------------------------------------------------
%% Function: gen_keys() -> ok | {error, Error}
%% Description: Generates keys.
%%--------------------------------------------------------------------
gen_keys() ->
		random:seed(now()),
		{_Module, _Binary, Filename} = code:get_object_code(?MODULE),
		BaseDir = filename:dirname(Filename),
		%% Generate common DSS key pair
		ErliosDss = filename:join([BaseDir, "erlios", "include", "erlios_dss.hrl"]),
		gen_dss_key_pair(ErliosDss, "ERLIOS", 2048),
		%% Generate server's AES 128 key
		ErliosAes128 = filename:join([BaseDir, "erlios", "include", "erlios_aes.hrl"]),
		gen_aes_128_key(ErliosAes128, "ERLIOS").

%%--------------------------------------------------------------------
%% Make sure that application is started
%%--------------------------------------------------------------------
ensure_app(App) ->
		case application:start(App) of
				ok ->
						ok;
				{error,{already_started,App}} ->
						ok
		end.

%%--------------------------------------------------------------------
%% Executes OS command. If command returns 0, than its output is discarded
%% and ok returned. If exit status is not equal to 0, than command output
%% is dumped to screen and error is returned.
%%--------------------------------------------------------------------
exec(Cmd, Filter) ->
		Opts = [stream, exit_status, use_stdio, stderr_to_stdout, binary],
		Port = open_port({spawn, lists:flatten(Cmd)}, Opts),
		io:format("~s~n", [Cmd]),
		exec_loop(Port, <<>>, Filter).

exec_loop(Port, Res, Filter) ->
		receive
				{Port, {exit_status, 0}} ->
						ok;
				{Port, {exit_status, Err}} ->
						io:format("~s", [filter_bin_str(Res, Filter)]),
						{error, {exit_status, Err}};
				{Port, {data, Data}} ->
						exec_loop(Port, <<Res/binary, Data/binary>>, Filter)
%% 				_ ->
%% 						exec_loop(Port, Res, Filter)
		end.

%%--------------------------------------------------------------------
%% Function: filter_bin_str(Bin, DoFilter) -> NewBin
%% Description: Filters binary from unprintable characters.
%%    Mode = debug | release. Default is debug
%%--------------------------------------------------------------------
filter_bin_str(Str, true) ->
		filter_bin_str(Str, <<>>);
filter_bin_str(Str, false) ->
		Str;
filter_bin_str(<<>>, Res) ->
		Res;
filter_bin_str(<<H, T/binary>>, Res) when H >= 32, H < 128 ->
		filter_bin_str(T, <<Res/binary, H>>);
filter_bin_str(<<10, T/binary>>, Res) ->
		filter_bin_str(T, <<Res/binary, 10>>);
filter_bin_str(<<_, T/binary>>, Res) ->
		filter_bin_str(T, Res).

%%--------------------------------------------------------------------
%% Compiles C++ source file, returns true if file was indeed compiled
%%--------------------------------------------------------------------
compile_c_linux(File, debug) ->
		BaseTarget = filename:join([filename:dirname(File), "obj.linux", filename:basename(File)]),
		Target = BaseTarget ++ ".debug.o",
		DepFile = BaseTarget ++ ".debug.d",
		case is_c_file_changed(Target, DepFile) of
				true ->
						Opts = "-O0 -g -ggdb -Wall -Werror -fPIC -MMD " ++
								"-I/usr/local/lib/erlang/usr/include " ++
								"-I/usr/local/lib/erlang/lib/erl_interface-3.5.9/include",
						Cmd = io_lib:format("g++-4.2 -c \"~s\" -o \"~s\" ~s", [File, Target, Opts]),
						ok = exec(Cmd, true),
						true;
				false ->
						false
		end;
compile_c_linux(File, release) ->
		BaseTarget = filename:join([filename:dirname(File), "obj.linux", filename:basename(File)]),
		Target = BaseTarget ++ ".release.o",
		DepFile = BaseTarget ++ ".release.d",
		case is_c_file_changed(Target, DepFile) of
				true ->
						Opts = "-O3 -Wall -Werror -fPIC -MMD -DNDEBUG " ++
								"-I/usr/local/lib/erlang/usr/include " ++
								"-I/usr/local/lib/erlang/lib/erl_interface-3.5.9/include",
						Cmd = io_lib:format("g++-4.2 -c \"~s\" -o \"~s\" ~s", [File, Target, Opts]),
						ok = exec(Cmd, true),
						true;
				false ->
						false
		end.

%%--------------------------------------------------------------------
%% Returns true if C++ file or some file, on which target file depends,
%% are changed.
%%--------------------------------------------------------------------
is_c_file_changed(Target, DepFile) ->
		case fs_object_exist(Target) andalso fs_object_exist(DepFile) of
				false ->
						true;
				true ->
						check_c_dependencies(Target, DepFile)
		end.

check_c_dependencies(Target, DepFile) ->
		{ok, DepData} = file:read_file(DepFile),
		Depends = parse_make_deps(Target, DepData),
		TargetLastModified = filelib:last_modified(Target),
		DepLastModified = filelib:last_modified(DepFile),
		if
				DepLastModified > TargetLastModified ->
						true;
				true ->
						check_dependencies(Target, Depends)
		end.

%%--------------------------------------------------------------------
%% Parses dependendcies in style of makefile
%%--------------------------------------------------------------------
parse_make_deps(_, <<>>) ->
		[];
parse_make_deps(Target, DepData) when is_list(Target) ->
		parse_make_deps(list_to_binary(Target), DepData);
parse_make_deps(Target, DepData00) ->
		TargetSize = size(Target),
		<<Target:TargetSize/binary, $:, DepData10/binary>> = DepData00,
		parse_make_deps(DepData10, [], []).

parse_make_deps(<<H, T/binary>>, Curr, Depends00) when H =< 32; H =:= $\\ ->
		Depends10 =
				case Curr of
						[] ->
								Depends00;
						_ ->
								[lists:reverse(Curr) | Depends00]
				end,
		parse_make_deps(T, [], Depends10);
parse_make_deps(<<H, T/binary>>, Curr, Depends) ->
		parse_make_deps(T, [H | Curr], Depends);
parse_make_deps(<<>>, Curr, Depends) ->
		case Curr of
				[] ->
						lists:reverse(Depends);
				_ ->
						[lists:reverse(Curr) | Depends]
		end.

%%--------------------------------------------------------------------
%% Builds kdb driver and unit tests
%%--------------------------------------------------------------------
build_kdb() ->
		build_kdb(debug),
		build_kdb(release).

build_kdb(Mode) ->
		KdbDir = filename:join(kdb, c_src),
		mkdir_ensure(filename:join(KdbDir, "obj.linux")),
		mkdir_ensure(filename:join(KdbDir, "tests.linux")),
		{ok, DirList} = file:list_dir(KdbDir),
		%% Build sources list
		AllSources =
				lists:map(fun(X) ->
													filename:join(KdbDir, X)
									end,
									lists:filter(fun(X) -> ".cpp" =:= filename:extension(X) end,
															 DirList)),
		%% Tests sources
		TestSources =
				lists:map(fun(X) ->
													filename:join(KdbDir, X)
									end,
									["ld_bd_test.cpp",
									 "ld_pp_test.cpp",
									 "stm_test_1.cpp",
									 "stm_test_2.cpp",
									 "stm_test_perf.cpp",
									 "btree_test_1.cpp",
									 "btree_test_perf_1.cpp",
									 "btree_test_perf_2.cpp",
									 "bdb_test_perf.cpp",
									 "kdb_drv.cpp"]),
		%% other sources
		LibSources = AllSources -- TestSources,
		%% buld KDB library for tests
		build_kdb_lib_linux(LibSources, Mode),
		%% build tests
		lists:foreach(fun(X) ->
													build_c_test_linux(X, Mode)
									end,
									TestSources),
		%% copy drivers to private directory
		case Mode of
				release ->
						CopyCmdLinux = io_lib:format("cp -puf \"~s\" \"~s\"",
																		[filename:join([kdb, c_src, "tests.linux", "kdb_drv.release"]),
																		 filename:join([kdb, priv, "kdb_drv"])]),
						exec(CopyCmdLinux, false),
						CopyCmdWin32 = io_lib:format("cp -puf \"~s\" \"~s\"",
																				 [filename:join([kdb, win32, "kdb_drv.exe"]),
																					filename:join(kdb, priv)]),
						exec(CopyCmdWin32, false);
				_ ->
						ok
		end.

%%--------------------------------------------------------------------
%% Builds KDB library
%%--------------------------------------------------------------------
build_kdb_lib_linux(Sources, Mode) ->
		KdbDir = filename:join(kdb, c_src),
		Target = filename:join([KdbDir, "obj.linux", "libkdb."]) ++ atom_to_list(Mode) ++ ".a",
		%% Compile sources
		lists:foreach(fun(X) ->
													compile_c_linux(X, Mode)
									end,
									Sources),
		%% Build objects list
		Objects00 =
				lists:map(fun(X) ->
													[X, ".", atom_to_list(Mode), ".o"]
									end,
									lists:map(fun(X) ->
																		filename:join([KdbDir, "obj.linux", filename:basename(X)])
														end,
														Sources)),
		case check_dependencies(Target, Objects00) of
				true ->
						%% Sleep before linking so file system will not resolve time difference
						timer:sleep(1000),
						Objects10 =
								lists:map(fun(X) ->
																	["\"", X, "\" "]
													end,
													Objects00),
						Cmd = io_lib:format("ar -crs \"~s\" ~s", [Target, Objects10]),
						ok = exec(Cmd, true);
				false ->
						ok
		end.

%%--------------------------------------------------------------------
%% Builds C unit tests
%%--------------------------------------------------------------------
build_c_test_linux(TestSrc, Mode) ->
		KdbDir = filename:join(kdb, c_src),
		Target = filename:join([KdbDir, "tests.linux", filename:basename(TestSrc, ".cpp")]) ++ "." ++ atom_to_list(Mode),
		KdbLib = filename:join([KdbDir, "obj.linux", "libkdb."]) ++ atom_to_list(Mode) ++ ".a",
		%% Compile sources
		compile_c_linux(TestSrc, Mode),
		%% Build objects list
		Objects00 =
				lists:map(fun(X) ->
													[X, ".", atom_to_list(Mode), ".o"]
									end,
									lists:map(fun(X) ->
																		filename:join([KdbDir, "obj.linux", filename:basename(X)])
														end,
														[TestSrc])),
		ModeOpts =
				case Mode of
						debug ->
								"-g -ggdb";
						release ->
								"-s"
				end,
		case check_dependencies(Target, [KdbLib | Objects00]) of
				true ->
						%% Sleep before linking so file system will not resolve time difference
						timer:sleep(1000),
						Objects10 =
								lists:map(fun(X) ->
																	["\"", X, "\" "]
													end,
													[Objects00]),
						DbLib =
								case string:str(Target, "bdb_test_perf") of
										0 ->
												[];
										_ ->
												"-ldb"
								end,
						Libs =
								"/usr/local/lib/erlang/lib/erl_interface-3.5.9/lib/liberl_interface.a "++
								"/usr/local/lib/erlang/lib/erl_interface-3.5.9/lib/libei.a " ++
								DbLib ++ " -lboost_thread -lz -lpthread",
						Cmd = io_lib:format("g++-4.2 -static -static-libgcc -Xlinker -zmuldefs -o \"~s\" ~s ~s \"~s\" ~s", [Target, ModeOpts, Objects10, KdbLib, Libs]),
						ok = exec(Cmd, true);
				false ->
						ok
		end.

