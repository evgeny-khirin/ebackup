%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : target_system.erl
%%% Author  : Evgeny Khirin <>
%%% Description :
%%%
%%% Created : 21 Dec 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(target_system).

-include_lib("kernel/include/file.hrl").
-include("eklib/include/debug.hrl").

-export([create/3, install/3]).

-define(BUFSIZE, 8192).

%%--------------------------------------------------------------------
%% Function: create(System, Platform, Variant) -> ok | {error, Reason}
%% Description: Creates release of system.
%%    System - atom. Name of system without ".rel" or ".relup".
%%    Platform - linux | win32 | all
%%--------------------------------------------------------------------
create(System, win32, Variant) ->
		ok = create_win32(System, Variant);
create(System, linux, Variant) ->
		ok = create_linux(System, Variant);
create(System, all, Variant) ->
		ok = create_win32(System, Variant),
		ok = create_linux(System, Variant).

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: get_src_dir(Module) -> PrivDir
%% Description: Returns path of module's src directory.
%%--------------------------------------------------------------------
get_module_src_dir(Module)->
		{_Module, _Binary, Filename} = code:get_object_code(Module),
		filename:absname_join(filename:dirname(Filename), filename:join("..", "src")).

%%--------------------------------------------------------------------
%% creates system release for windows 32
%%--------------------------------------------------------------------
-define(ERL_WIN32_ROOT, "/home/evgeny/work/bzr_reps/erlang.r12b-5/otp_win32_R12B-5").
create_win32(System, Variant) ->
		SrcDir = get_module_src_dir(?MODULE),
		RelFile = filename:join(SrcDir, System) ++ ".rel",
    {ok, [RelSpec]} = file:consult(RelFile),
    {release, {_RelName, RelVsn}, {erts, ErtsVsn}, _AppVsns} = RelSpec,
		TmpDir = eklib:temp_filename("/tmp"),
		RootDir = ?ERL_WIN32_ROOT,
		file:make_dir(TmpDir),

		%% copy files rel file into tmp dir
		{ok, _} = file:copy(RelFile, filename:join(TmpDir, System) ++ ".rel"),

		Path = [filename:join([RootDir, lib, "*", ebin])],

		%% generate script
		ok = systools:make_script(filename:join(TmpDir, System), [{path, Path}]),

		%% make tar
		ok = systools:make_tar(filename:join(TmpDir, System),
													 [{outdir, TmpDir}, {erts, RootDir ++ ".lite"}, {path, Path}]),

		%% extract tar
		extract_tar(filename:join(TmpDir, System) ++ ".tar.gz", TmpDir),

 		%% create bin directory of distribution
		ok = file:make_dir(filename:join(TmpDir, bin)),
 		[] = os:cmd(io_lib:format("cp -r ~s ~s", [filename:join([RootDir ++ ".lite", bin]), TmpDir])),
		copy_file(filename:join([TmpDir, releases, RelVsn, "start.boot"]),
							filename:join([TmpDir, bin, "start.boot"])),

 		%% copy files related to system
		copy_file(filename:join(SrcDir, "COPYRIGHT") ++ "." ++ System,
							filename:join(TmpDir, "COPYRIGHT")),
		copy_file(filename:join(SrcDir, "LICENSE") ++ "." ++ System,
							filename:join(TmpDir, "LICENSE")),
		copy_file(filename:join(SrcDir, System) ++ ".nsi",
							filename:join(TmpDir, System) ++ ".nsi"),

		%% copy erlios starter to distribution
		copy_file(filename:join([SrcDir, "..", win32, "eb_pers.exe"]),
							filename:join(TmpDir, "eb_pers.exe")),
		copy_file(filename:join([SrcDir, "..", win32, "eb_pers_win.exe"]),
							filename:join(TmpDir, "eb_pers_win.exe")),

		%% create eb_pers.ini file
    StartErlIniFile = filename:join(TmpDir, "eb_pers.ini"),
    StartErlData = io_lib:fwrite("[erlios]~nerts=~s~nrel=~s~n", [ErtsVsn, RelVsn]),
    write_file(StartErlIniFile, StartErlData),

		%% strip files
		eklib:walk_fs([TmpDir], [],
									fun(X, _Fi, Acc) ->
													case filename:extension(X) of
															".beam" ->
																	case beam_lib:strip(X) of
																			{ok, {_Module, X}} ->
																					ok;
																			Error ->
																					io:format("~s: Failed to strip: ~p\n", [X, Error])
																	end;
															_ ->
																	ok
													end,
													{continue, Acc}
									end,
									ok),

		%% remove linux executables
		file:delete(TmpDir ++ "/lib/kdb-1.0.1/priv/kdb_drv"),

		%% create windows installer
		[] = os:cmd(io_lib:format("makensis -V2 ~s", [filename:join(TmpDir, System) ++ ".nsi"])),

		%% Copy release files to priv directory
		PrivDir = eklib:get_priv_dir(?MODULE),
		case Variant of
				production ->
						copy_file(filename:flatten([filename:join(TmpDir, RelVsn), ".win32.exe"]),
											filename:flatten([filename:join(PrivDir, RelVsn), ".win32.exe"]));
				sandbox ->
						copy_file(filename:flatten([filename:join(TmpDir, RelVsn), ".win32.exe"]),
											filename:flatten([filename:join(PrivDir, RelVsn), ".sandbox.win32.exe"]))
		end,

		%% remove tmp dir
		remove_dir_tree(TmpDir),
		ok.

%%--------------------------------------------------------------------
%% creates system release for linux
%%--------------------------------------------------------------------
create_linux(System, Variant) ->
		SrcDir = get_module_src_dir(?MODULE),
		RelFile = filename:join(SrcDir, System) ++ ".rel",
    {ok, [RelSpec]} = file:consult(RelFile),
    {release, {_RelName, RelVsn}, {erts, ErtsVsn}, _AppVsns} = RelSpec,
		TmpDir = eklib:temp_filename("/tmp"),
		RootDir = code:root_dir(),
		file:make_dir(TmpDir),

		%% copy files rel file into tmp dir
		{ok, _} = file:copy(RelFile, filename:join(TmpDir, System) ++ ".rel"),

		Path = [filename:join([RootDir, lib, "*", ebin])],

		%% generate script
		ok = systools:make_script(filename:join(TmpDir, System), [{path, Path}]),

		%% make tar
		ok = systools:make_tar(filename:join(TmpDir, System),
													 [{outdir, TmpDir}, {erts, RootDir}, {path, Path}]),

		%% extract and remove tar
		extract_tar(filename:join(TmpDir, System) ++ ".tar.gz", TmpDir),
		file:delete(filename:join(TmpDir, System) ++ ".tar.gz"),

		%% Fix binary directories
		ok = file:make_dir(filename:join(TmpDir, bin)),
		TmpBinDir = filename:join([TmpDir, bin]),
    ErtsBinDir = filename:join([TmpDir, "erts-" ++ ErtsVsn, bin]),
		%% delete erl and start from erts bin directoriy
    file:delete(filename:join([ErtsBinDir, "erl"])),
    file:delete(filename:join([ErtsBinDir, "start"])),
		%%  copy epmd, run_erl and to_erl from erts to bin directory
    copy_file(filename:join([ErtsBinDir, "epmd"]),
              filename:join([TmpBinDir, "epmd"]), [preserve]),
    copy_file(filename:join([ErtsBinDir, "run_erl"]),
              filename:join([TmpBinDir, "run_erl"]), [preserve]),
    copy_file(filename:join([ErtsBinDir, "to_erl"]),
              filename:join([TmpBinDir, "to_erl"]), [preserve]),
		%% copy start.boot to bin directory
		copy_file(filename:join([TmpDir, releases, RelVsn, "start.boot"]),
							filename:join([TmpDir, bin, "start.boot"])),

 		%% copy files related to system
		copy_file(filename:join(SrcDir, "COPYRIGHT") ++ "." ++ System,
							filename:join(TmpDir, "COPYRIGHT")),
		copy_file(filename:join(SrcDir, "LICENSE") ++ "." ++ System,
							filename:join(TmpDir, "LICENSE")),

		%% create start_erl.data file
    StartErlDataFile = filename:join([TmpDir, "releases", "start_erl.data"]),
    StartErlData = io_lib:fwrite("~s ~s~n", [ErtsVsn, RelVsn]),
    write_file(StartErlDataFile, StartErlData),

		%% strip files
		eklib:walk_fs([TmpDir], [],
									fun(X, _Fi, Acc) ->
													case filename:extension(X) of
															".beam" ->
																	case beam_lib:strip(X) of
																			{ok, {_Module, X}} ->
																					ok;
																			Error ->
																					io:format("~s: Failed to strip: ~p\n", [X, Error])
																	end;
															_ ->
																	ok
													end,
													{continue, Acc}
									end,
									ok),

		%% remove windows executables
		file:delete(TmpDir ++ "/lib/kdb-1.0.1/priv/kdb_drv.exe"),

		%% Recreate tar file from contents in directory
 		PrivDir = eklib:get_priv_dir(?MODULE),
		ReleaseTarName =
				case Variant of
						production ->
								filename:join(PrivDir, RelVsn) ++ ".tar.gz";
						sandbox ->
								filename:join(PrivDir, RelVsn) ++ ".sandbox.tar.gz"
				end,
		file:delete(ReleaseTarName),
    {ok, Tar} = erl_tar:open(ReleaseTarName, [write, compressed]),
    {ok, Cwd} = file:get_cwd(),
    file:set_cwd(TmpDir),
    erl_tar:add(Tar, "COPYRIGHT", []),
    erl_tar:add(Tar, "LICENSE", []),
    erl_tar:add(Tar, "bin", []),
    erl_tar:add(Tar, "erts-" ++ ErtsVsn, []),
    erl_tar:add(Tar, "releases", []),
    erl_tar:add(Tar, "lib", []),
    erl_tar:close(Tar),
    file:set_cwd(Cwd),

 		%% remove tmp dir
 		remove_dir_tree(TmpDir),
		ok.

%%--------------------------------------------------------------------
%% installs system release for linux
%%--------------------------------------------------------------------
install(System, Variant, RootDir) ->
		SrcDir = get_module_src_dir(?MODULE),
		RelFileName = filename:join(SrcDir, System),
    {ok, [RelSpec]} = file:consult(RelFileName ++ ".rel"),
    {release, {_RelName, RelVsn}, {erts, _ErtsVsn}, _AppVsns} = RelSpec,
 		PrivDir = eklib:get_priv_dir(?MODULE),
		TarFile =
				case Variant of
						production ->
								filename:join(PrivDir, RelVsn) ++ ".tar.gz";
						sandbox ->
								filename:join(PrivDir, RelVsn) ++ ".sandbox.tar.gz"
				end,
    io:fwrite("Extracting ~s ...~n", [TarFile]),
    extract_tar(TarFile, RootDir),
    StartErlDataFile = filename:join([RootDir, "releases", "start_erl.data"]),
    {ok, StartErlData} = read_txt_file(StartErlDataFile),
    [ErlVsn, _RelVsn| _] = string:tokens(StartErlData, " \n"),
    ErtsBinDir = filename:join([RootDir, "erts-" ++ ErlVsn, "bin"]),
    BinDir = filename:join([RootDir, "bin"]),
    io:fwrite("Substituting in erl.src, start.src and start_erl.src to\n"
              "form erl, start and start_erl ...\n"),
    subst_src_scripts(["erl", "start", "start_erl"], ErtsBinDir, BinDir,
                      [{"FINAL_ROOTDIR", RootDir}, {"EMU", "beam"}],
                      [preserve]),
    io:fwrite("Creating the RELEASES file ...\n"),
    create_RELEASES(RootDir,
                    filename:join([RootDir, "releases", RelFileName])).

%% LOCALS

%% extract_tar(TarFile, DestDir)
%%
extract_tar(TarFile, DestDir) ->
    erl_tar:extract(TarFile, [{cwd, DestDir}, compressed]).

create_RELEASES(DestDir, RelFileName) ->
    release_handler:create_RELEASES(DestDir, RelFileName ++ ".rel").

subst_src_scripts(Scripts, SrcDir, DestDir, Vars, Opts) ->
    lists:foreach(fun(Script) ->
                          subst_src_script(Script, SrcDir, DestDir,
                                           Vars, Opts)
                  end, Scripts).

subst_src_script(Script, SrcDir, DestDir, Vars, Opts) ->
    subst_file(filename:join([SrcDir, Script ++ ".src"]),
               filename:join([DestDir, Script]),
               Vars, Opts).

subst_file(Src, Dest, Vars, Opts) ->
    {ok, Conts} = read_txt_file(Src),
    NConts = subst(Conts, Vars),
    write_file(Dest, NConts),
    case lists:member(preserve, Opts) of
        true ->
            {ok, FileInfo} = file:read_file_info(Src),
            file:write_file_info(Dest, FileInfo);
        false ->
            ok
    end.

%% subst(Str, Vars)
%% Vars = [{Var, Val}]
%% Var = Val = string()
%% Substitute all occurrences of %Var% for Val in Str, using the list
%% of variables in Vars.
%%
subst(Str, Vars) ->
		subst(Str, Vars, []).

subst([$%, C| Rest], Vars, Result) when $A =< C, C =< $Z ->
		subst_var([C| Rest], Vars, Result, []);
subst([$%, C| Rest], Vars, Result) when $a =< C, C =< $z ->
		subst_var([C| Rest], Vars, Result, []);
subst([$%, C| Rest], Vars, Result) when  C == $_ ->
		subst_var([C| Rest], Vars, Result, []);
subst([C| Rest], Vars, Result) ->
		subst(Rest, Vars, [C| Result]);
subst([], _Vars, Result) ->
		lists:reverse(Result).

subst_var([$%| Rest], Vars, Result, VarAcc) ->
		Key = lists:reverse(VarAcc),
		case lists:keysearch(Key, 1, Vars) of
				{value, {Key, Value}} ->
						subst(Rest, Vars, lists:reverse(Value, Result));
				false ->
						subst(Rest, Vars, [$%| VarAcc ++ [$%| Result]])
		end;
subst_var([C| Rest], Vars, Result, VarAcc) ->
		subst_var(Rest, Vars, Result, [C| VarAcc]);
subst_var([], Vars, Result, VarAcc) ->
		subst([], Vars, [VarAcc ++ [$%| Result]]).

copy_file(Src, Dest) ->
		copy_file(Src, Dest, []).

copy_file(Src, Dest, Opts) ->
    {ok, InFd} = file:open(Src, [raw, binary, read]),
    {ok, OutFd} = file:open(Dest, [raw, binary, write]),
    do_copy_file(InFd, OutFd),
    file:close(InFd),
    file:close(OutFd),
    case lists:member(preserve, Opts) of
        true ->
            {ok, FileInfo} = file:read_file_info(Src),
            file:write_file_info(Dest, FileInfo);
        false ->
            ok
    end.

do_copy_file(InFd, OutFd) ->
    case file:read(InFd, ?BUFSIZE) of
        {ok, Bin} ->
            file:write(OutFd, Bin),
            do_copy_file(InFd, OutFd);
        eof  ->
            ok
    end.

write_file(FName, Conts) ->
    {ok, Fd} = file:open(FName, [write]),
    file:write(Fd, Conts),
    file:close(Fd).

read_txt_file(File) ->
    {ok, Bin} = file:read_file(File),
    {ok, binary_to_list(Bin)}.

remove_dir_tree(Dir) ->
    remove_all_files(".", [Dir]).

remove_all_files(Dir, Files) ->
    lists:foreach(fun(File) ->
                          FilePath = filename:join([Dir, File]),
                          {ok, FileInfo} = file:read_file_info(FilePath),
                          case FileInfo#file_info.type of
                              directory ->
                                  {ok, DirFiles} = file:list_dir(FilePath),
                                  remove_all_files(FilePath, DirFiles),
                                  file:del_dir(FilePath);
                              _ ->
                                  file:delete(FilePath)
                          end
                  end, Files).

