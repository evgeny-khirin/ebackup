%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eb_pers_bckp.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Backup engine.
%%%
%%% Created :  7 Feb 2010 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(eb_pers_bckp).

-include_lib("kernel/include/file.hrl").
-include("eklib/include/debug.hrl").
-include("eb_pers/src/eb_pers_bckp.hrl").

%% API
-export([sources/0,
				 add_source/1,
				 delete_source/1,
				 excludes/0,
				 add_exclude/1,
				 delete_exclude/1,
				 backup/0,
				 restart_db/0,
				 ver_info_to_file_info/1,
				 ver_info_status/1,
				 open_file/2,
				 read_file_block/2,
				 close_file/1,
				 print_snapshots/0,
				 stats/0,
				 restore/2,
				 start_db_cleaner/0,
				 notify_cleaner_start_job/1,
				 notify_cleaner_end_job/1,
				 storage_used/1
				]).

%%--------------------------------------------------------------------
%% Backup/restore session state
%%--------------------------------------------------------------------
-record(state, {status = ok,
								n2id,														% name to id table
								snapshots,											% snapshots table
								snap_id,												% current snapshot id
								ver_info,												% version info table
								log,														% errors log table
								files_idx,											% file storage descriptor
								stm,														% Storage manager table, see
																								% init_db for details.
								dedup,													% deduplication table
								arc_file,												% archive file
								restore_tmp											% temporary restore table
							 }).

%%--------------------------------------------------------------------
%% Function: sources() -> List
%% Description: Returns list of sources.
%%--------------------------------------------------------------------
sources() ->
		F = fun() ->
								{ok, T} = kdb:open_table(eb_pers_conf, ?MODULE),
								{ok, I} = kdb:seek(eb_pers_conf, T, {source, []}, next),
								Res = sources(I, kdb:next(eb_pers_conf, I), []),
								kdb:close_iter(eb_pers_conf, I),
								kdb:close_table(eb_pers_conf, T),
								Res
				end,
		{atomic, Sources} = kdb:transaction(eb_pers_conf, F),
		Sources.

sources(I, {ok, {source, Src}, _}, Res) ->
		sources(I, kdb:next(eb_pers_conf, I), [Src | Res]);
sources(_, _ , Res) ->
		lists:reverse(Res).

%%--------------------------------------------------------------------
%% Function: add_source(Path) -> ok | {error, Error}
%% Description: Adds file or directory to list of sources.
%%--------------------------------------------------------------------
add_source(Path) ->
		Native = filename:nativename(Path),
		add_source(Native, filename:pathtype(Native)).

add_source(Path, absolute) ->
		add_source(Path, kdb:get_path_type(eb_pers_conf, Path));
add_source(Path, local) ->
		eb_pers_conf:set_value(?MODULE, {source, Path}, []);
add_source(Path, undefined) ->
		eb_pers_conf:set_value(?MODULE, {source, Path}, []);
add_source(_Path, remote) ->
		{error, remote_path};
add_source(_Path, _) ->
		{error, not_absolute}.

%%--------------------------------------------------------------------
%% Function: delete_source(Src) -> ok | {error, Error}
%% Description: Deletes file or directory from list of sources.
%%--------------------------------------------------------------------
delete_source(Src) ->
		eb_pers_conf:delete(?MODULE, {source, Src}).

%%--------------------------------------------------------------------
%% Function: excludes() -> List
%% Description: Returns list of excludes.
%%--------------------------------------------------------------------
excludes() ->
		F = fun() ->
								{ok, T} = kdb:open_table(eb_pers_conf, ?MODULE),
								{ok, I} = kdb:seek(eb_pers_conf, T, {exclude, []}, next),
								Res = excludes(I, kdb:next(eb_pers_conf, I), []),
								kdb:close_iter(eb_pers_conf, I),
								kdb:close_table(eb_pers_conf, T),
								Res
				end,
		{atomic, Excludes} = kdb:transaction(eb_pers_conf, F),
		Excludes.

excludes(I, {ok, {exclude, Exclude}, _}, Res) ->
		excludes(I, kdb:next(eb_pers_conf, I), [Exclude | Res]);
excludes(_, _ , Res) ->
		lists:reverse(Res).

%%--------------------------------------------------------------------
%% Function: add_exclude(Path) -> ok | {error, Error}
%% Description: Adds file or directory to list of excludes.
%%--------------------------------------------------------------------
add_exclude(Path) ->
		Native = filename:nativename(Path),
		add_exclude(Native, filename:pathtype(Native)).

add_exclude(Path, absolute) ->
		eb_pers_conf:set_value(?MODULE, {exclude, Path}, []);
add_exclude(_Path, _) ->
		{error, not_absolute}.



%%--------------------------------------------------------------------
%% Function: delete_exclude(Exclude) -> ok | {error, Error}
%% Description: Deletes file or directory from list of excludes.
%%--------------------------------------------------------------------
delete_exclude(Exclude) ->
		eb_pers_conf:delete(?MODULE, {exclude, Exclude}).

%%--------------------------------------------------------------------
%% Function: backup() -> ok | {error,Error}
%% Description: Performs backup task.
%%--------------------------------------------------------------------
backup() ->
		%% link backup task to KDB server
		Ref = notify_cleaner_start_job(true),
		link(whereis(?MODULE)),
		case eb_pers_conf:get_value(?MODULE, destination) of
				not_found ->
						ok;
				{ok, Destination} ->
						case eklib:mkdir_ensure(Destination) of
								ok ->
										ok = backup(Destination);
								_ ->
										ok
						end
		end,
		%% unlink backup task from KDB server
		unlink(whereis(?MODULE)),
		notify_cleaner_end_job(Ref),
		ok.

%%--------------------------------------------------------------------
%% Function: restart_db() -> ok | {error,Error}
%% Description: Restarts backup DB server.
%%--------------------------------------------------------------------
restart_db() ->
		%% Stop list
		StopList = [eb_pers_bckp_kdb,
								eb_pers_bckp_tm,
								eb_pers_bckp_stm,
								eb_pers_bckp_bm,
								eb_pers_bckp_data_crc,
								eb_pers_bckp_logger,
								eb_pers_bckp_ld_pp,
								eb_pers_bckp_ld_bd,
								eb_pers_bckp_log_cache,
								eb_pers_bckp_log_dwrite,
								eb_pers_bckp_log_crc,
								eb_pers_bckp_data_part,
								eb_pers_bckp_factor,
								eb_pers_bckp_log_part,
								eb_pers_bckp_file],
		%% set stop list
		kdb:set_stop_list(?MODULE, StopList),
		%% stop DB objects
		lists:foreach(fun(Obj) ->
													kdb:stop_object(?MODULE, Obj)
									end,
									StopList),
		%% restart DB
		case eb_pers_conf:get_value(?MODULE, destination) of
				not_found ->
						ok;
				{ok, Destination} ->
						case eklib:mkdir_ensure(Destination) of
								ok ->
										start_db(Destination);
								_ ->
										ok
						end
		end.

%%--------------------------------------------------------------------
%% system parameters
%%--------------------------------------------------------------------
-define(LOG_BLOCK_SIZE, (4 * 1024)).																% 4 KB
-define(DATA_BLOCK_SIZE, (128 * 1024)).															% 128 KB
-define(FACTOR, (?DATA_BLOCK_SIZE div ?LOG_BLOCK_SIZE)).						% factor
-define(LOG_CAPACITY, (256 * 1024 * 1024 div ?LOG_BLOCK_SIZE)).			% 256 MB
-define(DATA_PART_START,																						% start of data partition
				(?LOG_CAPACITY * ?LOG_BLOCK_SIZE div ?DATA_BLOCK_SIZE)).
-define(DATA_CAPACITY,													                    %  % 2^64 B (4 EB)
				((16#ffffffffffffffff div ?DATA_BLOCK_SIZE) - ?DATA_PART_START)).
-define(CP_LOG_SIZE, (?LOG_CAPACITY * ?LOG_BLOCK_SIZE div 2)).			% half of log size
-define(BM_CACHE_CAPACITY, (64 * 1024 * 1024 div ?DATA_BLOCK_SIZE)). % 64 MB
-define(KDB_FILE, "eb_pers.idx").
-define(ARC_FILE, "eb_pers.dat").
-define(DEDUP_BLOCK_SIZE, (128 * 1024)).				% 128 KB

%%--------------------------------------------------------------------
%% Start DB
%%--------------------------------------------------------------------
start_db(Destination) ->
		case catch start_db_1(Destination) of
				ok ->
						ok;
				Error ->
						?ERROR("Failed to start DB: ~p", [Error]),
						Error
		end.

start_db_1(Destination) ->
		%% Make sure that directory exists
		ok = eklib:mkdir_ensure(Destination),
		%% Common file
		{ok, _} = kdb:start_object(?MODULE, bd_file,
															 [{name, eb_pers_bckp_file},
																{block_size, ?LOG_BLOCK_SIZE},
																{file, filename:join(Destination, ?KDB_FILE)}]),
		%% Log partition
		{ok, _} = kdb:start_object(?MODULE, bd_part,
															 [{name, eb_pers_bckp_log_part},
																{block_device, eb_pers_bckp_file},
																{start, 0},
																{capacity, ?LOG_CAPACITY}]),
		%% Factor device
		{ok, _} = kdb:start_object(?MODULE, bd_factor,
															 [{name, eb_pers_bckp_factor},
																{block_device, eb_pers_bckp_file},
																{factor, ?FACTOR},
																{stop_underlaying, false}]),
		%% Data partition
		{ok, _} = kdb:start_object(?MODULE, bd_part,
															 [{name, eb_pers_bckp_data_part},
																{block_device, eb_pers_bckp_factor},
																{start, ?DATA_PART_START},
																{capacity, ?DATA_CAPACITY}]),
		%% Log CRC
		{ok, _} = kdb:start_object(?MODULE, bd_crc,
															 [{name, eb_pers_bckp_log_crc},
																{block_device, eb_pers_bckp_log_part}]),
		%% Log dwrite
		{ok, _} = kdb:start_object(?MODULE, bd_dwrite,
															 [{name, eb_pers_bckp_log_dwrite},
																{block_device, eb_pers_bckp_log_crc},
																{buffer_capacity, 64}]),
		%% Log cache
		{ok, LogCache} = kdb:start_object(?MODULE, bd_cache,
																			[{name, eb_pers_bckp_log_cache},
																			 {block_device, eb_pers_bckp_log_dwrite},
																			 {cache_capacity, 4}]),
		%% Format
		case kdb:ld_bd_is_formatted(?MODULE, LogCache, ?MODULE) of
				true ->
						ok;
				false ->
						ok = kdb:ld_bd_format(?MODULE, LogCache, ?MODULE)
		end,
		%% LD BD
		{ok, LdBd} = kdb:start_object(?MODULE, ld_bd,
																	[{name, eb_pers_bckp_ld_bd},
																	 {block_device, eb_pers_bckp_log_cache}]),
		%% LD PP
		{ok, _} = kdb:start_object(?MODULE, ld_pp,
															 [{name, eb_pers_bckp_ld_pp},
																{log_device, eb_pers_bckp_ld_bd}]),
		%% Logger
		{ok, _} = kdb:start_object(?MODULE, blogger,
															 [{name, eb_pers_bckp_logger},
																{log_device, eb_pers_bckp_ld_pp}]),
		%% TM
		{ok, _} = kdb:start_object(?MODULE, trans_mgr,
															 [{name, eb_pers_bckp_tm},
																{logger, eb_pers_bckp_logger},
																{buffer_manager, eb_pers_bckp_bm},
																{cp_log_size, ?CP_LOG_SIZE}]),
		%% Data CRC
		{ok, SysGen} = kdb:ld_bd_system_generation(?MODULE, LdBd),
		{ok, _} = kdb:start_object(?MODULE, bd_crc,
															 [{name, eb_pers_bckp_data_crc},
																{block_device, eb_pers_bckp_data_part},
																{trailer, SysGen}]),
		%% BM
		{ok, _} = kdb:start_object(?MODULE, buffer_mgr,
															 [{name, eb_pers_bckp_bm},
																{block_device, eb_pers_bckp_data_crc},
																{transaction_manager, eb_pers_bckp_tm},
																{cache_capacity, ?BM_CACHE_CAPACITY}]),
		%% STM
		{ok, _} = kdb:start_object(?MODULE, stm,
															 [{name, eb_pers_bckp_stm},
																{transaction_manager, eb_pers_bckp_tm},
																{buffer_manager, eb_pers_bckp_bm}]),
		%% KDB
		{ok, _} = kdb:start_object(?MODULE, kdb,
															 [{name, eb_pers_bckp_kdb},
																{transaction_manager, eb_pers_bckp_tm},
																{buffer_manager, eb_pers_bckp_bm},
																{storage_manager, eb_pers_bckp_stm}]),
		%% Perform recovery
		ok = kdb:recover(?MODULE),
		%% Initialize DB
		init_db().

%%--------------------------------------------------------------------
%% Opens file for reading by external module
%%--------------------------------------------------------------------
-record(file_descr, {arc_file,
										 files_idx,
										 file_name}).
open_file(Id, Ver) ->
		{ok, Destination} = eb_pers_conf:get_value(?MODULE, destination),
		{ok, ArcFile} = file:open(filename:join(Destination, ?ARC_FILE),
															[read, raw, binary]),
		{ok, FilesIdx} = kdb:open_table(?MODULE, files_idx),
		#file_descr{arc_file = ArcFile, files_idx = FilesIdx, file_name = {Id, Ver}}.

%%--------------------------------------------------------------------
%% Closes file
%%--------------------------------------------------------------------
close_file(#file_descr{arc_file = ArcFile, files_idx = FilesIdx}) ->
		kdb:close_table(?MODULE, FilesIdx),
		file:close(ArcFile).

%%--------------------------------------------------------------------
%% Reads file
%%--------------------------------------------------------------------
read_file_block(File, N) ->
		#file_descr{arc_file = ArcFile, files_idx = FilesIdx, file_name = FileName} = File,
		read_file_block(FilesIdx, ArcFile, FileName, N).

%%---------------------------------------------------------------------
%% prints snapshots
%% --------------------------------------------------------------------
print_snapshots() ->
		print_snapshots(-16#ffffffffffffffff).

print_snapshots(Prev) ->
		F = fun() ->
								{ok, T} = kdb:open_table(?MODULE, snapshots),
								{ok, I} = kdb:seek(?MODULE, T, {snap, Prev}, next),
								Res = print_snapshots(I, kdb:next(?MODULE, I), Prev, 128),
								ok = kdb:close_iter(?MODULE, I),
								ok = kdb:close_table(?MODULE, T),
								Res
				end,
		case kdb:transaction(?MODULE, F) of
				{atomic, eof} ->
						ok;
				{atomic, NewPrev} ->
						print_snapshots(NewPrev)
		end.

print_snapshots(_I, _, Prev, 0) ->
		Prev;
print_snapshots(I, {ok, {snap, Id}, SnapDescr}, _, Count) ->
		#snap_descr{date = Date, time = Time, status = Status,
								dirs = Dirs, files = Files, size = Size, errors = Errors} = SnapDescr,
		io:format("snapsot id ~p, date ~p, time ~p, status ~p, dirs ~p, files ~p, size ~p, errors ~p~n",
							[Id, Date, Time, Status, Dirs, Files, Size, Errors]),
		print_snapshots(I, kdb:next(?MODULE, I), Id, Count - 1);
print_snapshots(_I, _, _Prev, _Count) ->
		eof.

%%---------------------------------------------------------------------
%% collects backup storage statistics
%% --------------------------------------------------------------------
stats() ->
		{Blocks, Total, Dedup, Final} = stats({-1, -1, -1}, 0, 0, 0, 0),
		io:format("blocks: ~p\n", [Blocks]),
		io:format("total storage: ~p\n", [Total]),
		io:format("final storage: ~p\n", [Final]),
		io:format("dedup ratio: ~p\n", [(Total - Dedup) / Total]),
		io:format("compression ratio: ~p\n", [Final / (Total - Dedup)]),
		io:format("final ratio: ~p\n", [Final / Total]),
		ok.

stats(Prev00, Blocks00, Total00, Dedup00, Final00) ->
		F = fun() ->
								{ok, T} = kdb:open_table(?MODULE, dedup),
								{ok, I} = kdb:seek(?MODULE, T, Prev00, next),
								Res = stats(I, kdb:next(?MODULE, I), Blocks00, Total00, Dedup00, Final00, Prev00, 1024),
								ok = kdb:close_iter(?MODULE, I),
								ok = kdb:close_table(?MODULE, T),
								Res
				end,
		case kdb:transaction(?MODULE, F) of
				{atomic, {ok, Blocks10, Total10, Dedup10, Final10, Prev10}} ->
						stats(Prev10, Blocks10, Total10, Dedup10, Final10);
				{atomic, {eof, Blocks10, Total10, Dedup10, Final10}} ->
						{Blocks10, Total10, Dedup10, Final10}
		end.

stats(_I, _, Blocks, Total, Dedup, Final, Prev, 0) ->
		{ok, Blocks, Total, Dedup, Final, Prev};
stats(I, {ok, {_, BlockSize, _} = Prev, {RefCount, StoredSize, _}}, Blocks, Total, Dedup, Final, _, Count) ->
		stats(I, kdb:next(?MODULE, I),
					Blocks + 1,
					Total + BlockSize * RefCount,
					Dedup + BlockSize * (RefCount - 1),
					Final + StoredSize,
					Prev, Count - 1);
stats(_I, _, Blocks, Total, Dedup, Final, _, _) ->
		{eof, Blocks, Total, Dedup, Final}.

%%---------------------------------------------------------------------
%% restores snapshot
%% --------------------------------------------------------------------
restore(SnapId, Prefix) ->
		%% link backup task to KDB server
		link(whereis(?MODULE)),
		Ref = notify_cleaner_start_job(true),
		Res =
				case eklib:fs_object_exist(Prefix) of
						true ->
								{error, destination_exists};
						false ->
								Self = self(),
								F = fun() ->
														%% Check that no other restore are active
														case eb_pers_conf:get_value(?MODULE, restore) of
																not_found ->
																		eb_pers_conf:set_value(?MODULE, restore, Self);
																{ok, Pid} ->
																		case is_process_alive(Pid) of
																				true ->
																						{error, restore_running};
																				false ->
																						eb_pers_conf:set_value(?MODULE, restore, Self)
																		end
														end
										end,
								case kdb:transaction(eb_pers_conf, F) of
										{atomic, ok} ->
												Res1 = restore_1(SnapId, Prefix),
												eb_pers_conf:delete(?MODULE, restore),
												Res1;
										{atomic, Other} ->
												Other
								end
				end,
		%% unlink backup task from KDB server
		unlink(whereis(?MODULE)),
		notify_cleaner_end_job(Ref),
		Res.

restore_1(SnapId, Prefix) ->
		Self = self(),
		F = fun() ->
								%% mark snapshot as restoring
								{ok, T} = kdb:open_table(?MODULE, snapshots),
								{ok, SnapDescr} = kdb:lookup(?MODULE, T, {snap, SnapId}),
								#snap_descr{status = Status} = SnapDescr,
								Res =
										case Status of
												{Action, Pid} ->
														case is_process_alive(Pid) of
																true ->
																		{error, snapshot_busy};
																false ->
																		case Action of
																				{restore, SavedStatus} ->
																						ok = kdb:insert(?MODULE, T, {snap, SnapId},
																														SnapDescr#snap_descr{status = {{restore, SavedStatus}, Self}});
																				_ ->
																						ok = kdb:insert(?MODULE, T, {snap, SnapId},
																														SnapDescr#snap_descr{status = {{restore, Status}, Self}})
																		end
														end;
												_ ->
														ok = kdb:insert(?MODULE, T, {snap, SnapId},
																						SnapDescr#snap_descr{status = {{restore, Status}, Self}})
										end,
								ok = kdb:close_table(?MODULE, T),
								Res
				end,
		case kdb:transaction(?MODULE, F) of
				{atomic, ok} ->
						restore_2(SnapId, Prefix);
				{atomic, Error} ->
						Error
		end.

restore_2(SnapId, Prefix) ->
		ok = kdb:drop_table(?MODULE, restore_tmp),
		%% init restore state
		{ok, Destination} = eb_pers_conf:get_value(?MODULE, destination),
		{ok, NameToId} = kdb:open_table(?MODULE, n2id),
		{ok, Snapshots} = kdb:open_table(?MODULE, snapshots),
		{ok, VerInfo} = kdb:open_table(?MODULE, ver_info),
		{ok, FilesIdx} = kdb:open_table(?MODULE, files_idx),
		{ok, ArcFile} = file:open(filename:join(Destination, ?ARC_FILE),
															[read, raw, binary]),
		{ok, RestoreTmp} = kdb:open_table(?MODULE, restore_tmp),
		State = #state{n2id = NameToId,
									 snapshots = Snapshots,
									 snap_id = SnapId,
									 ver_info = VerInfo,
									 files_idx = FilesIdx,
									 arc_file = ArcFile,
									 restore_tmp = RestoreTmp},
		%% Perfor restore
		ok = restore_pass1(State, Prefix),
		ok = restore_pass2(State),
		%% drop temporary table
		ok = kdb:drop_table(?MODULE, restore_tmp),
		F = fun() ->
								%% restore snapshot status
								{ok, SnapDescr} = kdb:lookup(?MODULE, Snapshots, {snap, SnapId}),
								#snap_descr{status = {{restore, SavedStatus}, _Pid}} = SnapDescr,
								ok = kdb:insert(?MODULE, Snapshots, {snap, SnapId}, SnapDescr#snap_descr{status = SavedStatus})
				end,
		{atomic, ok} = kdb:transaction(?MODULE, F),
		%% Close tables
		kdb:close_table(?MODULE, Snapshots),
		kdb:close_table(?MODULE, NameToId),
		kdb:close_table(?MODULE, VerInfo),
		kdb:close_table(?MODULE, FilesIdx),
		kdb:close_table(?MODULE, RestoreTmp),
		file:close(ArcFile),
		ok.

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% initializes DB
%%--------------------------------------------------------------------
init_db() ->
		F = fun() ->
								{ok, T} = kdb:open_table(?MODULE, stm),
								case kdb:lookup(?MODULE, T, init_done) of
										{ok, true} ->
												ok;
										not_found ->
												ok = kdb:insert(?MODULE, T, init_done, true),
												%% Start address => Length
												ok = kdb:insert(?MODULE, T, 0, 16#ffffffffffffffff),
												%% Hole descriptor: {Size, StartAddr}
												ok = kdb:insert(?MODULE, T, {16#ffffffffffffffff, 0}, [])
								end,
								ok = kdb:close_table(?MODULE, T)
				end,
		{atomic, ok} = kdb:transaction(?MODULE, F),
		ok.

%%--------------------------------------------------------------------
%% Real backup routine.
%%--------------------------------------------------------------------
backup(Destination) ->
		%% Execute command before backup started
		case eb_pers_conf:get_value(?MODULE, execute_before, "") of
				{ok, ""} ->
						ok;
				{ok, ExecuteBefore} ->
						os:cmd(ExecuteBefore)
		end,
		%% get backup parameters
		Sources = sources(),
		Excludes = excludes(),
		%% init backup state
		{ok, NameToId} = kdb:open_table(?MODULE, n2id),
		{ok, Snapshots} = kdb:open_table(?MODULE, snapshots),
		{ok, VerInfo} = kdb:open_table(?MODULE, ver_info),
		{ok, Log} = kdb:open_table(?MODULE, log),
		{ok, FilesIdx} = kdb:open_table(?MODULE, files_idx),
		{ok, Stm} = kdb:open_table(?MODULE, stm),
		{ok, Dedup} = kdb:open_table(?MODULE, dedup),
		{ok, ArcFile} = file:open(filename:join(Destination, ?ARC_FILE),
															[read, write, raw, binary]),
		State00 = #state{n2id = NameToId,
										 snapshots = Snapshots,
										 ver_info = VerInfo,
										 log = Log,
										 files_idx = FilesIdx,
										 stm = Stm,
										 dedup = Dedup,
										 arc_file = ArcFile},
		State10 = new_snapshot(State00),
		%% perform backup
		KdbExclude = filename:nativename(filename:join(Destination, ?KDB_FILE)),
		ArcExclude = filename:nativename(filename:join(Destination, ?ARC_FILE)),
		State20 =
				eklib:walk_fs(Sources,
											[KdbExclude, ArcExclude | Excludes],
											fun (Src, {error, Error}, State) ->
															log_error(State, Src, Error),
															{continue, State#state{status = error}};
													(Src, SrcFi, #state{status = Status00} = State) ->
															case kdb:get_path_type(?MODULE, Src) of
																	remote ->
																			log_error(State, Src, "remote path, skipping"),
																			{skip, State#state{status = error}};
																	_ ->
																			Res = (catch case SrcFi#file_info.type of
																											 symlink ->
																													 backup_symlink(State, Src, SrcFi);
																											 directory ->
																													 backup_dir(State, Src, SrcFi);
																											 regular ->
																													 backup_file(State, Src, SrcFi);
																											 _ ->
																													 {continue, ok}
																									 end),
																			case Res of
																					{continue, Status10} ->
																							Status20 =
																									case Status00 of
																											ok -> Status10;
																											error -> error
																									end,
																							{continue, State#state{status = Status20}};
																					{skip, Status10} ->
																							Status20 =
																									case Status00 of
																											ok -> Status10;
																											error -> error
																									end,
																							{skip, State#state{status = Status20}};
																					Error ->
																							log_error(State, Src, io_lib:format("unexpected: ~p", [Error])),
																							{continue, State#state{status = error}}
																			end
															end
											end,
											State10),
		%% close snapshot
		close_snapshot(State20),
		%% Execute command after backup finished
		case eb_pers_conf:get_value(?MODULE, execute_after, "") of
				{ok, ""} ->
						ok;
				{ok, ExecuteAfter} ->
						os:cmd(ExecuteAfter)
		end,
		%% remove redundant snapshots
 		clear_history(State20),
		%% close tables
		kdb:close_table(?MODULE, Snapshots),
		kdb:close_table(?MODULE, NameToId),
		kdb:close_table(?MODULE, VerInfo),
		kdb:close_table(?MODULE, Log),
		kdb:close_table(?MODULE, FilesIdx),
		kdb:close_table(?MODULE, Stm),
		kdb:close_table(?MODULE, Dedup),
		file:close(ArcFile),
		%% flush transaction log
		kdb:transaction(?MODULE, fun() -> ok end, hard),
		ok.


%%--------------------------------------------------------------------
%% logs backup error
%%--------------------------------------------------------------------
log_error(State, Src, Error) ->
		#state{log = Log, snap_id = SnapId, snapshots = Snapshots} = State,
		F = fun() ->
								ok = kdb:insert(?MODULE, Log, {SnapId, Src}, Error),
								{ok, SnapDescr} = kdb:lookup(?MODULE, Snapshots, {snap, SnapId}),
								#snap_descr{errors = Errors} = SnapDescr,
								ok = kdb:insert(?MODULE, Snapshots, {snap, SnapId},
																SnapDescr#snap_descr{errors = Errors + 1})
				end,
		{atomic, ok} = kdb:transaction(?MODULE, F),
		ok.

%%--------------------------------------------------------------------
%% removes snapshot from error log
%%--------------------------------------------------------------------
clear_log(State, SnapId) ->
		#state{log = Log} = State,
		F = fun() ->
								{ok, I} = kdb:seek(?MODULE, Log, {SnapId, []}, next),
								Res = clear_log(Log, SnapId, I, kdb:next(?MODULE, I), 128, []),
								kdb:close_iter(?MODULE, I),
								Res
				end,
		case kdb:transaction(?MODULE, F) of
				{atomic, eof} ->
						ok;
				{atomic, continue} ->
						clear_log(State, SnapId)
		end.

clear_log(T, SnapId, _I, {ok, {SnapId, _}, _}, 0, Keys) ->
		lists:foreach(fun(X) ->
													kdb:remove(?MODULE, T, X)
									end,
									Keys),
		continue;
clear_log(T, SnapId, I, {ok, {SnapId, _} = K, _}, Count, Keys) ->
		clear_log(T, SnapId, I, kdb:next(?MODULE, I), Count - 1, [K | Keys]);
clear_log(T, _SnapId, _I, _, _Count, Keys) ->
		lists:foreach(fun(X) ->
													kdb:remove(?MODULE, T, X)
									end,
									Keys),
		eof.

%%--------------------------------------------------------------------
%% symlink backup
%%--------------------------------------------------------------------
backup_symlink(State, Src, SrcFi) ->
		#state{snapshots = T, snap_id = SnapId} = State,
		F = fun() ->
								Id = name_to_id(State, Src),
								{MustBackup, CurrVer, AlreadyInSnapshot} =
										case last_file_info(State, Id) of
												not_found ->
														{true, 0, false};
												{ok, LastInfo, V} ->
														case kdb:lookup(?MODULE, T, {SnapId, Id}) of
																not_found ->
																		{not_equal =:= compare_file_info(SrcFi, LastInfo), V, false};
																{ok, _} ->
																		{false, V, true}
														end
										end,
								if
										MustBackup ->
												case file:read_link(Src) of
														{error, Error} ->
																log_error(State, Src, file:format_error(Error)),
																error;
														{ok, LinkTarget} ->
																BinTarget = list_to_binary(LinkTarget),
																NewVer = new_version(State, Id, SrcFi),
																FileName = {Id, NewVer},
																{ok, _} = write_file_block(State, FileName, 0, BinTarget, true),
																ok = set_version_status(State, Id, NewVer, ok),
																ok = kdb:insert(?MODULE, T, {SnapId, Id}, NewVer),
																%% Update snapshot stats
																{ok, SnapDescr} = kdb:lookup(?MODULE, T, {snap, SnapId}),
																#snap_descr{files = Files, size = Size} = SnapDescr,
																ok = kdb:insert(?MODULE, T, {snap, SnapId},
																								SnapDescr#snap_descr{files = Files + 1,
																																		 size = Size + SrcFi#file_info.size})
												end;
										AlreadyInSnapshot ->
												ok;
										true ->
												ok = inc_version_ref_count(State, Id, CurrVer),
												ok = kdb:insert(?MODULE, T, {SnapId, Id}, CurrVer),
												%% Update snapshot stats
												{ok, SnapDescr} = kdb:lookup(?MODULE, T, {snap, SnapId}),
												#snap_descr{files = Files, size = Size} = SnapDescr,
												ok = kdb:insert(?MODULE, T, {snap, SnapId},
																				SnapDescr#snap_descr{files = Files + 1,
																														 size = Size + SrcFi#file_info.size})
								end
				end,
		{atomic, Status} = kdb:transaction(?MODULE, F),
		{continue, Status}.

%%--------------------------------------------------------------------
%% directory backup
%%--------------------------------------------------------------------
backup_dir(State, Src, SrcFi) ->
		#state{snapshots = T, snap_id = SnapId} = State,
		F = fun() ->
								Id = name_to_id(State, Src),
								{MustBackup, CurrVer, AlreadyInSnapshot} =
										case last_file_info(State, Id) of
												not_found ->
														{true, 0, false};
												{ok, LastInfo, V} ->
														case kdb:lookup(?MODULE, T, {SnapId, Id}) of
																not_found ->
																		{not_equal =:= compare_file_info(SrcFi, LastInfo), V, false};
																{ok, _} ->
																		{false, V, true}
														end
										end,
								if
										MustBackup ->
												NewVer = new_version(State, Id, SrcFi),
												ok = set_version_status(State, Id, NewVer, ok),
												ok = kdb:insert(?MODULE, T, {SnapId, Id}, NewVer),
												%% Update snapshot stats
												{ok, SnapDescr} = kdb:lookup(?MODULE, T, {snap, SnapId}),
												#snap_descr{dirs = Dirs, size = Size} = SnapDescr,
												ok = kdb:insert(?MODULE, T, {snap, SnapId},
																				SnapDescr#snap_descr{dirs = Dirs + 1,
																														 size = Size + SrcFi#file_info.size}),
												{continue, ok};
										AlreadyInSnapshot ->
												{skip, ok};
										true ->
												ok = inc_version_ref_count(State, Id, CurrVer),
												ok = kdb:insert(?MODULE, T, {SnapId, Id}, CurrVer),
												%% Update snapshot stats
												{ok, SnapDescr} = kdb:lookup(?MODULE, T, {snap, SnapId}),
												#snap_descr{dirs = Dirs, size = Size} = SnapDescr,
												ok = kdb:insert(?MODULE, T, {snap, SnapId},
																				SnapDescr#snap_descr{dirs = Dirs + 1,
																														 size = Size + SrcFi#file_info.size}),
												{continue, ok}
								end
				end,
		{atomic, Res} = kdb:transaction(?MODULE, F),
		Res.

%%--------------------------------------------------------------------
%% file backup
%%--------------------------------------------------------------------
backup_file(State, Src, SrcFi) ->
		#state{snapshots = T, snap_id = SnapId} = State,
		F = fun() ->
								Id = name_to_id(State, Src),
								{MustBackup, CurrVer, AlreadyInSnapshot} =
										case last_file_info(State, Id) of
												not_found ->
														{true, 0, false};
												{ok, LastInfo, V} ->
														case kdb:lookup(?MODULE, T, {SnapId, Id}) of
																not_found ->
																		{not_equal =:= compare_file_info(SrcFi, LastInfo), V, false};
																{ok, _} ->
																		{false, V, true}
														end
										end,
								if
										MustBackup ->
												NewVer = new_version(State, Id, SrcFi),
												ok = kdb:insert(?MODULE, T, {SnapId, Id}, NewVer),
												{true, Id, NewVer};
										AlreadyInSnapshot ->
												{false, Id, CurrVer};
										true ->
												ok = inc_version_ref_count(State, Id, CurrVer),
												ok = kdb:insert(?MODULE, T, {SnapId, Id}, CurrVer),
												%% Update snapshot stats
												{ok, SnapDescr} = kdb:lookup(?MODULE, T, {snap, SnapId}),
												#snap_descr{files = Files, size = Size} = SnapDescr,
												ok = kdb:insert(?MODULE, T, {snap, SnapId},
																				SnapDescr#snap_descr{files = Files + 1,
																														 size = Size + SrcFi#file_info.size}),
												{false, Id, CurrVer}
								end
				end,
		{atomic, {MustBackup, Id, Ver}} = kdb:transaction(?MODULE, F),
		Status =
				if
						MustBackup ->
								FileName = {Id, Ver},
								case copy_file(State, Src, FileName) of
										ok ->
												F1 = fun() ->
																		 set_version_status(State, Id, Ver, ok),
																		 %% Update snapshot stats
																		 {ok, SnapDescr} = kdb:lookup(?MODULE, T, {snap, SnapId}),
																		 #snap_descr{files = Files, size = Size} = SnapDescr,
																		 ok = kdb:insert(?MODULE, T, {snap, SnapId},
																										 SnapDescr#snap_descr{files = Files + 1,
																																					size = Size + SrcFi#file_info.size})
														 end,
												{atomic, ok} = kdb:transaction(?MODULE, F1),
												ok;
										{error, Error} ->
												log_error(State, Src, file:format_error(Error)),
												error
								end;
						true ->
								ok
				end,
		{continue, Status}.

%%--------------------------------------------------------------------
%% creates new snapshot, shapshots IDs grow in negative direction
%%--------------------------------------------------------------------
new_snapshot(State) ->
		#state{snapshots = T} = State,
		Pid = self(),
		F = fun() ->
								Id =
										case kdb:lookup(?MODULE, T, free) of
												not_found ->
														0;
												{ok, I} ->
														I
										end,
								ok = kdb:insert(?MODULE, T, free, Id - 1),
								ok = kdb:insert(?MODULE, T, {snap, Id}, #snap_descr{status = {run, Pid}}),
								Id
				end,
		{atomic, Id} = kdb:transaction(?MODULE, F),
		State#state{snap_id = Id}.

%%--------------------------------------------------------------------
%% closes snapshot
%%--------------------------------------------------------------------
close_snapshot(State) ->
		#state{snapshots = T, snap_id = Id, status = Status} = State,
		F = fun() ->
								{ok, SnapDescr} = kdb:lookup(?MODULE, T, {snap, Id}),
								ok = kdb:insert(?MODULE, T, {snap, Id}, SnapDescr#snap_descr{status = Status})
				end,
		{atomic, ok} = kdb:transaction(?MODULE, F, hard),
		ok.

%%--------------------------------------------------------------------
%% deletes snapshot
%%--------------------------------------------------------------------
delete_snapshot(State, Id) ->
		#state{snapshots = T} = State,
		ok = delete_snapshot_files(State, Id),
		ok = clear_log(State, Id),
		ok = kdb:remove(?MODULE, T, {snap, Id}).

%%--------------------------------------------------------------------
%% deletes all snapshot files
%%--------------------------------------------------------------------
delete_snapshot_files(State, SnapId) ->
		delete_snapshot_files(State, SnapId, -1).

delete_snapshot_files(State, SnapId, LastId) ->
		#state{snapshots = T} = State,
		F = fun() ->
								{ok, I} = kdb:seek(?MODULE, T, {SnapId, LastId}, next),
								Res = delete_snapshot_files(I, SnapId, kdb:next(?MODULE, I), [], 1024),
								ok = kdb:close_iter(?MODULE, I),
								Res
				end,
		case kdb:transaction(?MODULE, F, read_only) of
				{atomic, []} ->
						ok;
				{atomic, List} ->
						lists:foreach(fun({Id, Ver}) ->
																	ok = dec_version_ref_count(State, Id, Ver),
																	ok = kdb:remove(?MODULE, T, {SnapId, Id})
													end,
													List),
						{NewLastId, _} = hd(List),
						delete_snapshot_files(State, SnapId, NewLastId)
		end.

delete_snapshot_files(_I, _SnapId, _, List, 0) ->
		List;
delete_snapshot_files(I, SnapId, {ok, {SnapId, Id}, Ver}, List, Count) ->
		delete_snapshot_files(I, SnapId, kdb:next(?MODULE, I), [{Id, Ver} | List], Count - 1);
delete_snapshot_files(_I, _SnapId, _, List, _Count) ->
		List.

%%--------------------------------------------------------------------
%% translates name to ID
%%--------------------------------------------------------------------
name_to_id(#state{n2id = T}, Name) ->
		F = fun() ->
								Id =
										case kdb:lookup(?MODULE, T, Name) of
												{ok, V} ->
														V;
												not_found ->
														I =
																case kdb:lookup(?MODULE, T, free) of
																		not_found ->
																				0;
																		{ok, Free} ->
																				Free
																end,
														ok = kdb:insert(?MODULE, T, free, I + 1),
														ok = kdb:insert(?MODULE, T, Name, I),
														ok = kdb:insert(?MODULE, T, I, Name),
														I
										end,
								Id
				end,
		{atomic, Id} = kdb:transaction(?MODULE, F),
		Id.

%%--------------------------------------------------------------------
%% compare_file_info
%%--------------------------------------------------------------------
compare_file_info(First, Second) ->
		#file_info{size = FirstSize,
							 type = FirstType,
							 mtime = FirstMtime,
							 ctime = FirstCtime,
							 mode = FirstMode,
							 uid = FirstUid,
							 gid = FirstGid} = First,

		#file_info{size = SecondSize,
							 type = SecondType,
							 mtime = SecondMtime,
							 ctime = SecondCtime,
							 mode = SecondMode,
							 uid = SecondUid,
							 gid = SecondGid} = Second,

		if
				FirstSize =:= SecondSize andalso
				FirstType =:= SecondType andalso
				FirstMtime =:= SecondMtime andalso
				FirstCtime =:= SecondCtime andalso
				FirstMode =:= SecondMode andalso
				FirstUid =:= SecondUid andalso
				FirstGid =:= SecondGid ->
						equal;
				true ->
						not_equal
		end.

%%--------------------------------------------------------------------
%% Version info
%%--------------------------------------------------------------------
-record(ver_info, {status = error,							% file status
									 file_info,										% file_info
									 ref_count = 1								% version reference count
									}).

%%--------------------------------------------------------------------
%% Extracts file_info from ver_info
%%--------------------------------------------------------------------
ver_info_to_file_info(#ver_info{file_info = Fi}) ->
		Fi.

%%--------------------------------------------------------------------
%% Extracts file_info from ver_info
%%--------------------------------------------------------------------
ver_info_status(#ver_info{status = Status}) ->
		Status.

%%--------------------------------------------------------------------
%% creates new version info, version number grows in negative direction
%%--------------------------------------------------------------------
new_version(State, Id, Fi) ->
		#state{ver_info = T} = State,
		F = fun() ->
								{ok, It} = kdb:seek(?MODULE, T, {Id, -16#ffffffffffffffff}, next),
								NewVer =
										case kdb:next(?MODULE, It) of
												{ok, {Id, PrevVer}, _VerInfo} ->
														PrevVer - 1;
												_ ->
														0
										end,
								kdb:close_iter(?MODULE, It),
								VerInfo = #ver_info{file_info = Fi},
								ok = kdb:insert(?MODULE, T, {Id, NewVer}, VerInfo),
								NewVer
				end,
		{atomic, NewVer} = kdb:transaction(?MODULE, F),
		NewVer.

%%--------------------------------------------------------------------
%% returns last valid file info and corresponding version number
%%--------------------------------------------------------------------
last_file_info(State, Id) ->
		#state{ver_info = T} = State,
		F = fun() ->
								{ok, It} = kdb:seek(?MODULE, T, {Id, -16#ffffffffffffffff}, next),
								Res = last_file_info(It, Id, kdb:next(?MODULE, It)),
								kdb:close_iter(?MODULE, It),
								Res
				end,
		{atomic, Res} = kdb:transaction(?MODULE, F, read_only),
		Res.

last_file_info(_It, Id, {ok, {Id, Ver}, #ver_info{status = ok, file_info = Fi}}) ->
		{ok, Fi, Ver};
last_file_info(It, Id, {ok, {Id, _}, _}) ->
		last_file_info(It, Id, kdb:next(?MODULE, It));
last_file_info(_, _, _) ->
		not_found.

%%--------------------------------------------------------------------
%% returns true if file has single version
%%--------------------------------------------------------------------
has_single_version(State, Id) ->
		#state{ver_info = T} = State,
		F = fun() ->
								{ok, It} = kdb:seek(?MODULE, T, {Id, -16#ffffffffffffffff}, next),
								{ok, {Id, _}, _} = kdb:next(?MODULE, It),
								Res =
										case kdb:next(?MODULE, It) of
												{ok, {Id, _}, _} ->
														false;
												_ ->
														true
										end,
								kdb:close_iter(?MODULE, It),
								Res
				end,
		{atomic, Res} = kdb:transaction(?MODULE, F, read_only),
		Res.

%%--------------------------------------------------------------------
%% increments reference count of version info
%%--------------------------------------------------------------------
inc_version_ref_count(State, Id, Ver) ->
		#state{ver_info = T} = State,
		F = fun() ->
								{ok, VerInfo} = kdb:lookup(?MODULE, T, {Id, Ver}),
								#ver_info{ref_count = RefCount} = VerInfo,
								ok = kdb:insert(?MODULE, T, {Id, Ver}, VerInfo#ver_info{ref_count = RefCount + 1})
				end,
		{atomic, ok} = kdb:transaction(?MODULE, F),
		ok.

%%--------------------------------------------------------------------
%% marks file version as ok
%%--------------------------------------------------------------------
set_version_status(State, Id, Ver, Status) ->
		#state{ver_info = T, arc_file = ArcFile} = State,
		ok = file:sync(ArcFile),
		F = fun() ->
								{ok, VerInfo} = kdb:lookup(?MODULE, T, {Id, Ver}),
								ok = kdb:insert(?MODULE, T, {Id, Ver}, VerInfo#ver_info{status = Status})
				end,
		{atomic, ok} = kdb:transaction(?MODULE, F),
		ok.

%%--------------------------------------------------------------------
%% decrements reference count of version info
%%--------------------------------------------------------------------
dec_version_ref_count(State, Id, Ver) ->
		#state{ver_info = T, n2id = N} = State,
		F = fun() ->
								case kdb:lookup(?MODULE, T, {Id, Ver}) of
										not_found ->
												ok;
										{ok, VerInfo} ->
												#ver_info{ref_count = RefCount} = VerInfo,
												if
														RefCount > 1 ->
																ok = kdb:insert(?MODULE, T, {Id, Ver}, VerInfo#ver_info{ref_count = RefCount - 1});
														true ->
																ok = kdb:insert(?MODULE, T, {Id, Ver}, VerInfo#ver_info{status = error}),
																case has_single_version(State, Id) of
																		false ->
																				ok;
																		true ->
																				case kdb:lookup(?MODULE, N, Id) of
																						not_found ->
																								ok;
																						{ok, Name} ->
																								ok = kdb:remove(?MODULE, N, Name)
																				end,
																				ok = kdb:remove(?MODULE, N, Id)
																end,
																delete
												end
								end
				end,
		case kdb:transaction(?MODULE, F) of
				{atomic, ok} ->
						ok;
				{atomic, delete} ->
						ok = delete_file(State, {Id, Ver}),
						ok = kdb:remove(?MODULE, T, {Id, Ver})
		end.

%%---------------------------------------------------------------------
%% copies file to database
%% --------------------------------------------------------------------
copy_file(State, Src, FileName) ->
		case file:open(Src, [raw, read, binary, {read_ahead, 1024 * 1024}]) of
				{ok, File} ->
						Res = copy_file(State, File, FileName, 0, file:read(File, ?DEDUP_BLOCK_SIZE), true),
						file:close(File),
						Res;
				Error ->
						Error
		end.

copy_file(_State,  _File, _FileName, _N, eof, _DoCompress) ->
		ok;
copy_file(State, File, FileName, N, {ok, Block}, DoCompress00) ->
		{ok, DoCompress10} = write_file_block(State, FileName, N, Block, DoCompress00),
		copy_file(State, File, FileName, N + 1, file:read(File, ?DEDUP_BLOCK_SIZE), DoCompress10);
copy_file(_State, _File, _FileName, _N, Error, _DoCompress00) ->
		Error.

%%---------------------------------------------------------------------
%% writes file block
%% --------------------------------------------------------------------
write_file_block(State, FileName, N, Block, DoCompress00) ->
		#state{files_idx = FilesIdx} = State,
		F = fun() ->
								{ok, Offset, StoredSize, IsCompressed, DoCompress10} =
										dedup_block(State, N, Block, DoCompress00),
								ok = kdb:insert(?MODULE, FilesIdx, {FileName, N}, {Offset, StoredSize, IsCompressed}),
								{ok, DoCompress10}
				end,
		{atomic, Res} = kdb:transaction(?MODULE, F),
		Res.

%%---------------------------------------------------------------------
%% compresses file block
%% --------------------------------------------------------------------
compress_block(N, Block, DoCompress) when DoCompress =:= true orelse N rem 100 =:= 0 ->
		Compr = compress(Block),
		if
				size(Compr) =< 0.8 * size(Block) ->
						{ok, Compr, 1, true};
				true ->
						{ok, Block, 0, false}
		end;
compress_block(_N, Block, false) ->
		{ok, Block, 0, false}.

%%---------------------------------------------------------------------
%% deduplicates block, function must be called inside transaction
%% --------------------------------------------------------------------
dedup_block(State, N, Block00, DoCompress00) ->
		#state{dedup = Dedup, arc_file = ArcFile} = State,
		Crc = erlang:crc32(Block00),
 		BlockSize = size(Block00),
		{ok, I} = kdb:seek(?MODULE, Dedup, {Crc, BlockSize, -1}, next),
		Res =
				case dedup_block(State, I, Block00, Crc, BlockSize, kdb:next(?MODULE, I)) of
						{ok, Offset, StoredSize, IsCompressed} ->
								{ok, Offset, StoredSize, IsCompressed, DoCompress00};
						not_found ->
								{ok, Block10, IsCompressed, DoCompress10} = compress_block(N, Block00, DoCompress00),
								StoredSize = size(Block10),
								Offset = alloc(State, StoredSize),
								ok = file:pwrite(ArcFile, Offset, Block10),
								ok = kdb:insert(?MODULE, Dedup, {Crc, BlockSize, Offset}, {1, StoredSize, IsCompressed}),
								ok = kdb:insert(?MODULE, Dedup, Offset, {Crc, BlockSize}),
								{ok, Offset, StoredSize, IsCompressed, DoCompress10}
				end,
		ok = kdb:close_iter(?MODULE, I),
		Res.

dedup_block(State, I, Block, Crc, BlockSize, {ok, {Crc, BlockSize, Offset}, {RefCount, StoredSize, IsCompressed}}) ->
		#state{dedup = Dedup, arc_file = ArcFile} = State,
		{ok, OtherBlock00} = file:pread(ArcFile, Offset, StoredSize),
		OtherBlock10 =
				if
						IsCompressed =:= 0 ->
								OtherBlock00;
						true ->
								decompress(OtherBlock00)
				end,
		if
				OtherBlock10 =:= Block ->
						ok = kdb:insert(?MODULE, Dedup, {Crc, BlockSize, Offset}, {RefCount + 1, StoredSize, IsCompressed}),
						{ok, Offset, StoredSize, IsCompressed};
				true ->
						dedup_block(State, I, Block, Crc, BlockSize, kdb:next(?MODULE, I))
		end;
dedup_block(_State, _I, _Block, _Crc, _BlockSize, _) ->
		not_found.

%%---------------------------------------------------------------------
%% decrements reference count of block, function must be called within
%% transaction
%% --------------------------------------------------------------------
dec_block_ref_count(State, Offset) ->
		#state{dedup = Dedup} = State,
		{ok, {Crc, BlockSize}} = kdb:lookup(?MODULE, Dedup, Offset),
		{ok, {RefCount, StoredSize, IsCompressed}} = kdb:lookup(?MODULE, Dedup, {Crc, BlockSize, Offset}),
		if
				RefCount > 1 ->
						ok = kdb:insert(?MODULE, Dedup, {Crc, BlockSize, Offset}, {RefCount - 1, StoredSize, IsCompressed});
				true ->
						ok = free(State, Offset, StoredSize),
						ok = kdb:remove(?MODULE, Dedup, Offset),
						ok = kdb:remove(?MODULE, Dedup, {Crc, BlockSize, Offset})
		end.

%%---------------------------------------------------------------------
%% reads file block
%% --------------------------------------------------------------------
read_file_block(FilesIdx, ArcFile, FileName, N) ->
		F = fun() ->
								case kdb:lookup(?MODULE, FilesIdx, {FileName, N}) of
										not_found ->
												eof;
										{ok, {Offset, StoredSize, IsCompressed}} ->
												{ok, Data} = file:pread(ArcFile, Offset, StoredSize),
												{ok, Data, IsCompressed}
								end
				end,
		case kdb:transaction(?MODULE, F, read_only) of
				{atomic, eof} ->
						eof;
				{atomic, {ok, Data, IsCompressed}} ->
						if
								IsCompressed =:= 0 ->
										{ok, Data};
								true ->
										{ok, decompress(Data)}
						end
		end.

%%---------------------------------------------------------------------
%% deletes file
%% --------------------------------------------------------------------
delete_file(State, FileName) ->
		#state{files_idx = T} = State,
		F = fun() ->
								{ok, I} = kdb:seek(?MODULE, T, {FileName, -1}, next),
								{Res, Keys} = delete_file(State, I, FileName, kdb:next(?MODULE, I), 8, []),
								ok = kdb:close_iter(?MODULE, I),
								lists:foreach(fun(K) ->
																			ok = kdb:remove(?MODULE, T, K)
															end,
															Keys),
								Res
				end,
		case kdb:transaction(?MODULE, F) of
				{atomic, eof} ->
						ok;
				{atomic, ok} ->
						delete_file(State, FileName)
		end.

delete_file(_State, _I, _FileName, _, 0, Keys) ->
		{ok, Keys};
delete_file(State, I, FileName, {ok, {FileName, _N} = K, {Offset, _StoredSize, _IsCompressed}}, Count, Keys) ->
		ok = dec_block_ref_count(State, Offset),
		delete_file(State, I, FileName, kdb:next(?MODULE, I), Count - 1, [K | Keys]);
delete_file(_State, _I, _FileName, _, _Count, Keys) ->
		{eof, Keys}.

%%---------------------------------------------------------------------
%% allocates storage, function must be called inside transaction
%% --------------------------------------------------------------------
alloc(State, Amount00) when Amount00 > 0 ->
		#state{stm = Stm} = State,
		Amount10 =
				if
						Amount00 rem 512 =:= 0 ->
								Amount00;
						true ->
								Amount00 + 512 - Amount00 rem 512
				end,
		{ok, I} = kdb:seek(?MODULE, Stm, {Amount10, 0}, exact_next),
		{ok, {Length00, Start00}, _} = kdb:next(?MODULE, I),
		ok = kdb:remove(?MODULE, Stm, Start00),
		ok = kdb:remove(?MODULE, Stm, {Length00, Start00}),
		Length10 = Length00 - Amount10,
		if
				Length10 > 0 ->
						Start10 = Start00 + Amount10,
						ok = kdb:insert(?MODULE, Stm, {Length10, Start10}, []),
						ok = kdb:insert(?MODULE, Stm, Start10, Length10);
				true ->
						ok
		end,
		ok = kdb:close_iter(?MODULE, I),
		Start00.

%%---------------------------------------------------------------------
%% frees storage, function must be called inside transaction
%% --------------------------------------------------------------------
free(State, Start00, Amount) ->
		#state{stm = Stm} = State,
		Length00 =
				if
						Amount rem 512 =:= 0 ->
								Amount;
						true ->
								Amount + 512 - Amount rem 512
				end,
		%% try to merge with previous interval
		{ok, PrevI} = kdb:seek(?MODULE, Stm, Start00, prev),
		{Start10, Length10} =
				case kdb:next(?MODULE, PrevI) of
						{ok, Start05, Length05} when Start05 + Length05 =:= Start00 ->
								ok = kdb:remove(?MODULE, Stm, Start05),
								ok = kdb:remove(?MODULE, Stm, {Length05, Start05}),
								{Start05, Length05 + Length00};
						_ ->
								{Start00, Length00}
				end,
		ok = kdb:close_iter(?MODULE, PrevI),
		%% try to merge with next interval
		{ok, NextI} = kdb:seek(?MODULE, Stm, Start10, next),
		{Start20, Length20} =
				case kdb:next(?MODULE, NextI) of
						{ok, Start15, Length15} when Start10 + Length10 =:= Start15 ->
								ok = kdb:remove(?MODULE, Stm, Start15),
								ok = kdb:remove(?MODULE, Stm, {Length15, Start15}),
								{Start10, Length10 + Length15};
						_ ->
								{Start10, Length10}
				end,
		ok = kdb:close_iter(?MODULE, NextI),
		%% insert resulting interval
		ok = kdb:insert(?MODULE, Stm, {Length20, Start20}, []),
		ok = kdb:insert(?MODULE, Stm, Start20, Length20).

%%---------------------------------------------------------------------
%% compresses block of binary data
%% --------------------------------------------------------------------
compress(Data) ->
 		Z = zlib:open(),
 		ok = zlib:deflateInit(Z, 1, deflated, -15, 9, default),
		Compressed = zlib:deflate(Z, Data, finish),
 		ok = zlib:close(Z),
		iolist_to_binary(Compressed).

%%---------------------------------------------------------------------
%% decompresses block data, previosly compressed with compress function
%% --------------------------------------------------------------------
decompress(Compressed) ->
 		Z = zlib:open(),
 		ok = zlib:inflateInit(Z, -15),
 		Data = zlib:inflate(Z, Compressed),
 		ok = zlib:close(Z),
 		iolist_to_binary(Data).

%%---------------------------------------------------------------------
%% Deletes redundant snapshots
%% --------------------------------------------------------------------
clear_history(State) ->
		{ok, Snapshots} = eb_pers_conf:get_value(?MODULE, snapshots),
		clear_history(State, Snapshots).

clear_history(_State, "unlimited") ->
		ok;
clear_history(State, Snapshots) ->
		#state{snapshots = T} = State,
		Pid = self(),
		F = fun() ->
								%% Get last snapshot ID
								{ok, I} = kdb:seek(?MODULE, T, {snap, -16#ffffffffffffffff}, next),
								{ok, {snap, LastId}, _} = kdb:next(?MODULE, I),
								ok = kdb:close_iter(?MODULE, I),
								Prev = LastId + Snapshots - 1,
								{ok, J} = kdb:seek(?MODULE, T, {snap, Prev}, next),
								Res = clear_history(T, J, Pid, kdb:next(?MODULE, J)),
								ok = kdb:close_iter(?MODULE, J),
								Res
				end,
		case kdb:transaction(?MODULE, F) of
				{atomic, eof} ->
						ok;
				{atomic, Id} ->
						ok = delete_snapshot(State, Id),
						clear_history(State, Snapshots)
		end.

clear_history(T, I, MyPid, {ok, {snap, Id}, #snap_descr{status = {_, OtherPid}} = SnapDescr}) ->
		case is_process_alive(OtherPid) of
				true ->
						clear_history(T, I, MyPid, kdb:next(?MODULE, I));
				false ->
						ok = kdb:insert(?MODULE, T, {snap, Id}, SnapDescr#snap_descr{status = {delete, MyPid}}),
						Id
		end;
clear_history(T, _I, MyPid, {ok, {snap, Id}, SnapDescr}) ->
		ok = kdb:insert(?MODULE, T, {snap, Id}, SnapDescr#snap_descr{status = {delete, MyPid}}),
		Id;
clear_history(_T, _I, _MyPid, _) ->
		eof.

%%---------------------------------------------------------------------
%% Restore, pass 1 - restores files and symlinks
%% --------------------------------------------------------------------
restore_pass1(State, Prefix) ->
		restore_pass1(State, Prefix, -1).

restore_pass1(State, Prefix, PrevId) ->
		#state{snap_id = SnapId, snapshots = Snapshots, ver_info = ViTbl,
					 n2id = N2Id, restore_tmp = RestoreTmp} = State,
		F = fun() ->
								{ok, I} = kdb:seek(?MODULE, Snapshots, {SnapId, PrevId}, next),
								Res = restore_pass1(I, SnapId, kdb:next(?MODULE, I), [], 1024),
								ok = kdb:close_iter(?MODULE, I),
								Res
				end,
		RestoreObject =
				fun({Id, Ver} = FileName) ->
								{ok, VerInfo} = kdb:lookup(?MODULE, ViTbl, {Id, Ver}),
								#ver_info{status = Status, file_info = FileInfo} = VerInfo,
								if
										Status =:= ok ->
												{ok, Name} = kdb:lookup(?MODULE, N2Id, Id),
												RestName = make_restore_name(Prefix, Name),
												case fs_object_exist(RestName) of
														false ->
																case FileInfo#file_info.type of
																		symlink ->
																				%% Just create symlink, do not write file info, because, erlang
																				%% uses link derefencing functions.
																				ok = restore_symlink(State, FileName, RestName);
																		directory ->
																				%% create directory
																				ok = eklib:mkdir_ensure(RestName),
																				%% save directory for second pass
																				Depth = -length(filename:split(RestName)),
																				ok = kdb:insert(?MODULE, RestoreTmp, {Depth, RestName}, FileInfo);
																		regular ->
																				ok = restore_file(State, FileName, RestName, FileInfo)
																end;
														true ->
																ok
												end;
										true ->
												ok
								end
				end,
		case kdb:transaction(?MODULE, F, read_only) of
				{atomic, {eof, List}} ->
						lists:foreach(fun(X) ->
																	catch RestoreObject(X)
													end,
													lists:reverse(List)),
						ok;
				{atomic, {ok, List}} ->
						{LastId, _} = hd(List),
						lists:foreach(fun(X) ->
																	catch RestoreObject(X)
													end,
													lists:reverse(List)),
						restore_pass1(State, Prefix, LastId)
		end.

restore_pass1(_I, _SnapId, _, List, 0) ->
		{ok, List};
restore_pass1(I, SnapId, {ok, {SnapId, Id}, Ver}, List, Count) ->
		restore_pass1(I, SnapId, kdb:next(?MODULE, I), [{Id, Ver} | List], Count - 1);
restore_pass1(_I, _SnapId, _, List, _Count) ->
		{eof, List}.

%%---------------------------------------------------------------------
%% Restore pass 2 - restores directories and their permissions
%% --------------------------------------------------------------------
restore_pass2(State) ->
		restore_pass2(State, {-16#ffffffffffffffff, []}).

restore_pass2(State, PrevKey) ->
		#state{restore_tmp = RestoreTmp} = State,
		F = fun() ->
								{ok, I} = kdb:seek(?MODULE, RestoreTmp, PrevKey, next),
								Res = restore_pass2(I, kdb:next(?MODULE, I), [], 1024),
								ok = kdb:close_iter(?MODULE, I),
								Res
				end,
		RestoreObject =
				fun({{_Depth, RestName}, FileInfo}) ->
								file:write_file_info(RestName, FileInfo)
				end,
		case kdb:transaction(?MODULE, F, read_only) of
				{atomic, {eof, List}} ->
						lists:foreach(RestoreObject, lists:reverse(List)),
						ok;
				{atomic, {ok, List}} ->
						{LastKey, _} = hd(List),
						lists:foreach(RestoreObject, lists:reverse(List)),
						restore_pass2(State, LastKey)
		end.

restore_pass2(_I, _, List, 0) ->
		{ok, List};
restore_pass2(I, {ok, K, V}, List, Count) ->
		restore_pass2(I, kdb:next(?MODULE, I), [{K, V} | List], Count - 1);
restore_pass2(_I, _, List, _Count) ->
		{eof, List}.

%%---------------------------------------------------------------------
%% buils restore path
%% --------------------------------------------------------------------
make_restore_name("", Name) ->
		Name;
make_restore_name(Prefix, Name) ->
		make_restore_name(Prefix, Name, os:type()).

make_restore_name(Prefix, Name, {win32, _}) ->
		case Name of
				[Drive, $: | T] ->
						lists:flatten([Prefix, $\\, Drive, $_, T]);
				[$\\, $\\ | T] ->
						lists:flatten([Prefix, $\\, $_, T])
		end;
make_restore_name(Prefix, Name, {unix, _}) ->
		lists:flatten([Prefix, Name]).

%%---------------------------------------------------------------------
%% restores symlink
%% --------------------------------------------------------------------
restore_symlink(State, FileName, RestName) ->
		#state{files_idx = FilesIdx, arc_file = ArcFile} = State,
		{ok, LinkTarget} = read_file_block(FilesIdx, ArcFile, FileName, 0),
		ok = filelib:ensure_dir(RestName),
		case file:make_symlink(binary_to_list(LinkTarget), RestName) of
				ok ->
						ok;
				{error, enoent} ->
						ok
		end.

%%---------------------------------------------------------------------
%% restores file
%% --------------------------------------------------------------------
restore_file(State, FileName, RestName, FileInfo) ->
		#state{files_idx = FilesIdx, arc_file = ArcFile} = State,
		ok = filelib:ensure_dir(RestName),
		{ok, File} = file:open(RestName, [raw, write, binary]),
		ok = restore_file(FilesIdx, ArcFile, FileName, 0, File),
		ok = file:close(File),
		ok = file:write_file_info(RestName, FileInfo).

restore_file(FilesIdx, ArcFile, FileName, N, File) ->
		case read_file_block(FilesIdx, ArcFile, FileName, N) of
				eof ->
						ok;
				{ok, Data} ->
						ok = file:write(File, Data),
						restore_file(FilesIdx, ArcFile, FileName, N + 1, File)
		end.

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
%% DB cleaner functions
%%---------------------------------------------------------------------------
start_db_cleaner() ->
		Pid = spawn_link(fun() ->
														 register(eb_pers_bckp_db_cleaner, self()),
														 process_flag(trap_exit, true),
														 link(whereis(?MODULE)),
														 cleaner_loop(0, false)
										 end),
		{ok, Pid}.

cleaner_loop(Jobs, MustRestart) ->
		receive
				{'DOWN', _Ref, _Type, _Object, _Info} ->
						if
								Jobs =< 1 ->
										if MustRestart -> kdb:stop(?MODULE);
											 true -> ok
										end,
										cleaner_loop(0, false);
								true ->
										cleaner_loop(Jobs - 1, MustRestart)
						end;
				{job_started, Pid, Ref, RestartOnFinish} ->
						MonitorRef = erlang:monitor(process, Pid),
						Pid ! {Ref, MonitorRef},
						cleaner_loop(Jobs + 1, MustRestart orelse RestartOnFinish);
				{job_finished, Ref} ->
						erlang:demonitor(Ref, [flush]),
						if
								Jobs =< 1 ->
										if MustRestart -> kdb:stop(?MODULE);
											 true -> ok
										end,
										cleaner_loop(0, false);
								true ->
										cleaner_loop(Jobs - 1, MustRestart)
						end;
				{'EXIT', _, _} ->
						%% linked process terminated
						ok;
				_ ->
						cleaner_loop(Jobs, MustRestart)
		end.

notify_cleaner_start_job(RestartOnFinish) ->
		Ref = make_ref(),
		eb_pers_bckp_db_cleaner ! {job_started, self(), Ref, RestartOnFinish},
		receive
				{Ref, MonitorRef} ->
						MonitorRef
		after
				5000 ->
						exit(timeout)
		end.

notify_cleaner_end_job(Ref) ->
		eb_pers_bckp_db_cleaner ! {job_finished, Ref},
		ok.

%%---------------------------------------------------------------------------
%% returns storage size, used by backups
%%---------------------------------------------------------------------------
storage_used(Destination) ->
		{ok,
		 filelib:file_size(filename:join(Destination, ?KDB_FILE)) +
		 filelib:file_size(filename:join(Destination, ?ARC_FILE))}.
