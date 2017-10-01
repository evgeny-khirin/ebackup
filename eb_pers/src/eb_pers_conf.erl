%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eb_pers_conf.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Configuration database supervisor. KDB server
%%% registered with ?MODULE name.
%%%
%%% Created : 14 Dec 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(eb_pers_conf).

-include("eklib/include/debug.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0,
				 get_value/2,
				 get_value/3,
				 set_value/3,
				 delete/2
				]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
		supervisor:start_link(?MODULE, []).

%%--------------------------------------------------------------------
%% Function: get_value(Table, Key) -> {ok,Value} | not_found | {error,Error}
%% Description: Returns configuration value.
%%    Table = term.
%%    Key = term.
%%--------------------------------------------------------------------
get_value(Table, Key) ->
		{ok, T} = kdb:open_table(?MODULE, Table),
		Res = kdb:lookup(?MODULE, T, Key),
		kdb:close_table(?MODULE, T),
		Res.

%%--------------------------------------------------------------------
%% Function: get_value(Table, Key, Default) -> {ok,Value} | {error,Error}
%% Description: Returns configuration value.
%%    Table = term.
%%    Key = term.
%%    Default = term. Default value, returned if key is not found.
%%--------------------------------------------------------------------
get_value(Table, Key, Default) ->
		F = fun() ->
								{ok, T} = kdb:open_table(?MODULE, Table),
								Res =
										case kdb:lookup(?MODULE, T, Key) of
												not_found ->
														ok = kdb:insert(?MODULE, T, Key, Default),
														{ok, Default};
												{ok, Value} ->
														{ok, Value}
										end,
								kdb:close_table(?MODULE, T),
								Res
				end,
		{atomic, Result} = kdb:transaction(?MODULE, F, hard),
		Result.


%%--------------------------------------------------------------------
%% Function: set_value(Table, Key, Value) -> ok | {error,Error}
%% Description: Sets configuration value.
%%    Table = term.
%%    Key = term.
%%    Value = term.
%%--------------------------------------------------------------------
set_value(Table, Key, Value) ->
		F = fun() ->
								{ok, T} = kdb:open_table(?MODULE, Table),
								ok = kdb:insert(?MODULE, T, Key, Value),
								kdb:close_table(?MODULE, T)
				end,
		{atomic, ok} = kdb:transaction(?MODULE, F, hard),
		ok.

%%--------------------------------------------------------------------
%% Function: delete(Table, Key) -> ok | {error,Error}
%% Description: Deletes configuration key.
%%--------------------------------------------------------------------
delete(Table, Key) ->
		F = fun() ->
								{ok, T} = kdb:open_table(?MODULE, Table),
								ok = kdb:remove(?MODULE, T, Key),
								kdb:close_table(?MODULE, T)
				end,
		{atomic, ok} = kdb:transaction(?MODULE, F, hard),
		ok.

%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using
%% supervisor:start_link/[2,3], this function is called by the new process
%% to find out about restart strategy, maximum restart frequency and child
%% specifications.
%%--------------------------------------------------------------------
init([]) ->
		%% KDB
		Kdb = {kdb,
					 {kdb, start_link, [{local, ?MODULE}]},
					 permanent, 30000, worker, [kdb]},

		%% Start DB
		StartDb =
				{start_db,
				 {erlang, apply, [fun() ->
																	start_db(),
																	%% make supervisor happy with PID
																	{ok, spawn_link(fun() -> ok end)}
													end, []]},
				 transient, brutal_kill, worker, [?MODULE]},

		{ok, {{one_for_all, 100, 1}, [Kdb, StartDb]}}.

%%--------------------------------------------------------------------
%% starts KDB performs recovery
%%--------------------------------------------------------------------
start_db() ->
		%% Make sure that DB directory exists
		DbDir = get_db_dir(),
		ok = eklib:mkdir_ensure(DbDir),

		%% Set DB properties
		FileName = filename:join(DbDir, "conf.dat"),
		BlockSize = (1024 * 16),										 % 16K
		Capacity = 10 * 1024 * 1024 div BlockSize,	 % 10m
		LogCapacity = 1 * 1024 * 1024 div BlockSize, % 1M
		CpLogSize = LogCapacity div 2 * BlockSize,	 % 512K

		%% Common file
		{ok, _} = kdb:start_object(?MODULE, bd_file,
															 [{name, eb_pers_conf_file},
																{block_size, BlockSize},
																{capacity, Capacity},
																{file, FileName}]),
		%% Log partition
		{ok, _} = kdb:start_object(?MODULE, bd_part,
															 [{name, eb_pers_conf_log_part},
																{block_device, eb_pers_conf_file},
																{start, 0},
																{capacity, LogCapacity}]),
		%% Log CRC
		{ok, LogCrc} = kdb:start_object(?MODULE, bd_crc,
																		[{name, eb_pers_conf_log_crc},
																		 {block_device, eb_pers_conf_log_part}]),
		%% Format
		case kdb:ld_bd_is_formatted(?MODULE, LogCrc, ?MODULE) of
				true ->
						ok;
				false ->
						ok = kdb:ld_bd_format(?MODULE, LogCrc, ?MODULE)
		end,
		%% LD BD
		{ok, LdBd} = kdb:start_object(?MODULE, ld_bd,
																	[{name, eb_pers_conf_ld_bd},
																	 {block_device, eb_pers_conf_log_crc}]),
		%% LD PP
		{ok, _} = kdb:start_object(?MODULE, ld_pp,
															 [{name, eb_pers_conf_ld_pp},
																{log_device, eb_pers_conf_ld_bd}]),
		%% Logger
		{ok, _} = kdb:start_object(?MODULE, blogger,
															 [{name, eb_pers_conf_logger},
																{log_device, eb_pers_conf_ld_pp}]),
		%% TM
		{ok, _} = kdb:start_object(?MODULE, trans_mgr,
															 [{name, eb_pers_conf_tm},
																{logger, eb_pers_conf_logger},
																{buffer_manager, eb_pers_conf_bm},
																{cp_log_size, CpLogSize}]),
		%% Data partition
		{ok, _} = kdb:start_object(?MODULE, bd_part,
															 [{name, eb_pers_conf_data_part},
																{block_device, eb_pers_conf_file},
																{start, LogCapacity},
																{capacity, Capacity - LogCapacity}]),
		%% Data CRC
		{ok, SysGen} = kdb:ld_bd_system_generation(?MODULE, LdBd),
		{ok, _} = kdb:start_object(?MODULE, bd_crc,
															 [{name, eb_pers_conf_data_crc},
																{block_device, eb_pers_conf_data_part},
																{trailer, SysGen}]),
		%% BM
		{ok, _} = kdb:start_object(?MODULE, buffer_mgr,
															 [{name, eb_pers_conf_bm},
																{block_device, eb_pers_conf_data_crc},
																{transaction_manager, eb_pers_conf_tm},
																{cache_capacity, 8}]),
		%% STM
		{ok, _} = kdb:start_object(?MODULE, stm,
															 [{name, eb_pers_conf_stm},
																{transaction_manager, eb_pers_conf_tm},
																{buffer_manager, eb_pers_conf_bm}]),
		%% KDB
		{ok, _} = kdb:start_object(?MODULE, kdb,
															 [{name, eb_pers_conf_kdb},
																{transaction_manager, eb_pers_conf_tm},
																{buffer_manager, eb_pers_conf_bm},
																{storage_manager, eb_pers_conf_stm}]),
		%% Perform recovery
		ok = kdb:recover(?MODULE),

		%% Set stop list
		ok = kdb:set_stop_list(?MODULE, [eb_pers_conf_kdb]).

%%====================================================================
%% Internal functions
%%====================================================================
get_db_dir() ->
		get_db_dir(os:type()).

get_db_dir({unix, linux}) ->
		case os:getenv("HOME") of
				false ->
						exit({error, home_dir_env});
				Home ->
						filename:join(Home, ".eb_pers")
		end;
get_db_dir({win32, _}) ->
		code:root_dir().
