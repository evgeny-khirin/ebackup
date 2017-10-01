%%%-------------------------------------------------------------------
%%% File    : kdb_test.erl
%%% Author  : Evgeny Khirin <>
%%% Description : KDB sanity test.
%%%
%%% Created : 23 May 2010 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(kdb_test).

-include("eklib/include/debug.hrl").

%% API
-export([run/0
				]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: run() -> ok
%% Description: Executes test.
%%--------------------------------------------------------------------
run() ->
		%% Start KDB server
		{ok, Kdb} = kdb:start_link(),
		ok = start_db(Kdb),
		{ok, Used} = kdb:used(Kdb),
		?TRACE("used: ~p blocks", [Used]),
		{ok, Tbl} = kdb:open_table(Kdb, ?MODULE),
		%% run tests
		ok = test_empty(Kdb, Tbl),
		ok = test_insert(Kdb, Tbl),
		ok = test_lookup(Kdb, Tbl),
		ok = test_scan(Kdb, Tbl),
		ok = test_seek(Kdb, Tbl),
		ok = test_remove(Kdb, Tbl),
		ok = test_empty(Kdb, Tbl),
		ok = test_insert(Kdb, Tbl),
 		ok = kdb:drop_table(Kdb, ?MODULE),
		ok = test_scan(Kdb, Tbl),
		%% Stop KDB server
		ok = kdb:close_table(Kdb, Tbl),
		ok = kdb:stop(Kdb).

%%====================================================================
%% internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% starts DB
%%--------------------------------------------------------------------
start_db(Kdb) ->
		%% Start devices
		{ok, _} = kdb:start_object(Kdb, bd_file,
															 [{name, log_file},
																{block_size, 4096},
																{capacity, 2 * 1024 * 1024 div 4096},
																{file, "/var/ramdisk/kdb_test.log"}]),
		{ok, _} = kdb:start_object(Kdb, bd_crc,
															 [{name, log_crc},
																{block_device, log_file}]),
		{ok, LogDwrite} = kdb:start_object(Kdb, bd_dwrite,
																			 [{name, log_dwrite},
																				{block_device, log_crc},
																				{buffer_capacity, 8}]),
		case kdb:ld_bd_is_formatted(Kdb, LogDwrite, ?MODULE) of
				true ->
						ok;
				false ->
						ok = kdb:ld_bd_format(Kdb, LogDwrite, ?MODULE)
		end,
		{ok, LdBd} = kdb:start_object(Kdb, ld_bd,
																	[{name, ld_bd},
																	 {block_device, log_dwrite}]),
		{ok, _} = kdb:start_object(Kdb, ld_pp,
															 [{name, ld_pp},
																{log_device, ld_bd}]),
		{ok, _} = kdb:start_object(Kdb, blogger,
															 [{name, logger},
																{log_device, ld_pp}]),
		{ok, _} = kdb:start_object(Kdb, trans_mgr,
															 [{name, tm},
																{logger, logger},
																{buffer_manager, bm},
																{cp_log_size, 512 * 1024}]),
		{ok, _} = kdb:start_object(Kdb, bd_file,
															 [{name, dat_file},
																{block_size, 4096},
																{file, "/var/ramdisk/kdb_test.dat"}]),
		{ok, SysGen} = kdb:ld_bd_system_generation(Kdb, LdBd),
		{ok, _} = kdb:start_object(Kdb, bd_crc,
															 [{name, dat_crc},
																{block_device, dat_file},
																{trailer, SysGen}]),
		{ok, _} = kdb:start_object(Kdb, bd_dwrite,
															 [{name, dat_dwrite},
																{block_device, dat_crc},
																{buffer_capacity, 8}]),
		{ok, _} = kdb:start_object(Kdb, buffer_mgr,
															 [{name, bm},
																{block_device, dat_dwrite},
																{transaction_manager, tm},
																{cache_capacity, 32}]),
		{ok, _} = kdb:start_object(Kdb, stm,
															 [{name, stm},
																{transaction_manager, tm},
																{buffer_manager, bm}]),
		{ok, _} = kdb:start_object(Kdb, kdb,
															 [{name, kdb},
																{transaction_manager, tm},
																{buffer_manager, bm},
																{storage_manager, stm}]),
		%% Perform recovery
		ok = kdb:recover(Kdb).

%%--------------------------------------------------------------------
%% waits untill all processes are terminated
%%--------------------------------------------------------------------
wait([H|T]) ->
		wait(H),
		wait(T);
wait([]) ->
		ok;
wait(Pid) ->
		wait(Pid, is_process_alive(Pid)).

wait(_Pid, false) ->
		ok;
wait(Pid, true) ->
		erlang:yield(),
		wait(Pid, is_process_alive(Pid)).

%%--------------------------------------------------------------------
%% tests that table is empty
%%--------------------------------------------------------------------
test_empty(Kdb, Tbl) ->
		F = fun() ->
								{ok, It} = kdb:iterator(Kdb, Tbl),
								eof = kdb:next(Kdb, It),
								ok = kdb:close_iter(Kdb, It)
				end,
		{atomic, ok} = kdb:transaction(Kdb, F, read_only),
		ok.

%%--------------------------------------------------------------------
%% performs insert test
%%--------------------------------------------------------------------
test_insert(Kdb, Tbl) ->
		P1 = spawn_link(fun() ->
														do_insert(Kdb, Tbl, 1, 50001)
										end),
		P2 = spawn_link(fun() ->
														do_insert(Kdb, Tbl, 50001, 100001)
										end),
		wait([P1, P2]),
		ok.

do_insert(_Kdb, _Tbl, Count, Count) ->
		ok;
do_insert(Kdb, Tbl, N, Count) ->
		F = fun() ->
								{atomic, ok} = kdb:transaction(Kdb,
																							 fun() ->
																											 kdb:insert(Kdb, Tbl, N, -N)
																							 end),
								ok
				end,
		{atomic, ok} = kdb:transaction(Kdb, F),
		do_insert(Kdb, Tbl, N + 1, Count).

%%--------------------------------------------------------------------
%% performs lookup test
%%--------------------------------------------------------------------
test_lookup(Kdb, Tbl) ->
		P1 = spawn_link(fun() ->
														do_lookup(Kdb, Tbl, 1, 50001)
										end),
		P2 = spawn_link(fun() ->
														do_lookup(Kdb, Tbl, 50001, 100001)
										end),
		wait([P1, P2]),
		ok.

do_lookup(_Kdb, _Tbl, Count, Count) ->
		ok;
do_lookup(Kdb, Tbl, N, Count) ->
		F = fun() ->
								{ok, V} = kdb:lookup(Kdb, Tbl, N),
								V = -N,
								ok
				end,
		{atomic, ok} = kdb:transaction(Kdb, F, read_only),
		do_lookup(Kdb, Tbl, N + 1, Count).

%%--------------------------------------------------------------------
%% performs iterator test
%%--------------------------------------------------------------------
test_scan(Kdb, Tbl) ->
		F = fun() ->
								{ok, It} = kdb:iterator(Kdb, Tbl),
								ok = do_scan(Kdb, It, kdb:next(Kdb, It), 1, 100001)
				end,
		{atomic, ok} = kdb:transaction(Kdb, F, read_only),
		ok.

do_scan(Kdb, It, eof, Count, Count) ->
		ok = kdb:close_iter(Kdb, It);
do_scan(Kdb, It, {ok, N, V}, N, Count) when V =:= -N ->
		do_scan(Kdb, It, kdb:next(Kdb, It), N + 1, Count).

%%--------------------------------------------------------------------
%% performs iterator seek test
%%--------------------------------------------------------------------
test_seek(Kdb, Tbl) ->
		F = fun() ->
								{ok, It00} = kdb:seek(Kdb, Tbl, 1, prev),
								ok = do_scan(Kdb, It00, kdb:next(Kdb, It00), 1, 1),
								{ok, It10} = kdb:seek(Kdb, Tbl, 100000, next),
								ok = do_scan(Kdb, It10, kdb:next(Kdb, It10), 100000, 100000),
								{ok, It20} = kdb:seek(Kdb, Tbl, 100000, exact),
								ok = do_scan(Kdb, It20, kdb:next(Kdb, It20), 100000, 100001),
								{ok, It30} = kdb:seek(Kdb, Tbl, 100001, exact_prev),
								ok = do_scan(Kdb, It30, kdb:next(Kdb, It30), 100000, 100001),
								{ok, It40} = kdb:seek(Kdb, Tbl, 0, exact_next),
								ok = do_scan(Kdb, It40, kdb:next(Kdb, It40), 1, 100001)
				end,
		{atomic, ok} = kdb:transaction(Kdb, F, read_only),
		ok.

%%--------------------------------------------------------------------
%% performs remove test
%%--------------------------------------------------------------------
test_remove(Kdb, Tbl) ->
		P1 = spawn_link(fun() ->
														do_remove(Kdb, Tbl, 1, 50001)
										end),
		P2 = spawn_link(fun() ->
														do_remove(Kdb, Tbl, 50001, 100001)
										end),
		wait([P1, P2]),
		ok.

do_remove(_Kdb, _Tbl, Count, Count) ->
		ok;
do_remove(Kdb, Tbl, N, Count) ->
		F = fun() ->
								{atomic, ok} = kdb:transaction(Kdb,
																							 fun() ->
																											 kdb:remove(Kdb, Tbl, N)
																							 end),
								ok
				end,
		{atomic, ok} = kdb:transaction(Kdb, F),
		do_remove(Kdb, Tbl, N + 1, Count).
