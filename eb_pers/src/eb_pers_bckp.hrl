%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eb_pers_bckp.hrl
%%% Author  : Evgeny Khirin <>
%%% Description : Public backup records.
%%%
%%% Created : 24 Jul 2010 by Evgeny Khirin <>
%%%-------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Snapshot descriptor
%%--------------------------------------------------------------------
-record(snap_descr, {date = date(),					 % date when snapshot is started
										 time = time(),					 % time when snapshot is started
										 status,								 % snapshot status: ok | error |
																						 % {run, Pid} |
																						 % {{restore, SavedStatus}, Pid} |
																						 % {delete, Pid}
										 dirs = 0,							 % number of directories in snapshot
										 files = 0,							 % number of files in snapshot
										 size = 0,							 % size of snapshot
										 errors = 0							 % number of errors in snapshot
										}).
