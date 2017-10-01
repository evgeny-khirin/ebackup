%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eb_pers_sup.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Erlios backup supervisor.
%%%
%%% Created : 14 Dec 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(eb_pers_sup).

-behaviour(supervisor).

-include("eklib/include/debug.hrl").

%% API
-export([start_link/0, stop/0]).

%% Supervisor callbacks
-export([init/1, start_backup_db/0]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts application supervisor
%%--------------------------------------------------------------------
start_link() ->
		supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Function: stop() -> ok | {error,Error}
%% Description: Stops the supervisor
%%--------------------------------------------------------------------
stop() ->
		case whereis(?SERVER) of
				undefined ->
						ok;
				Pid ->
						exit(Pid, normal)
		end,
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
%% start basic services first
init([]) ->
		%% configuration DB
		Conf = {eb_pers_conf,
						{eb_pers_conf, start_link, []},
						permanent, infinity, supervisor, [eb_pers_conf]},

		%% Web UI http(s) server
		Inets = {eb_pers_inets,
						 {eb_pers_inets, start_link, []},
						 permanent, 2000, worker, [eb_pers_inets]},

		%% crontab
		Cron = {eb_pers_cron,
						{eb_pers_cron, start_link, []},
						permanent, infinity, supervisor, [eb_pers_cron]},

		%% Backup DB server
		Db = {eb_pers_bckp_db,
					{erlang, apply, [fun() ->
																	 start_backup_db()
													 end, []]},
					permanent, 30000, worker, [kdb]},

		%% Backup DB cache cleaner (restarts server after backup and restore)
		DbCleaner = {eb_pers_bckp_db_cleaner,
								 {eb_pers_bckp, start_db_cleaner, []},
								 permanent, 1000, worker, [eb_pers_bckp]},

 		{ok, {{one_for_one, 100, 1}, [Conf, Inets, Cron, Db, DbCleaner]}}.

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% starts backup DB server
%%--------------------------------------------------------------------
start_backup_db() ->
		{ok, Pid} = kdb:start_link({local, eb_pers_bckp}),
		eb_pers_bckp:restart_db(),
		{ok, Pid}.

