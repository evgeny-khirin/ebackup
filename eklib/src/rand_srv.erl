%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : rand_srv.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Random numbers generator server.
%%%
%%% Created : 30 Jun 2010 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(rand_srv).

-behaviour(gen_server).

-include("eklib/include/debug.hrl").

%% API
-export([start_link/0, start_link/1, uniform/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
				 terminate/2, code_change/3]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts unnamed server
%%--------------------------------------------------------------------
start_link() ->
		gen_server:start_link(?MODULE, [], []).

%%--------------------------------------------------------------------
%% Function: start_link(Name) -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts named server
%%    Name - {local, atom()} | {global, atom()}. See gen_server:start_link for
%%       details.
%%--------------------------------------------------------------------
start_link(Name) ->
		gen_server:start_link(Name, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Function: uniform(Pid, N) -> {ok, N} | {error,Error}
%% Description: Same as random:uniform(N).
%%--------------------------------------------------------------------
uniform(Pid, N) ->
		gen_server:call(Pid, {uniform, N}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
		process_flag(trap_exit, true),
		random:seed(now()),
		{ok, []}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({uniform, N}, _From, State) ->
		{reply, random:uniform(N), State};

handle_call(Request, From, State) ->
		?WARN("unexpected call ~p from ~p", [Request, From]),
		Reply = {error, unexpected_call},
		{reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
		?WARN("unexpected cast ~p", [Msg]),
		{noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(Info, State) ->
		?WARN("unexpected info ~p", [Info]),
		{noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
		ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(OldVsn, State, Extra) ->
		?WARN("unexpected code_change: old vsn ~p, extra ~p", [OldVsn, Extra]),
		{ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
