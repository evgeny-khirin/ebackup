%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eb_pers.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Erlios Backup Personal Edition application.
%%%
%%% Created : 22 Dec 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(eb_pers).

-behaviour(application).

-include("eklib/include/debug.hrl").

%% Application callbacks
-export([start/2,
				 stop/1]).

%% Server side API
-export([start/0,
				 stop/0,
				 restart/0,
				 run_app/1,
				 get_run_mode/0,
				 set_run_mode/1,
				 uninstall_srv/0
				]).

%% Client side API
-export([uninstall/0,
				 add_source/1,
				 add_exclude/1]).

%%====================================================================
%% Application callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start(Type, StartArgs) -> {ok, Pid} |
%%                                     {ok, Pid, State} |
%%                                     {error, Reason}
%% Description: This function is called whenever an application
%% is started using application:start/1,2, and should start the processes
%% of the application. If the application is structured according to the
%% OTP design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%--------------------------------------------------------------------
start(_Type, _StartArgs) ->
		ok = eklib:ensure_app(crypto),
		ok = eklib:ensure_app(ssl),
		case eb_pers_sup:start_link() of
				{ok, Pid} ->
						{ok, Pid};
				Error ->
						Error
		end.

%%--------------------------------------------------------------------
%% Function: start() -> {ok, Pid} | {ok, Pid, State} | {error, Reason}
%% Description: Simplified version of application:start/2 callback.
%%--------------------------------------------------------------------
start() ->
		start([], []).

%%--------------------------------------------------------------------
%% Function: stop(State) -> void()
%% Description: This function is called whenever an application
%% has stopped. It is intended to be the opposite of Module:start/2 and
%% should do any necessary cleaning up. The return value is ignored.
%%--------------------------------------------------------------------
stop(_State) ->
		eb_pers_sup:stop().

%%--------------------------------------------------------------------
%% Function: stop() -> void()
%% Description: Simplified version of application:stop/1 callback.
%%--------------------------------------------------------------------
stop() ->
		stop([]).

%%--------------------------------------------------------------------
%% Function: restart() -> void()
%% Description: Restarts erlios application
%%--------------------------------------------------------------------
restart() ->
		case whereis(eb_pers_sup) of
				undefined ->
						ok;
				_Pid ->
						stop(),
						eklib:wait_process_terminated(eb_pers_sup)
		end,
		start().

%%--------------------------------------------------------------------
%% Function: uninstall_srv() -> void()
%% Description: Deactivates erlios serial key, server side.
%%--------------------------------------------------------------------
uninstall_srv() ->
		ok.

%%--------------------------------------------------------------------
%% Function: uninstall() -> void()
%% Description: Deactivates erlios serial key, client side.
%%--------------------------------------------------------------------
uninstall() ->
		rpc:call(eb_pers_server@localhost, eb_pers, uninstall_srv, []).

%%--------------------------------------------------------------------
%% Function: run_app([Mode]) -> void()
%% Description: Starts erlios and waits its termination.
%%    Mode - win_service | win_user | linux
%%--------------------------------------------------------------------
run_app([Mode]) ->
		spawn_link(fun() ->
											 register(eb_pers_mode_server, self()),
											 mode_server(Mode)
							 end),
		{ok, Pid} = start(),
		eklib:wait_process_terminated(Pid).

%%--------------------------------------------------------------------
%% Function: get_run_mode() -> win_service | win_user | linux | undefined
%% Description: Returns running mode of application.
%%--------------------------------------------------------------------
get_run_mode() ->
		get_run_mode(whereis(eb_pers_mode_server)).

get_run_mode(undefined) ->
		undefined;
get_run_mode(Srv) ->
		Ref = make_ref(),
		Srv ! {self(), get_run_mode, Ref},
		receive
				{get_run_mode, Ref, Mode} ->
						Mode
		after
				5000 ->
						exit({error, timeout})
		end.

mode_server(Mode) ->
		receive
				{From, get_run_mode, Ref} ->
						From ! {get_run_mode, Ref, Mode},
						mode_server(Mode);
				{set_run_mode, NewMode} ->
						mode_server(NewMode)
		end.

%%--------------------------------------------------------------------
%% Function: set_run_mode(Mode) -> ok
%% Description: Sets run mode.
%%    Mode - win_service | win_user | linux
%%--------------------------------------------------------------------
set_run_mode(Mode) ->
		eb_pers_mode_server ! {set_run_mode, Mode},
		ok.

%%--------------------------------------------------------------------
%% Function: add_source([Path]) -> void()
%% Description: Adds source from command line.
%%    Path - atom
%%--------------------------------------------------------------------
add_source([Path]) ->
		S = atom_to_list(Path),
		case rpc:call(eb_pers_server@localhost, eb_pers_bckp, add_source, [S]) of
				ok ->
						ok;
				{error, remote_path} ->
						message_box(error, "Backup of remote drives is not supported");
				{error, not_absolute} ->
						message_box(error, "Path must be absolute");
				Other ->
						message_box(error, io_lib:format("Unxepected error: ~p", [Other]))
		end.

%%--------------------------------------------------------------------
%% Function: add_exclude([Path]) -> void()
%% Description: Adds excludes from command line.
%%    Path - atom
%%--------------------------------------------------------------------
add_exclude([Path]) ->
		S = atom_to_list(Path),
		case rpc:call(eb_pers_server@localhost, eb_pers_bckp, add_exclude, [S]) of
				ok ->
						ok;
				{error, not_absolute} ->
						message_box(error, "Path must be absolute");
				Other ->
						message_box(error, io_lib:format("Unxepected error: ~p", [Other]))
		end.

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% displays error message box
%%--------------------------------------------------------------------
message_box(Type, Msg) ->
		message_box(Type, Msg, os:type()).

message_box(error, Msg, {win32, _}) ->
		file:set_cwd(code:root_dir()),
		Cmd = lists:flatten(io_lib:format("eb_pers ++ -error_box \"~s\"", [Msg])),
		os:cmd(Cmd),
		ok;
message_box(_, _, _) ->
		ok.
