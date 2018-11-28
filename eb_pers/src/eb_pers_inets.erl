%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eb_pers_inets.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Starts/stops inets http server. Redirects root urls to
%%%               application.
%%%
%%% Created :  8 Apr 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(eb_pers_inets).

-behaviour(gen_server).

-include_lib("kernel/include/inet.hrl").
-include_lib("inets/src/http_server/httpd.hrl").
%% undefine macro defined in httpd.hrl
-undef(ERROR).

-include("eklib/include/debug.hrl").

%% API
-export([start_link/0, stop/0]).

%% EWSAPI (Erlang Web Server API) module callbacks
-export([do/1, load/2, store/2, remove/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
				 terminate/2, code_change/3]).

%% Server definitions
-define(SERVER, ?MODULE).
-record(state, {httpd}).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link() ->
		gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Function: stop() -> ok
%% Description: Stops the server
%%--------------------------------------------------------------------
stop() ->
		gen_server:call(?SERVER, stop).

%%====================================================================
%% EWSAPI (Erlang Web Server API) module callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% This a dummy httpd callback function
%%--------------------------------------------------------------------
load([], []) ->
		{error, not_defined}.

%%--------------------------------------------------------------------
%% This a dummy httpd callback function
%%--------------------------------------------------------------------
store([], []) ->
		{error, not_defined}.

%%--------------------------------------------------------------------
%% When httpd is shutdown it will try to execute remove/1 in each
%% Erlang web server callback module.
%%--------------------------------------------------------------------
remove(_ConfigDB) ->
		ok.

%%--------------------------------------------------------------------
%% When a valid request reaches httpd it calls do/1 in each module
%% defined by the Modules configuration option.
%% This implementation just redirects "/" requests to
%% /eb/eb_pers_ui/home URL.
%%--------------------------------------------------------------------
do(ModData) ->
		case ModData#mod.request_uri of
				"/" ->
						%% redirect root requesr
						do_redirect_root(ModData);
				_ ->
 						{proceed, ModData#mod.data}
 		end.

do_redirect_root(ModData) ->
		AbsUri =
				if
						ModData#mod.absolute_uri =:= nohost ->
								get_server_name() ++ port_string(get_port()) ++ "/";
						true ->
								ModData#mod.absolute_uri
				end,
		URL = "http://" ++ AbsUri ++ "eb/eb_pers_ui/home",
		ReasonPhrase = httpd_util:reason_phrase(301),
		Message = httpd_util:message(301, URL, ModData#mod.config_db),
		{proceed,
	     [{response,
				 {response,
					[{code, 301}, {location, URL}, {content_type, "text/html"}],
					h:html([h:head(h:title(ReasonPhrase)),
									h:body([h:h1(ReasonPhrase),
													Message])])}}]}.

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
		{ok, Httpd} = start_httpd(),
		ok = make_url_file(),
		{ok, #state{httpd = Httpd}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(stop, _From, State) ->
		{stop, normal, ok, State};

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

%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, State) ->
		#state{httpd = Httpd} = State,
		stop_httpd(Httpd).

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
%%--------------------------------------------------------------------
%% Function: start() -> {ok, Pid} | {error, Reason}
%% Description: Initiates web server
%%--------------------------------------------------------------------
start_httpd() ->
		ok = eklib:ensure_app(inets),
		ServerRoot = get_doc_root(),
		DocRoot = ServerRoot,
		inets:start(httpd,
								[%% Mandatory options
								 {bind_address, get_bind_address()},
								 {port, get_port()},
								 {server_name, get_server_name()},
								 {server_root, ServerRoot},
								 {document_root, DocRoot},
								 %% {socket_type, ssl},
								 {socket_type, ip_comm},

								 %% Do not change the order of these module names!!
								 {modules, [?MODULE,
														%% mod_alias,
														%% mod_auth,
														mod_esi,
														%% mod_actions,
														%% mod_cgi,
														%% mod_include,
														%% mod_dir,
														mod_get
														%% mod_head,
														%% mod_log,
														%% mod_disk_log,
													 ]},

								 %% Logs
%% 								 {error_disk_log, "/tmp/errors.log"},
%% 								 {error_disk_log_size, {1024 * 1024, 10}},

%% 								 {security_disk_log, "/tmp/security.log"},
%% 								 {security_disk_log_size, {1024 * 1024, 10}},

%% 								 {transfer_disk_log, "/tmp/transfer.log"},
%% 								 {transfer_disk_log_size, {1024 * 1024, 10}},

								 %% MIME types
								 {mime_types, [{"txt", "text/plain"},
															 {"html", "text/html"},
															 {"htm", "text/html"},
															 {"css", "text/css"},
															 {"png", "image/png"},
															 {"jpeg", "image/jpeg"},
															 {"gif", "image/gif"}]},
								 {mime_type, "text/html"},

								 %% Set up mod_esi
								 {erl_script_alias, {"/eb", [eb_pers_ui]}}],
							 stand_alone).

%%--------------------------------------------------------------------
%% Function: stop_httpd() -> ok | {error, Reason}
%% Description: Stops web server
%%--------------------------------------------------------------------
stop_httpd(Httpd) ->
		inets:stop(httpd, Httpd).

%%--------------------------------------------------------------------
%% Returns web server port
%%--------------------------------------------------------------------
get_port() ->
		case eb_pers_conf:get_value(?MODULE, port) of
				not_found ->
						{ok, Port} = get_free_port(get_bind_address(), 8000, 4096),
						eb_pers_conf:set_value(?MODULE, port, Port),
						Port;
				{ok, Port} ->
						Port
		end.

%%--------------------------------------------------------------------
%% Returns web server name
%%--------------------------------------------------------------------
get_server_name() ->
		"localhost".

%%--------------------------------------------------------------------
%% Returns web server bind address
%%--------------------------------------------------------------------
get_bind_address() ->
		"localhost".
%%		{127,0,0,1}.
%% 		{ok, BindAddress} = eb_pers_conf:get_value(?MODULE, bind_address, {127,0,0,1}),
%% 		BindAddress.

%%--------------------------------------------------------------------
%% Returns string for the port
%%--------------------------------------------------------------------
port_string(80) ->
    "";
port_string(Port) ->
    ":"++integer_to_list(Port).

%%--------------------------------------------------------------------
%% Function: get_free_port(Addr, InitialPort, MaxPortsToCheck) -> {ok, Port} | {error, Error}
%% Description: Determinates first free port on Addr in interval from
%% InitialPort and upto InitialPort + MaxPortsToCheck
%%--------------------------------------------------------------------
get_free_port(_Addr, _Port, 0) ->
    {error, no_free_port_found};
get_free_port(Addr, Port, N) ->
    case gen_tcp:connect(Addr, Port, []) of
				{ok, Sock} ->
						gen_tcp:close(Sock),
						get_free_port(Addr, Port + 1, N - 1);
				_ ->
						case gen_tcp:listen(Port, []) of
								{ok, Sock} ->
										gen_tcp:close(Sock),
										{ok, Port};
								_ ->
										get_free_port(Addr, Port + 1, N - 1)
						end
    end.

%%--------------------------------------------------------------------
%% Function: get_doc_root() -> Dir
%% Description: Returns documentation root of server.
%%--------------------------------------------------------------------
get_doc_root() ->
		PrivDir = eklib:get_priv_dir(?MODULE),
		filename:join(PrivDir, www).

%%--------------------------------------------------------------------
%% Function: make_url_file() -> Dir
%% Description: Creates URL file for windows shortcut
%%--------------------------------------------------------------------
make_url_file() ->
		make_url_file(os:type()).

make_url_file({win32, _}) ->
		UrlFile = filename:join(code:root_dir(), "eb_pers.url"),
		case eklib:fs_object_exist(UrlFile) of
				true ->
						ok;
				false ->
						UrlData = io_lib:format("[InternetShortcut]~nURL=http://~s:~p~n",
																		[get_server_name(), get_port()]),
						ok = file:write_file(UrlFile, UrlData)
		end;
make_url_file(_) ->
		ok.
