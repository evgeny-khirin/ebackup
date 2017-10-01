%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : kdb.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Fault tolerant database.
%%%
%%% Created :  2 Dec 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(kdb).

-behaviour(gen_server).

-include("eklib/include/debug.hrl").

%% KDB API
-export([start_link/0,
				 start_link/1,
				 stop/1,
				 start_object/3,
				 find_object/2,
				 stop_object/2,
				 set_stop_list/2,
				 ld_bd_is_formatted/3,
				 ld_bd_format/3,
				 ld_bd_system_generation/2,
				 recover/1,
				 open_table/2,
				 close_table/2,
				 drop_table/2,
				 used/1,
				 transaction/2,
				 transaction/3,
				 transaction/4,
				 insert/4,
				 lookup/3,
				 remove/3,
				 iterator/2,
				 seek/4,
				 next/2,
				 close_iter/2,
				 root_iterator/1,
				 get_path_type/2
				]).

%% Utilities
-export([print/2,
				 ls/1
				]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
				 terminate/2, code_change/3]).

%% Server state
-record(kdb, {port,													% KDB driver port
							stop_list = [],								% objects, which must be stopped
							stack = [],										% transactions stack: [{Pid, TxnId}]
							pending = queue:new()					% pending queue
						 }).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link([Name]) -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the KDB server.
%%    Name = {local,Name} | {global,Name}. See gen_server:start_link for details.
%%--------------------------------------------------------------------
start_link() ->
		gen_server:start_link(?MODULE, [], []).
start_link(Name) ->
		gen_server:start_link(Name, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% Function: stop(Pid) -> ok
%% Description: Stops the KDB server.
%%    Pid = pid
%%--------------------------------------------------------------------
stop(Pid) ->
		gen_server:call(Pid, stop).

%%--------------------------------------------------------------------
%% Function: start_object(Pid, Class, Options) -> {ok, ObjId} | {error, Error}
%% Description: Starts KDB object. Returns object ID.
%%    Pid = pid
%%    Class = atom
%%    Options = [{Key, Value}].
%%    Key = atom
%%    Value = term
%%    ObjId = integer
%%--------------------------------------------------------------------
start_object(Pid, Class, Options) ->
		gen_server:call(Pid, {start_object, Class, Options}, infinity).

%%--------------------------------------------------------------------
%% Function: find_object(Pid, Name) -> {ok, ObjId} | not_found | {error,Error}
%% Description: Finds object by name.
%%    Pid = pid
%%    Name = term
%%    ObjId = integer
%%--------------------------------------------------------------------
find_object(Pid, Name) ->
		gen_server:call(Pid, {find_object, Name}).

%%--------------------------------------------------------------------
%% Function: stop_object(Pid, Name) -> ok | {error,Error}
%% Description: Stops object by name.
%%    Pid = pid
%%    Name = term
%%--------------------------------------------------------------------
stop_object(Pid, Name) ->
		gen_server:call(Pid, {stop_object, Name}, 30000).

%%--------------------------------------------------------------------
%% Function: set_stop_list(Pid, List) -> ok
%% Description: Sets list of objects, which must be stopped on terminate.
%%    Pid = pid
%%    List = [term]
%%--------------------------------------------------------------------
set_stop_list(Pid, List) ->
		gen_server:call(Pid, {set_stop_list, List}).

%%--------------------------------------------------------------------
%% Function: ld_bd_is_formatted(Pid, BlockDeviceId, AppName) -> bool | {error, Error}
%% Description: Calls ld_bd::is_formatted for specified block device ID and
%% application name.
%%    Pid = pid
%%    BlockDeviceId = integer
%%    AppName = atom or string
%%--------------------------------------------------------------------
ld_bd_is_formatted(Pid, BlockDeviceId, AppName) ->
		gen_server:call(Pid, {ld_bd_is_formatted, BlockDeviceId, AppName}).

%%--------------------------------------------------------------------
%% Function: ld_bd_format(Pid, BlockDeviceId, AppName) -> ok | {error, Error}
%% Description: Calls ld_bd::format for specified block device ID and
%% application name.
%%    Pid = pid
%%    BlockDeviceId = integer
%%    AppName = atom or string
%%--------------------------------------------------------------------
ld_bd_format(Pid, BlockDeviceId, AppName) ->
		gen_server:call(Pid, {ld_bd_format, BlockDeviceId, AppName}).

%%--------------------------------------------------------------------
%% Function: ld_bd_system_generation(Pid, LdBdId) -> {ok, Gen} | {error, Error}
%% Description: Calls ld_bd::system_generation for specified ld_bd device ID.
%%    Pid = pid
%%    LdBdId = integer
%%    Gen = integer
%%--------------------------------------------------------------------
ld_bd_system_generation(Pid, LdBdId) ->
		gen_server:call(Pid, {ld_bd_system_generation, LdBdId}).

%%--------------------------------------------------------------------
%% Function: recover(Pid) -> ok | {error, Error}
%% Description: Calls kdb::recover.
%%    Pid = pid
%%--------------------------------------------------------------------
recover(Pid) ->
		gen_server:call(Pid, recover, infinity).

%%--------------------------------------------------------------------
%% Function: open_table(Pid, Name) -> {ok, TblId} | {error, Error}
%% Description: Opens table on KDB object.
%%    Pid = pid
%%    Name = term
%%    TblId = integer
%%--------------------------------------------------------------------
open_table(Pid, Name) ->
		gen_server:call(Pid, {open_table, Name}).

%%--------------------------------------------------------------------
%% Function: close_table(Pid, TblId) -> ok | {error, Error}
%% Description: Closes table on KDB object.
%%    Pid = pid
%%    TblId = integer
%%--------------------------------------------------------------------
close_table(Pid, TblId) ->
		gen_server:call(Pid, {close_table, TblId}).

%%--------------------------------------------------------------------
%% Function: drop_table(Pid, Name) -> ok | {error, Error}
%% Description: Drops table on KDB object.
%%    Pid = pid
%%    Name = term
%%--------------------------------------------------------------------
drop_table(Pid, Name) ->
		gen_server:call(Pid, {drop_table, Name}).

%%--------------------------------------------------------------------
%% Function: used(Pid) -> {ok, Used} | {error,Error}
%% Description: Returns number of blocks used by KDB.
%%    Pid = pid
%%    Used = integer
%%--------------------------------------------------------------------
used(Pid) ->
		gen_server:call(Pid, used).

%%--------------------------------------------------------------------
%% Function: transaction(Pid, Fun[, Type[, Parent]]) -> {atomic, Result} | {aborted, Reason}
%% Description: Executes function as transaction in context of KDB.
%%    Pid = pid
%%    Fun = function
%%    Type = hard | soft | read_only. Default is soft.
%%    Parent = pid
%%--------------------------------------------------------------------
transaction(Pid, Fun) ->
		transaction(Pid, Fun, soft).

transaction(Pid, Fun, Type) ->
		transaction(Pid, Fun, Type, self()).

transaction(Pid, Fun, Type, Parent) ->
		case gen_server:call(Pid, {begin_transaction, Type, Parent}) of
		{ok, TxnId} ->
						case catch Fun() of
								{'EXIT', Reason} ->
										ok = gen_server:call(Pid, {rollback, TxnId}),
										{aborted, Reason};
								Result ->
										ok = gen_server:call(Pid, {commit, TxnId}),
										{atomic, Result}
						end;
				Error ->
						Error
		end.

%%--------------------------------------------------------------------
%% Function: insert(Pid, TblId, K, V) -> ok | {error, Error}
%% Description: Inserts key value pair into table.
%%    Pid = pid
%%    TblId = integer
%%    K = term
%%    V = term
%%--------------------------------------------------------------------
insert(Pid, TblId, K, V) ->
		gen_server:call(Pid, {insert, TblId, K, V}).

%%--------------------------------------------------------------------
%% Function: lookup(Pid, TblId, K) -> {ok, V} | not_found | {error, Error}
%% Description: Searches table for key.
%%    Pid = pid
%%    TblId = integer
%%    K = term
%%    V = term
%%--------------------------------------------------------------------
lookup(Pid, TblId, K) ->
		gen_server:call(Pid, {lookup, TblId, K}).

%%--------------------------------------------------------------------
%% Function: remove(Pid, TblId, K) -> ok | {error, Error}
%% Description: Removes key from table.
%%    Pid = pid
%%    TblId = integer
%%    K = term
%%--------------------------------------------------------------------
remove(Pid, TblId, K) ->
		gen_server:call(Pid, {remove, TblId, K}).

%%--------------------------------------------------------------------
%% Function: iterator(Pid, TblId) -> {ok, Iter} | {error, Error}
%% Description: Opens table iterator.
%%    Pid = pid
%%    TblId = integer
%%    Iter = integer
%%--------------------------------------------------------------------
iterator(Pid, TblId) ->
		gen_server:call(Pid, {iterator, TblId}).

%%--------------------------------------------------------------------
%% Function: seek(Pid, TblId, K, Pos) -> {ok, Iter} | {error, Error}
%% Description: Opens table iterator with required position.
%%    Pid = pid
%%    TblId = integer
%%    K = term
%%    Pos = prev | next | exact | exact_prev | exact_next
%%    Iter = integer
%%--------------------------------------------------------------------
seek(Pid, TblId, K, Pos) ->
		gen_server:call(Pid, {seek, TblId, K, Pos}).

%%--------------------------------------------------------------------
%% Function: next(Pid, Iter) -> {ok, K, V} | eof | {error, Error}
%% Description: Fetches next key-value pair by iterator.
%%    Pid = pid
%%    Iter = integer
%%    K = term
%%    V = term
%%--------------------------------------------------------------------
next(Pid, Iter) ->
		gen_server:call(Pid, {next, Iter}).

%%--------------------------------------------------------------------
%% Function: close_iter(Pid, Iter) -> ok | {error, Error}
%% Description: Rleases iterator.
%%    Pid = pid
%%    Iter = integer
%%--------------------------------------------------------------------
close_iter(Pid, Iter) ->
		gen_server:call(Pid, {close_iter, Iter}).

%%--------------------------------------------------------------------
%% Function: root_iterator(Pid) -> {ok, Iter} | {error,Error}
%% Description: Obtains iterator for root table of KDB..
%%    Pid = pid
%%    Iter = integer
%%--------------------------------------------------------------------
root_iterator(Pid) ->
		gen_server:call(Pid, root_iterator).

%%--------------------------------------------------------------------
%% Function: get_path_type(Port, Path) -> local | remote | undefined | {error, Error}
%% Description: Returns type of drive for path: local, remote or undefined.
%%    Pid = pid
%%    Path = string
%%--------------------------------------------------------------------
get_path_type(Pid, Path) ->
		get_path_type(Pid, filename:nativename(Path), os:type()).

get_path_type(_Pid, [$\\, $\\ | _], {win32, _}) ->
		remote;
get_path_type(Pid, Path, _) ->
		gen_server:call(Pid, {get_path_type, Path}).

%%====================================================================
%% Utilities
%%====================================================================
%%--------------------------------------------------------------------
%% Function: print(Pid, Table) -> ok | {error,Error}
%% Description: Prints content of table.
%%    Pid = pid
%%    Table = term (table name)
%%--------------------------------------------------------------------
print(Pid, Table) ->
		F = fun() ->
								{ok, T} = open_table(Pid, Table),
								{ok, Iter} = iterator(Pid, T),
								print(Pid, Iter, next(Pid, Iter)),
								close_iter(Pid, Iter),
								close_table(Pid, T)
				end,
		{atomic, ok} = transaction(Pid, F),
		ok.

print(_Pid, _Iter, eof) ->
		ok;
print(Pid, Iter, {ok, K, V}) ->
		io:format("~p, ~p~n", [K, V]),
		print(Pid, Iter, next(Pid, Iter)).

%%--------------------------------------------------------------------
%% Function: ls(Pid) -> ok | {error,Error}
%% Description: Lists content of root table.
%%--------------------------------------------------------------------
ls(Pid) ->
		F = fun() ->
								{ok, Iter} = root_iterator(Pid),
								print(Pid, Iter, next(Pid, Iter)),
								close_iter(Pid, Iter)
				end,
		{atomic, ok} = transaction(Pid, F, read_only),
		ok.

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
		Port = kdb_drv:load(),
		{ok, #kdb{port = Port}}.

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

handle_call({start_object, Class, Options}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:start_object(Port, Class, Options),
		{reply, Reply, State};

handle_call({find_object, Name}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:find_object(Port, Name),
		{reply, Reply, State};

handle_call({stop_object, Name}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:stop_object(Port, Name),
		{reply, Reply, State};

handle_call({set_stop_list, List}, _From, State) ->
		{reply, ok, State#kdb{stop_list = List}};

handle_call({ld_bd_is_formatted, BlockDeviceId, AppName}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:ld_bd_is_formatted(Port, BlockDeviceId, AppName),
		{reply, Reply, State};

handle_call({ld_bd_format, BlockDeviceId, AppName}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:ld_bd_format(Port, BlockDeviceId, AppName),
		{reply, Reply, State};

handle_call({ld_bd_system_generation, LdBdId}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:ld_bd_system_generation(Port, LdBdId),
		{reply, Reply, State};

handle_call(recover, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:recover(Port),
		{reply, Reply, State};

handle_call({open_table, Name}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:open_table(Port, term_to_binary(Name)),
		{reply, Reply, State};

handle_call({close_table, Tbl}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:close_table(Port, Tbl),
		{reply, Reply, State};

handle_call({drop_table, Name}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:drop_table(Port, term_to_binary(Name)),
		{reply, Reply, State};

handle_call(used, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:used(Port),
		{reply, Reply, State};

handle_call({begin_transaction, Type, Parent}, From, State) ->
		handle_begin_transaction(State, From, Type, Parent);

handle_call({commit, TxnId}, _From, State) ->
		#kdb{port = Port, stack = [_ | T]} = State,
		ok = kdb_drv:commit(Port, TxnId),
		{reply, ok, resume_pending(State#kdb{stack = T})};

handle_call({rollback, TxnId}, _From, State) ->
		#kdb{port = Port, stack = [_ | T]} = State,
		ok = kdb_drv:rollback(Port, TxnId),
		{reply, ok, resume_pending(State#kdb{stack = T})};

handle_call({insert, Tbl, K, V}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:insert(Port, Tbl,
													 term_to_binary(K),
													 term_to_binary(V)),
		{reply, Reply, State};

handle_call({lookup, Tbl, K}, _From, State) ->
		#kdb{port = Port} = State,
		Reply =
				case kdb_drv:lookup(Port, Tbl,
														term_to_binary(K)) of
						{ok, V} ->
								{ok, binary_to_term(V)};
						Other ->
								Other
				end,
		{reply, Reply, State};

handle_call({remove, Tbl, K}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:remove(Port, Tbl,
													 term_to_binary(K)),
		{reply, Reply, State};

handle_call({iterator, Tbl}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:iterator(Port, Tbl),
		{reply, Reply, State};

handle_call({seek, Tbl, K, Pos}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:seek(Port, Tbl,
												 term_to_binary(K),
												 Pos),
		{reply, Reply, State};

handle_call({close_iter, It}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:close_iter(Port, It),
		{reply, Reply, State};

handle_call({next, It}, _From, State) ->
		#kdb{port = Port} = State,
		Reply =
				case kdb_drv:next(Port, It) of
						{ok, K, V} ->
								{ok, binary_to_term(K), binary_to_term(V)};
						Other ->
								Other
				end,
		{reply, Reply, State};

handle_call(root_iterator, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:root_iterator(Port),
		{reply, Reply, State};

handle_call({get_path_type, Path}, _From, State) ->
		#kdb{port = Port} = State,
		Reply = kdb_drv:get_drive_type(Port, get_root_drive(Path)),
		{reply, Reply, State};

handle_call(Request, From, State) ->
		?WARN("unexpected call from ~p: ~p", [From, Request]),
		Reply = {error, unexpected_call},
		{reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
		?WARN("unexpected cast: ~p", [Msg]),
		{noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({Port, {data, _}}, #kdb{port = Port} = State) ->
		{stop, {error, unhandled_driver_reply}, State};

handle_info({'EXIT', _, normal}, State) ->
		{noreply, State};

handle_info({'EXIT', _, Reason}, State) ->
		{stop, {linked_process_exit, Reason}, State};

handle_info({Port,{exit_status,_}}, #kdb{port = Port} = State) ->
		%% driver terminated, let's supervisor to restrat the server
		{stop, {error, driver_dead}, State#kdb{port = undefined}};

handle_info(Info, State) ->
		?WARN("unexpected info: ~p", [Info]),
		{noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, #kdb{port = Port} = State) when is_port(Port) ->
		#kdb{stop_list = List} = State,
		lists:foreach(fun(Name) ->
													kdb_drv:stop_object(Port, Name)
									end,
									List);
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
%%--------------------------------------------------------------------
%% Starts new transaction
%%--------------------------------------------------------------------
handle_begin_transaction(State, From, Type, Parent) ->
		#kdb{stack = TxnStack} = State,
		handle_begin_transaction(State, From, Type, Parent, TxnStack).

handle_begin_transaction(State, {Pid, _}, Type, _Parent, []) ->
		%% No active transactions
		#kdb{port = Port} = State,
		Reply = kdb_drv:begin_transaction(Port, Type),
		case Reply of
				{ok, _} ->
						{reply, Reply, State#kdb{stack = [Pid]}};
				_ ->
						{reply, Reply, State}
		end;
handle_begin_transaction(State, {Pid, _}, Type, _Parent, [Pid | _] = TxnStack) ->
		%% Nested transaction from same process
		#kdb{port = Port} = State,
		Reply = kdb_drv:begin_transaction(Port, Type),
		case Reply of
				{ok, _} ->
						{reply, Reply, State#kdb{stack = [Pid | TxnStack]}};
				_ ->
						{reply, Reply, State}
		end;
handle_begin_transaction(State, {Pid, _}, Type, Parent, [Parent | _] = TxnStack) ->
		%% Nested transaction migrated between processes
		#kdb{port = Port} = State,
		Reply = kdb_drv:begin_transaction(Port, Type),
		case Reply of
				{ok, _} ->
						{reply, Reply, State#kdb{stack = [Pid | TxnStack]}};
				_ ->
						{reply, Reply, State}
		end;
handle_begin_transaction(State, From, Type, _Parent, _TxnStack) ->
		%% Nested transaction can not be started, put it in pending queue.
		#kdb{pending = Pending} = State,
		{noreply, State#kdb{pending = queue:in({From, Type}, Pending)}}.

%%--------------------------------------------------------------------
%% Resumes pending new transaction
%%--------------------------------------------------------------------
resume_pending(#kdb{stack = []} = State) ->
		#kdb{pending = Pending00} = State,
		case queue:out(Pending00) of
				{empty, Pending00} ->
						State;
				{{value, {{Pid, _} = From, Type}}, Pending10} ->
						#kdb{port = Port} = State,
						Reply = kdb_drv:begin_transaction(Port, Type),
						gen_server:reply(From, Reply),
						case Reply of
								{ok, _} ->
										State#kdb{stack = [Pid], pending = Pending10};
								_ ->
										State
						end
		end;
resume_pending(State) ->
		State.

%%--------------------------------------------------------------------
%% Returns root drive for get_drive_type function
%%--------------------------------------------------------------------
get_root_drive(Path) ->
		get_root_drive(Path, os:type()).

get_root_drive(Path, {unix, _}) ->
		Path;
%% Windows UNC names are checked before calling server
get_root_drive([Drive, $: | _], {win32, _}) ->
		[Drive, $:, $\\].
