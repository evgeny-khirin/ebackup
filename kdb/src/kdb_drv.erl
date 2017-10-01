%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : kdb_drv.erl
%%% Author  : Evgeny Khirin <>
%%% Description : KDB driver interface.
%%%-------------------------------------------------------------------
-module(kdb_drv).

-include("eklib/include/debug.hrl").

%% API
-export([load/0,
				 start_object/3,
				 find_object/2,
				 stop_object/2,
				 ld_bd_is_formatted/3,
				 ld_bd_format/3,
				 ld_bd_system_generation/2,
				 recover/1,
				 begin_transaction/2,
				 commit/2,
				 rollback/2,
				 open_table/2,
				 close_table/2,
				 drop_table/2,
				 insert/4,
				 lookup/3,
				 remove/3,
				 iterator/2,
				 seek/4,
				 next/2,
				 close_iter/2,
				 used/1,
				 root_iterator/1,
				 get_drive_type/2
				]).

%%====================================================================
%% Driver operations
%%====================================================================
-define(START_OBJECT, 1).
-define(FIND_OBJECT, 2).
-define(STOP_OBJECT, 3).
-define(LD_BD_IS_FORMATTED, 4).
-define(LD_BD_FORMAT, 5).
-define(LD_BD_SYSTEM_GENERATION, 6).
-define(RECOVER, 7).
-define(BEGIN_TRANSACTION, 8).
-define(COMMIT, 9).
-define(ROLLBACK, 10).
-define(OPEN_TABLE, 11).
-define(CLOSE_TABLE, 12).
-define(DROP_TABLE, 13).
-define(INSERT, 14).
-define(LOOKUP, 15).
-define(REMOVE, 16).
-define(BEGIN, 17).
-define(SEEK, 18).
-define(NEXT, 19).
-define(CLOSE_ITER, 20).
-define(USED, 21).
-define(ROOT_BEGIN, 22).
-define(GET_DRIVE_TYPE, 23).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: load() -> port()
%% Description: Loads KDB driver and opens port.
%%--------------------------------------------------------------------
load() ->
		PrivDir = eklib:get_priv_dir(?MODULE),
		DrvName = filename:join(PrivDir, ?MODULE),
		open_port({spawn, DrvName}, [{packet, 4}, hide, binary, exit_status]).

%%--------------------------------------------------------------------
%% Function: start_object(Port, Class, Options) -> {ok, ObjId} | {error, Error}
%% Description: Starts driver object. Returns object ID.
%%    Port = port
%%    Class = atom
%%    Options = [{Key, Value}].
%%    Key = atom
%%    Value = term
%%    ObjId = integer
%%--------------------------------------------------------------------
start_object(Port, Class, Options) ->
		Cmd = encode_command(?START_OBJECT),
		Packet = encode_options(Options, encode(Class, Cmd)),
		port_command(Port, Packet),
		get_reply(Port, infinity).

%%--------------------------------------------------------------------
%% Function: find_object(Port, Name) -> {ok, ObjId} | not_found | {error, Error}
%% Description: Finds object by name.
%%    Port = port
%%    Name = term
%%    ObjId = integer
%%--------------------------------------------------------------------
find_object(Port, Name) ->
		Cmd = encode_command(?FIND_OBJECT),
		Packet = encode(Name, Cmd),
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: stop_object(Port, Name) -> ok | {error, Error}
%% Description: Stops object by name.
%%    Port = port
%%    Name = term
%%--------------------------------------------------------------------
stop_object(Port, Name) ->
		Cmd = encode_command(?STOP_OBJECT),
		Packet = encode(Name, Cmd),
		port_command(Port, Packet),
		get_reply(Port, 30000).

%%--------------------------------------------------------------------
%% Function: ld_bd_is_formatted(Port, BlockDeviceId, AppName) -> bool | {error, Error}
%% Description: Calls ld_bd::is_formatted for specified block device ID and
%% application name.
%%    Port = port
%%    BlockDeviceId = integer
%%    AppName = atom or string
%%--------------------------------------------------------------------
ld_bd_is_formatted(Port, BlockDeviceId, AppName) ->
		Cmd = encode_command(?LD_BD_IS_FORMATTED),
		Packet = encode(AppName, <<Cmd/binary, BlockDeviceId:32/unsigned-native>>),
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: ld_bd_format(Port, BlockDeviceId, AppName) -> ok | {error, Error}
%% Description: Calls ld_bd::format for specified block device ID and
%% application name.
%%    Port = port
%%    BlockDeviceId = integer
%%    AppName = atom or string
%%--------------------------------------------------------------------
ld_bd_format(Port, BlockDeviceId, AppName) ->
		Cmd = encode_command(?LD_BD_FORMAT),
		Packet = encode(AppName, <<Cmd/binary, BlockDeviceId:32/unsigned-native>>),
		port_command(Port, Packet),
		ok.

%%--------------------------------------------------------------------
%% Function: ld_bd_system_generation(Port, LdBdId) -> {ok, Gen} | {error, Error}
%% Description: Calls ld_bd::system_generation for specified ld_bd device ID.
%%    Port = port
%%    LdBdId = integer
%%    Gen = integer
%%--------------------------------------------------------------------
ld_bd_system_generation(Port, LdBdId) ->
		Cmd = encode_command(?LD_BD_SYSTEM_GENERATION),
		Packet = <<Cmd/binary, LdBdId:32/unsigned-native>>,
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: recover(Port) -> ok | {error, Error}
%% Description: Calls kdb::recover for given KDB object..
%%    Port = port
%%--------------------------------------------------------------------
recover(Port) ->
		Cmd = encode_command(?RECOVER),
		port_command(Port, Cmd),
		get_reply(Port, infinity).

%%--------------------------------------------------------------------
%% Function: begin_transaction(Port, TxnType) -> {ok, TxnId} | {error, Error}
%% Description: Starts transaction on KDB object.
%%    Port = port
%%    TxnType = hard | soft | read_only
%%    TxnId = integer
%%--------------------------------------------------------------------
begin_transaction(Port, TxnType) ->
		Cmd = encode_command(?BEGIN_TRANSACTION),
		Packet = encode(TxnType, Cmd),
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: commit(Port, TxnId) -> ok | {error, Error}
%% Description: Commits transaction on KDB object.
%%    Port = port
%%    TxnId = integer
%%--------------------------------------------------------------------
commit(Port, TxnId) ->
		Cmd = encode_command(?COMMIT),
		Packet = <<Cmd/binary, TxnId:32/unsigned-native>>,
		port_command(Port, Packet),
		ok.

%%--------------------------------------------------------------------
%% Function: rollback(Port, TxnId) -> ok | {error, Error}
%% Description: Rolls back transaction on KDB object.
%%    Port = port
%%    TxnId = integer
%%--------------------------------------------------------------------
rollback(Port, TxnId) ->
		Cmd = encode_command(?ROLLBACK),
		Packet = <<Cmd/binary, TxnId:32/unsigned-native>>,
		port_command(Port, Packet),
		ok.

%%--------------------------------------------------------------------
%% Function: open_table(Port, Name) -> {ok, TblId} | {error, Error}
%% Description: Opens table on KDB object.
%%    Port = port
%%    Name = binary
%%    TblId = integer
%%--------------------------------------------------------------------
open_table(Port, Name) ->
		Cmd = encode_command(?OPEN_TABLE),
		Size = size(Name),
		Packet = <<Cmd/binary, Size:32/unsigned-native, Name/binary>>,
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: close_table(Port, TblId) -> ok | {error, Error}
%% Description: Closes table on KDB object.
%%    Port = port
%%    TblId = integer
%%--------------------------------------------------------------------
close_table(Port, TblId) ->
		Cmd = encode_command(?CLOSE_TABLE),
		Packet = <<Cmd/binary, TblId:32/unsigned-native>>,
		port_command(Port, Packet),
		ok.

%%--------------------------------------------------------------------
%% Function: drop_table(Port, Name) -> ok | {error, Error}
%% Description: Drops table on KDB object.
%%    Port = port
%%    Name = binary
%%--------------------------------------------------------------------
drop_table(Port, Name) ->
		Cmd = encode_command(?DROP_TABLE),
		Size = size(Name),
		Packet = <<Cmd/binary, Size:32/unsigned-native, Name/binary>>,
		port_command(Port, Packet),
		ok.

%%--------------------------------------------------------------------
%% Function: insert(Port, TblId, K, V) -> ok | {error, Error}
%% Description: Inserts key-value pair into table.
%%    Port = port
%%    TblId = integer
%%    K = binary
%%    V = binary
%%--------------------------------------------------------------------
insert(Port, TblId, K, V) ->
		Cmd = encode_command(?INSERT),
		KS = size(K),
		VS = size(V),
		Packet = <<Cmd/binary, TblId:32/unsigned-native, KS:32/unsigned-native, K/binary, VS:32/unsigned-native, V/binary>>,
		port_command(Port, Packet),
		ok.

%%--------------------------------------------------------------------
%% Function: lookup(Port, TblId, K) -> {ok, V} | not_found | {error, Error}
%% Description: Searches table for key.
%%    Port = port
%%    TblId = integer
%%    K = binary
%%    V = binary
%%--------------------------------------------------------------------
lookup(Port, TblId, K) ->
		Cmd = encode_command(?LOOKUP),
		KS = size(K),
		Packet = <<Cmd/binary, TblId:32/unsigned-native, KS:32/unsigned-native, K/binary>>,
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: remove(Port, TblId, K) -> ok | {error, Error}
%% Description: Removes key from table.
%%    Port = port
%%    TblId = integer
%%    K = binary
%%--------------------------------------------------------------------
remove(Port, TblId, K) ->
		Cmd = encode_command(?REMOVE),
		KS = size(K),
		Packet = <<Cmd/binary, TblId:32/unsigned-native, KS:32/unsigned-native, K/binary>>,
		port_command(Port, Packet),
		ok.

%%--------------------------------------------------------------------
%% Function: iterator(Port, TblId) -> {ok, Iter} | {error, Error}
%% Description: Opens table iterator.
%%    Port = port
%%    TblId = integer
%%    Iter = integer
%%--------------------------------------------------------------------
iterator(Port, TblId) ->
		Cmd = encode_command(?BEGIN),
		Packet = <<Cmd/binary, TblId:32/unsigned-native>>,
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: seek(Port, TblId, K, Pos) -> {ok, Iter} | {error, Error}
%% Description: Opens table iterator with required position.
%%    Port = port
%%    TblId = integer
%%    K = binary
%%    Pos = prev | next | exact | exact_prev | exact_next
%%    Iter = integer
%%--------------------------------------------------------------------
seek(Port, TblId, K, Pos) ->
		Cmd = encode_command(?SEEK),
		KS = size(K),
		Packet = encode(Pos, <<Cmd/binary, TblId:32/unsigned-native, KS:32/unsigned-native, K/binary>>),
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: next(Port, Iter) -> {ok, K, V} | eof | {error, Error}
%% Description: Fetches next key-value pair by iterator.
%%    Port = port
%%    Iter = integer
%%    K = binary
%%    V = binary
%%--------------------------------------------------------------------
next(Port, Iter) ->
		Cmd = encode_command(?NEXT),
		Packet = <<Cmd/binary, Iter:32/unsigned-native>>,
		port_command(Port, Packet),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: close_iter(Port, Iter) -> ok | {error, Error}
%% Description: Rleases iterator.
%%    Port = port
%%    Iter = integer
%%--------------------------------------------------------------------
close_iter(Port, Iter) ->
		Cmd = encode_command(?CLOSE_ITER),
		Packet = <<Cmd/binary, Iter:32/unsigned-native>>,
		port_command(Port, Packet),
		ok.

%%--------------------------------------------------------------------
%% Function: used(Port) -> {ok, Used} | {error, Error}
%% Description: Calls kdb::used for given KDB object..
%%    Port = port
%%    Used = integer
%%--------------------------------------------------------------------
used(Port) ->
		Cmd = encode_command(?USED),
		port_command(Port, Cmd),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: root_iterator(Port) -> {ok, Iter} | {error, Error}
%% Description: Obtains iterator for root table of KDB..
%%    Port = port
%%    Iter = integer
%%--------------------------------------------------------------------
root_iterator(Port) ->
		Cmd = encode_command(?ROOT_BEGIN),
		port_command(Port, Cmd),
		get_reply(Port).

%%--------------------------------------------------------------------
%% Function: get_drive_type(Port, Path) -> local | remote | {error, Error}
%% Description: Returns type of drive: local or remote.
%% A trailing backslash is required on Windows. For example, you
%% specify \\MyServer\MyShare as "\\MyServer\MyShare\", or the C drive as "C:\".
%%    Port = port
%%    Path = string
%%--------------------------------------------------------------------
get_drive_type(Port, Path) ->
		Cmd = encode_command(?GET_DRIVE_TYPE),
		Packet = encode(Path, <<Cmd/binary>>),
		port_command(Port, Packet),
		get_reply(Port).

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% encodes command into binary form
%%--------------------------------------------------------------------
encode_command(Cmd) ->
		<<Cmd:32/unsigned-native>>.

%%--------------------------------------------------------------------
%% encodes term into string binary form
%%--------------------------------------------------------------------
encode(X, Bin) ->
		B = list_to_binary(term_to_string(X)),
		S = size(B),
		<<Bin/binary, S:32/unsigned-native, B/binary>>.

%%--------------------------------------------------------------------
%% encodes list of {key, Value} pairs into binary string
%%--------------------------------------------------------------------
encode_options([], Bin) ->
		Bin;
encode_options([{K, V} | T], Bin) ->
		encode_options(T, encode(V, encode(K, Bin))).

%%--------------------------------------------------------------------
%% translates term to string
%%--------------------------------------------------------------------
term_to_string(X) when is_atom(X) ->
		atom_to_list(X);
term_to_string(X) when is_integer(X) ->
		integer_to_list(X);
term_to_string(X) when is_list(X) ->
		case catch list_to_binary(X) of
				{'EXIT', _} ->
						%% list of atoms
						list_to_string(X);
				_ ->
						%% list is string
						X
		end.

%%--------------------------------------------------------------------
%% translates list to string
%%--------------------------------------------------------------------
list_to_string(X) ->
		%% 91 is '[' and 93 is ']'
		list_to_string(X, [91]).

list_to_string([], Res) ->
		[Res, 93];
list_to_string([H], Res) ->
		S = term_to_string(H),
		[Res, S, 93];
list_to_string([H | T], Res) ->
		S = term_to_string(H),
		list_to_string(T, [Res, S, $,]).

%%--------------------------------------------------------------------
%% decodes driver reply
%%--------------------------------------------------------------------
get_reply(Port) ->
		get_reply(Port, 5000).

get_reply(Port, Timeout) ->
		receive
				{Port, {data, Data}} ->
						binary_to_term(Data)
		after Timeout ->
						exit(timeout)
		end.

