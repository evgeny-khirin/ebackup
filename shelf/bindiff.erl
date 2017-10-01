%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : bindiff.erl
%%% Author  : Evgeny Khirin <>
%%% Description :
%%%
%%% Created : 25 Jan 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(bindiff).

-include("eb.hrl").

%% API
-export([create_patch/3]).

%%====================================================================
%% Block size
%%====================================================================
-define(BLOCK_SIZE, 4096).

%%====================================================================
%% Patch versions
%%====================================================================
-define(PATCH_VERSION_1, 1).
-define(CURR_PATCH_VERSION, ?PATCH_VERSION_1).

%%====================================================================
%% Patch documemntation.
%% Patch file can hold following commands:
%%   {version, PatchInfoVersion} - this first command in patch file
%%      and stores version of patch info.
%%   {insert, NumOfBytes} - apply_patch function will insert NumOfBytes
%%      from patch file into new file.
%%   {copy, Offset, NumOfBytes} - apply_patch function will copy
%%      starting from Offset NumOfBytes of old file into new file.
%%====================================================================

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: create_patch(OldFile, NewFile, PatchFile) -> ok | {error, Reason}
%% Description: Creates patch file such that function apply_patch(OldFile, PatchFile)
%% will result in NewFile.
%%--------------------------------------------------------------------
create_patch(OldFile, NewFile, PatchFile) ->
		{ok, PatchDevice} = file:open(PatchFile, [write, raw, binary]),
		ok = eb_lib:write_term(PatchDevice, {version, ?CURR_PATCH_VERSION}),
		case eb_lib:fs_object_exist(OldFile) of
				false ->
						{ok, FileInfo} = file:read_file_info(NewFile),
						Size = FileInfo#file_info.size,
						ok = eb_lib:write_term(PatchDevice, {insert, size}),
						{ok, Size} = file:copy(NewFile, PatchDevice);
				true ->
						{ok, SignTable} = sign_file(OldFile),
						ok
		end,
		file:close(PatchDevice).


%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: sign_file(FileName) -> {ok, SignTable} | {error, Reason}
%% Description: Produces signature for FileName. Signatures are ets table.
%%--------------------------------------------------------------------
sign_file(FileName) ->
		{ok, FileDevice} = file:open(FileName, [read, raw, binary]),
		SignTable = ets:new(sign_tables, []),
		{ok, FileInfo} = file:read_file_info(FileName),
		Size = FileInfo#file_info.size,
		ok = sign_file(FileDevice, SignTable, Size, 0),
		{ok, SignTable}.

sign_file(_File, _Table, 0, _Offset) ->
		ok;
sign_file(File, Table, Remain, Offset) when Remain >= ?BLOCK_SIZE ->
		sign_block(File, Table, ?BLOCK_SIZE, Offset),
		sign_file(File, Table, Remain - ?BLOCK_SIZE, Offset + ?BLOCK_SIZE);
sign_file(File, Table, Remain, Offset) ->
		sign_block(File, Table, Remain, Offset).

sign_block(File, Table, BlockSize, Offset) ->
		{ok, Data} = file:read(File, BlockSize),
		{ok, Hash} = rabin_hash:calc_hash(Data, BlockSize),
		case ets:lookup(Table, Hash) of
				[] ->
						ets:insert(Table, {Hash, [{Offset, BlockSize}]});
				[{Hash, BlocksList}] ->
						ets:insert(Table, {Hash, [{Offset, BlockSize} | BlocksList]})
		end,
		ok.
