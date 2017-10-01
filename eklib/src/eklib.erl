%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eklib.erl
%%% Author  : Evgeny Khirin <>
%%% Description :
%%%
%%% Created : 28 Feb 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(eklib).

-include_lib("kernel/include/file.hrl").
-include("eklib/include/debug.hrl").

%% API
-export([mkdir_ensure/1,
				 fs_object_exist/1,
				 temp_filename/1,
				 walk_fs/4,
				 dss_sign/2,
				 dss_verify/3,
				 ensure_app/1,
				 aes_128_encrypt/2,
				 aes_128_decrypt/2,
				 get_priv_dir/1,
				 now_to_int/1,
				 int_to_now/1,
				 url_encode/1,
				 wait_process_terminated/1,
				 wait_process_terminated/2
				]).

%% Misc macros
-define(UINT32(X), X:32/unsigned-big-integer).

%%====================================================================
%% API
%%====================================================================
%%---------------------------------------------------------------------------
%% mkdir_ensure - builds whole directory hierarchy
%%---------------------------------------------------------------------------
mkdir_ensure(Dir) ->
		filelib:ensure_dir(Dir),
		file:make_dir(Dir),
		ok.

%%---------------------------------------------------------------------------
%% fs_object_exist - checks if there is object with such name in FS
%%---------------------------------------------------------------------------
fs_object_exist(Obj) ->
		case file:read_link_info(Obj) of
				{ok, _} ->
						true;
				{error, _} ->
						case file:read_file_info(Obj) of
								{ok, _} ->
										true;
								{error, _} ->
										false
						end
		end.

%%--------------------------------------------------------------------
%% Function: temp_filename(BaseDir) -> AbsName
%% Description: Generates temporary filename under BaseDir.
%%--------------------------------------------------------------------
temp_filename(BaseDir) ->
		N = random:uniform(1000000),
		BaseName = lists:concat(["tmp_", N, '.']),
		Prefix = filename:absname(filename:join(BaseDir, BaseName)),
		temp_filename(Prefix, 0).

temp_filename(Prefix, Suffix) ->
		Name = lists:concat([Prefix, Suffix]),
		case fs_object_exist(Name) of
				true ->
						temp_filename(Prefix, Suffix + 1);
				false ->
						Name
    end.

%%--------------------------------------------------------------------
%% Function: dss_sign(Term, PrivateKey) -> Signature
%% Description: Signs given term with DSS private key
%%--------------------------------------------------------------------
dss_sign(Term, PrivateKey) ->
		crypto:dss_sign(sized_binary(Term), PrivateKey).

%%--------------------------------------------------------------------
%% Function: dss_verify(Term, Signature, PublicKey) -> ok | {error, Error}
%% Description: Verifies signature of given term with DSS public key
%%--------------------------------------------------------------------
dss_verify(Term, Signature, PublicKey) ->
		Size = size(Signature),
		MpIntSignature = <<?UINT32(Size), Signature/binary>>,
		case crypto:dss_verify(sized_binary(Term), MpIntSignature, PublicKey) of
				true ->
						ok;
				false ->
						{error, signature_verification_failed}
		end.

%%--------------------------------------------------------------------
%% Make sure that application is started
%%--------------------------------------------------------------------
ensure_app(App) ->
		case application:start(App) of
				ok ->
						ok;
				{error,{already_started,App}} ->
						ok
		end.

%%--------------------------------------------------------------------
%% Function: aes_128_encrypt(Term, Key) -> ChipherText
%% Description: Encrypts Term with Key using AES-128 CBC algorithm
%%--------------------------------------------------------------------
aes_128_encrypt(Term, Key) ->
		IVec = crypto:rand_bytes(16),
		SizedBinary = sized_binary(Term),
		%% Make same padding as for 256 bits encryption
		PadLen = (32 - (size(SizedBinary) rem 32)) rem 32,
		Padding = crypto:rand_bytes(PadLen),
		Padded = <<SizedBinary/binary, Padding/binary>>,
		Encrypted = crypto:aes_cbc_128_encrypt(Key, IVec, Padded),
		<<IVec/binary, Encrypted/binary>>.

%%--------------------------------------------------------------------
%% Function: aes_128_decrypt(ChipherText, Key) -> Term
%% Description: Decrypts Term with Key using AES-128 CBC algorithm
%%--------------------------------------------------------------------
aes_128_decrypt(ChipherText, Key) ->
		<<IVec:16/binary, Encrypted/binary>> = ChipherText,
		Decrypted = crypto:aes_cbc_128_decrypt(Key, IVec, Encrypted),
    <<?UINT32(Size), Padded/binary>> = Decrypted,
		{Binary, _Padding} = split_binary(Padded, Size),
		binary_to_term(Binary).

%%--------------------------------------------------------------------
%% Function: get_priv_dir(Module) -> PrivDir
%% Description: Returns path of module's priv directory.
%%--------------------------------------------------------------------
get_priv_dir(Module)->
		{_Module, _Binary, Filename} = code:get_object_code(Module),
		filename:absname_join(filename:dirname(Filename), filename:join("..", "priv")).

%%--------------------------------------------------------------------
%% Function: walk_fs(Sources, Excludes, F, AccIn) -> AccOut
%% Description: Walks files and directories recursively, does not follow symlinks.
%%    Sources = [Name]
%%    Excludes = [Name]
%%    Name = string(). File or directory.
%%    F = fun(Name, Info, AccIn) -> {continue, AccOut} | {skip, AccOut}. If skip is returned,
%%       and item is directory, than everything under this directory is ignored.
%%    Info = {ok, file_info()} | {error, list_dir, Error} | {error, read_file_info, Error}
%%    Error = string(). Error string returned by file:format_error().
%%--------------------------------------------------------------------
walk_fs(Sources00, Excludes00, F, Acc) ->
		Sources10 =
				lists:map(fun(X) ->
													filename:nativename(X)
									end,
									Sources00),
		Excludes10 =
				lists:map(fun(X) ->
													normalize_path(X)
									end,
									Excludes00),
		walk_fs_1(Sources10, Excludes10, F, Acc).

walk_fs_1([], _Excludes, _F, Acc) ->
		Acc;
walk_fs_1([H | T], Excludes, F, Acc) ->
		case lists:member(normalize_path(H), Excludes) of
				true ->
						walk_fs_1(T, Excludes, F, Acc);
				false ->
						Fi =
								case file:read_link_info(H) of
										{ok, Fi1} ->
												{ok, Fi1};
										_ ->
												file:read_file_info(H)
								end,
								walk_fs_1(T, Excludes, F, H, Fi, Acc)
		end.

walk_fs_1(Sources00, Excludes, F, Src, {ok, Fi}, Acc00) ->
		{Acc20, Sources10} =
				case F(Src, Fi, Acc00) of
						{continue, Acc10} ->
								case Fi#file_info.type of
										directory ->
												case file:list_dir(Src) of
														{ok, List} ->
																Sources05 =
																		lists:foldl(fun(X, Sources) ->
																												[filename:nativename(filename:join([Src, X])) | Sources]
																								end,
																								Sources00,
																								List),
																{Acc10, Sources05};
														{error, Error} ->
																{_, Acc15} = F(Src, {error, file:format_error(Error)}, Acc10),
																{Acc15, Sources00}
												end;
										_ ->
												{Acc10, Sources00}
								end;
						{skip, Acc10} ->
								{Acc10, Sources00}
				end,
		walk_fs_1(Sources10, Excludes, F, Acc20);
walk_fs_1(Sources, Excludes, F, Src, {error, Error}, Acc00) ->
		{_, Acc10} = F(Src, {error, file:format_error(Error)}, Acc00),
		walk_fs_1(Sources, Excludes, F, Acc10).

%%--------------------------------------------------------------------
%% normalizes path for walk_fs function
%%--------------------------------------------------------------------
normalize_path(Path) ->
		normalize_path(filename:nativename(Path), os:type()).

normalize_path(Path, {win32, _}) ->
		string:to_lower(Path);
normalize_path(Path, _) ->
		Path.

%%--------------------------------------------------------------------
%% Function: now_to_int({MegaSecs, Secs, MicroSecs}) -> integer
%% Description: Converts tuple returned by now function to integer.
%%--------------------------------------------------------------------
now_to_int({MegaSecs, Secs, MicroSecs}) ->
		MegaSecs * 1000000000000 + Secs * 1000000 + MicroSecs.

%%--------------------------------------------------------------------
%% Function: int_to_now(I) -> {MegaSecs, Secs, MicroSecs}
%% Description: Converts integer to now tuple.
%%--------------------------------------------------------------------
int_to_now(I0) ->
		MicroSecs = I0 rem 1000000,
		I1 = I0 div 1000000,
		Secs = I1 rem 1000000,
		MegaSecs = I1 div 1000000,
		{MegaSecs, Secs, MicroSecs}.


%%--------------------------------------------------------------------
%% Function: url_encode(Str) -> UrlEncodedStr
%% Description: URL-encodes a string based on RFC 1738. Returns a flat list.
%%    Str = string()
%%    UrlEncodedStr = string()
%%--------------------------------------------------------------------
url_encode(Str) ->
    url_encode(lists:reverse(lists:flatten(Str)), []).

url_encode([X | T], Acc) when X >= $0, X =< $9 ->
    url_encode(T, [X | Acc]);
url_encode([X | T], Acc) when X >= $a, X =< $z ->
    url_encode(T, [X | Acc]);
url_encode([X | T], Acc) when X >= $A, X =< $Z ->
    url_encode(T, [X | Acc]);
url_encode([X | T], Acc) when X == $-; X == $_; X == $. ->
    url_encode(T, [X | Acc]);
url_encode([32 | T], Acc) ->
    url_encode(T, [$+ | Acc]);
url_encode([X | T], Acc) ->
    url_encode(T, [$%, d2h(X bsr 4), d2h(X band 16#0f) | Acc]);
url_encode([], Acc) ->
    Acc.

d2h(N) when N < 10 ->
		N + $0;
d2h(N) ->
		N + $a - 10.

%%--------------------------------------------------------------------
%% waits untill specified process is terminated forever
%%--------------------------------------------------------------------
wait_process_terminated(Pid) ->
		Ref = erlang:monitor(process, Pid),
		wait_loop(Ref).

wait_loop(Ref) ->
		receive
				{'DOWN', Ref, _Type, _Object, _Info} ->
						ok
		end.

%%--------------------------------------------------------------------
%% waits untill specified process is terminated with timeout
%%--------------------------------------------------------------------
wait_process_terminated(Pid, Timeout) ->
		Ref = erlang:monitor(process, Pid),
		wait_loop(Ref, Timeout).

wait_loop(Ref, Timeout) ->
		receive
				{'DOWN', Ref, _Type, _Object, _Info} ->
						ok
		after
				Timeout ->
						timeout
		end.

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% Converts term to binary, prefixed with size
%%--------------------------------------------------------------------
sized_binary(Term) ->
		Binary = term_to_binary(Term),
    Size = size(Binary),
    <<?UINT32(Size), Binary/binary>>.

