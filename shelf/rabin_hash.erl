%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : rabin_hash.erl
%%% Author  : Evgeny Khirin <>
%%% Description :
%%%
%%% Created : 21 Jan 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(rabin_hash).

-include("eb.hrl").

%% API
-export([init/2, update/4,
				 calc_bm/1, calc_hash/2,
				 test/0]).

%%====================================================================
%% Rabin rolling hash constants
%%====================================================================
%% Largest 55-bits prime.
%% Use largest prime such that Q * B will not exeed 64-bits.
-define(Q, 36028797018963913). %% 2**55 - 55

%% first prime larger than 256.
-define(B, 257).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(S, M) -> {ok, H, BM} | {error, Reason}
%% Returns:
%%   H - hash value does not exeed 64 bits.
%%   BM - constant (B**(m-1) mod Q), which must be passed to subsequent
%%        calls of update(). Does not exeed 64 bits.
%% Description: Calculates Rabin rolling 64-bits hash for binary string
%% S of length M. Later resulting hash can be updated with update().
%% Hash for binary s of length m is Hash = H % Q, where H is following polynom:
%% H = s[0] * B**(m - 1) + s[1] * B**(m - 2) + … + s[m - 2] * B**1 + s[m - 1] * B**0.
%%
%% Use this function with pattern, because it calculates BM depending
%% on pattern length. When BM is calculated than calc_hash may be
%% used only.
%%
%% If all patterns has same length (binary diff) than calc_bm() may
%% be called once only. And than use calc_hash(). It is not
%% necessary to use init() in that case at all.
%%--------------------------------------------------------------------
init(S, M) when M > 0 ->
		case calc_hash(S, M) of
				{ok, H} ->
						BM = calc_bm(M),
						{ok, H, BM};
				Error ->
						Error
		end.

%%--------------------------------------------------------------------
%% Function: calc_bm(M) -> BM
%% Description: Calculates BM coeffecient of Rabin rolling hash for
%% pattern of length M. BM coeffecient used in update() function.
%%--------------------------------------------------------------------
calc_bm(M) ->
		calc_bm(M - 1, 1).
calc_bm(0, BM) ->
		BM;
calc_bm(M, BM) ->
		calc_bm(M - 1, (BM * ?B) rem ?Q).

%%--------------------------------------------------------------------
%% Function: calc_hash(S, M) -> {ok, H} | {error, Reason}
%% Description: Calculates Rabin hash for binary string S of length M.
%%--------------------------------------------------------------------
calc_hash(S, M) ->
		calc_hash(S, M, 0).
%% size of S is greater than M
calc_hash(_S, 0, H) ->
		{ok, H};
%% size of S is not enough
calc_hash(<<>>, _M, _H) ->
		{error, m_greater_than_s};
calc_hash(<<C, T/binary>>, M, H) ->
		calc_hash(T, M - 1, (H * ?B + C) rem ?Q).

%%--------------------------------------------------------------------
%% Function: update(H, BM, Out, In) -> NewH.
%% Returns: new hash.
%% Parameters:
%%   H - current hash
%%   BM - as returned by rabin_init
%%   Out - byte outging from sliding window
%%   In - byte incoming into sliding window
%% Description: Calculates rolling value of rabin hash.
%%--------------------------------------------------------------------
update(H, BM, Out, In) ->
	H1 = (H + (?Q * ?B) - Out * BM) rem ?Q,
	(H1 * ?B + In) rem ?Q.

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: test_rabin() -> ok | fail
%% Description: tests Rabin rolling hash
%%--------------------------------------------------------------------
-define(MAX_TEST_PATTERN_LEN, 128).

test() ->
		crypto:start(),
		test(?MAX_TEST_PATTERN_LEN).

test(0) ->
		ok;
test(PatternLen) ->
		?TRACE("Pattern length = ~p", [PatternLen]),
		S = crypto:rand_bytes(?MAX_TEST_PATTERN_LEN * ?MAX_TEST_PATTERN_LEN),
		{ok, H, BM} = init(S, PatternLen),
		test(H, BM, S, PatternLen),
		test(PatternLen - 1).
test(_H, _BM, S, PatternLen) when size(S) =< PatternLen ->
		ok;
test(H, BM, <<Out, T/binary>> = S, PatternLen) ->
		{_, <<In, _/binary>>} = split_binary(S, PatternLen),
		NewH = update(H, BM, Out, In),
		{ok, NewH} = calc_hash(T, PatternLen),
		test(NewH, BM, T, PatternLen).
