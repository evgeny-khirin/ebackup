%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%% Trace macro
-define(TRACE(Fmt, Args),
				io:format("~p[~p]: ~s~n",
									[?MODULE, ?LINE, io_lib:format(Fmt, Args)])).

%% Log info macro
-define(INFO(Fmt, Args),
				error_logger:info_msg("~p[~p]: ~s~n",
															[?MODULE, ?LINE, io_lib:format(Fmt, Args)])).

%% Log warning macro
-define(WARN(Fmt, Args),
				error_logger:warning_msg("~p[~p]: ~s~n",
																 [?MODULE, ?LINE, io_lib:format(Fmt, Args)])).

%% Log error macro
-define(ERROR(Fmt, Args),
				error_logger:error_msg("~p[~p]: ~s~n",
															 [?MODULE, ?LINE, io_lib:format(Fmt, Args)])).

%% Makes transaction name
-define(TX_NAME(Name), {?MODULE, ?LINE, Name}).
