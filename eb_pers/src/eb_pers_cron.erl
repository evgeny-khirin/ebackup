%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eb_pers_cron.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Simple crontab implementation.
%%%
%%% Created :  5 Feb 2010 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(eb_pers_cron).

-include("eklib/include/debug.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0,
				 add_job/2,
				 delete_job/1,
				 add_temp_job/2,
				 delete_temp_job/1,
				 list/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
		supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Function: add_job(Id, Job) -> ok | {error,Error}
%% Description: Adds permanent job to crontab. If job with such Id is already
%% presented, than running job is terminated, its definition is updated and job
%% is restarted. Permanent job is stored in config DB and started every time
%% when crontab is started.
%%    Id = term.
%%    Job = {Scheduling, TimeSpecs, FunSpecs}
%%    Scheduling = daily | hourly | periodic. Scheduling type.
%%    FunSpecs = {Module, Fun, Args}
%%    TimeSpecs = depending on scheduling type:
%%       daily = {Hour, Minute, Second} - specifies time of job invoking once a
%%          day.
%%       hourly = {Interval, Minute, Second},
%%          where Interval = 1 | 2 | 3 | 4 | 6 | 8 | 12. Invokes job every
%%          Interval hour at Minute:Second. First hour is calculated from midnight.
%%       periodic = integer. Invokes job every X miliseconds, starting from moment
%%          when job is added. If job takes more time than interval, than next
%%          scheduling time will be adjusted on interval boundary.
%%--------------------------------------------------------------------
add_job(Id, Job) ->
		{_Scheduling, _TimeSpecs, FunSpecs} = Job,
		%% make sure that FunSpecs are correct
		{_Module, _Fun, _Args} = FunSpecs,
		ok = start_child({permanent, Id}, Job),
		ok = eb_pers_conf:set_value(?MODULE, Id, Job).

%%--------------------------------------------------------------------
%% Function: delete_job(Id) -> ok | {error,Error}
%% Description: Deletes permanent job from crontab.
%%    Id = term.
%%--------------------------------------------------------------------
delete_job(Id00) ->
		ok = eb_pers_conf:delete(?MODULE, Id00),
		Id10 = {permanent, Id00},
		supervisor:terminate_child(?SERVER, Id10),
		supervisor:delete_child(?SERVER, Id10),
		ok.

%%--------------------------------------------------------------------
%% Function: add_temp_job(Id, Job) -> ok | {error,Error}
%% Description: Adds temporary job to crontab. If job with such Id is already
%% presented, than running job is terminated, its definition is updated and job
%% is restarted. Temporary job is not stored in config DB.
%% All parameters are same as in add_job, except that FunSpecs accepts
%% {Fun, Args} form also. Note, that permanent and temporary jobs use different
%% namespaces.
%%--------------------------------------------------------------------
add_temp_job(Id, Job) ->
		ok = start_child({temporary, Id}, Job).

%%--------------------------------------------------------------------
%% Function: delete_job(Id) -> ok | {error,Error}
%% Description: Deletes permanent job from crontab.
%%    Id = term.
%%--------------------------------------------------------------------
delete_temp_job(Id00) ->
		Id10 = {temporary, Id00},
		supervisor:terminate_child(?SERVER, Id10),
		supervisor:delete_child(?SERVER, Id10),
		ok.

%%--------------------------------------------------------------------
%% Function: list() -> {ok, List} | {error,Error}
%% Description: List jobs currently running in crontab.
%%    List = [Job]
%%    Job = {temporary, Id} | {permanent, Id}
%%    Id = term.
%%--------------------------------------------------------------------
list() ->
		Ids = lists:map(fun(X) -> element(1, X) end,
										supervisor:which_children(?SERVER)),
		List = lists:filter(fun({_,_}) ->
																true;
													 (_) ->
																false
												end,
												Ids),
		{ok, List}.

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
init([]) ->
		Starter =
				{?MODULE,
				 {erlang, apply, [fun() ->
																	Pid = spawn_link(fun() ->
																													 start_config_jobs()
																									 end),
																	{ok, Pid}
													end, []]},
				 transient, brutal_kill, worker, [?MODULE]},
		{ok, {{one_for_one, 100, 1}, [Starter]}}.

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% starts permanent jobs stored in configuration DB
%%--------------------------------------------------------------------
start_config_jobs() ->
		F = fun() ->
								{ok, T} = kdb:open_table(eb_pers_conf, ?MODULE),
								{ok, I} = kdb:iterator(eb_pers_conf, T),
								start_config_jobs(I, kdb:next(eb_pers_conf, I)),
								kdb:close_table(eb_pers_conf, T)
				end,
		{atomic, ok} = kdb:transaction(eb_pers_conf, F),
		ok.

start_config_jobs(I, eof) ->
		kdb:close_iter(eb_pers_conf, I);
start_config_jobs(I, {ok, Id, Job}) ->
		ok = start_child({permanent, Id}, Job),
		start_config_jobs(I, kdb:next(eb_pers_conf, I)).

%%--------------------------------------------------------------------
%% adds job child as permanent process
%%--------------------------------------------------------------------
start_child(Id, Job) ->
		{_Scheduling, _TimeSpecs, _FunSpecs} = Job,
		Child =
				{Id,
				 {erlang, apply, [fun() ->
																	Pid = spawn_link(fun() ->
																													 loop_job(Job)
																									 end),
																	{ok, Pid}
													end, []]},
				 permanent, brutal_kill, worker, [?MODULE]},
		supervisor:terminate_child(?SERVER, Id),
		supervisor:delete_child(?SERVER, Id),
		{ok, _Pid} = supervisor:start_child(?SERVER, Child),
		ok.

%%--------------------------------------------------------------------
%% executes job in loop
%%--------------------------------------------------------------------
loop_job({daily, Time, FunSpecs}) ->
		daily_job(calendar:time_to_seconds(Time), FunSpecs);
loop_job({periodic, MiliSeconds, FunSpecs}) ->
		periodic_job(MiliSeconds, FunSpecs);
loop_job({hourly, Time, FunSpecs}) ->
		hourly_job(Time, FunSpecs).

%%--------------------------------------------------------------------
%% executes job's function
%%--------------------------------------------------------------------
execute({Fun, Args}) ->
		apply(Fun, Args);
execute({Module, Fun, Args}) ->
		apply(Module, Fun, Args).

%%--------------------------------------------------------------------
%% executes daily job. Time is number of seconds from midnight to start the
%% job.
%%--------------------------------------------------------------------
daily_job(Time, FunSpecs) ->
		CurrTime = calendar:time_to_seconds(time()),
		SleepTime =
				if
						Time >= CurrTime ->
								%% job time is after current time
								Time - CurrTime;
						true ->
								%% job time is passed. 86400 is number of seconds in 24 hours:
								%% (24 * 60 * 60).
								86400 + Time - CurrTime
				end,
		timer:sleep(SleepTime * 1000),
		spawn(fun() -> execute(FunSpecs) end),
		%% Sleep a little, in order to avoid starvation.
		timer:sleep(2000),
		daily_job(Time, FunSpecs).

%%--------------------------------------------------------------------
%% executes periodic job every X miliseconds.
%%--------------------------------------------------------------------
periodic_job(Time, FunSpecs) ->
		spawn(fun() -> execute(FunSpecs) end),
		timer:sleep(Time),
		periodic_job(Time, FunSpecs).

%%--------------------------------------------------------------------
%% executes hourly job
%%--------------------------------------------------------------------
hourly_job({HourInterval, Minute, Second}, FunSpecs) ->
		{CurrHour, CurrMinute, CurrSecond} = time(),
		SleepHours =
				if
						CurrHour rem HourInterval =:= 0 ->
								0;
						true ->
								HourInterval - CurrHour rem HourInterval
				end,
		SleepAdjust =
				calendar:time_to_seconds({0, Minute, Second}) -
				calendar:time_to_seconds({0, CurrMinute, CurrSecond}),
		SleepTime00 = SleepHours * 3600 + SleepAdjust,
		SleepTime10 =
				if
						SleepTime00 < 0 ->
								calendar:time_to_seconds({HourInterval, Minute, Second}) -
										calendar:time_to_seconds({0, CurrMinute, CurrSecond});
						true ->
								SleepTime00
				end,
		timer:sleep(SleepTime10 * 1000),
		spawn(fun() -> execute(FunSpecs) end),
		%% Sleep a little, in order to avoid starvation.
		timer:sleep(2000),
		hourly_job({HourInterval, Minute, Second}, FunSpecs).
