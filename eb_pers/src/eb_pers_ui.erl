%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : eb_pers_ui.erl
%%% Author  : Evgeny Khirin <>
%%% Description : Module exports only mod_esi callbacks
%%%
%%% Created :  8 Apr 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(eb_pers_ui).

-include_lib("kernel/include/file.hrl").
-include("eklib/include/debug.hrl").
-include("eb_pers/src/eb_pers_bckp.hrl").

%% mod_esi callbacks
%% Do not export any functions except one that should be accessed from web.
%% Otherwise, it will produce security holes. For example, if this module
%% would be gen_server process, then everybody from internet call its gen_server
%% callbacks.
-export([home/2,
				 settings/2,
				 update_run_mode/2,
				 update_scheduling/2,
				 update_sources/2,
				 update_excludes/2,
				 update_backup/2,
				 update_destination/2,
				 snapshots/3,
				 search/3,
				 print_log/3,
				 list_versions/3,
				 get_file/3,
				 faq/2,
				 restore/2,
				 restore_1/3,
				 about/2
				]).

%%--------------------------------------------------------------------
%% home page
%%--------------------------------------------------------------------
home(_Env, _Input) ->
		settings().

%%--------------------------------------------------------------------
%% settings dispatcher
%%--------------------------------------------------------------------
settings(_Env, _Input) ->
		settings().

settings() ->
		{DailyChecked, HourDaily, MinuteDaily,
		 HourlyChecked, HourHourly, MinuteHourly,
		 ManualChecked} =
				case eb_pers_conf:get_value(eb_pers_cron, backup) of
						not_found ->
								{"", 0, 0,
								 "", 1, 0,
								 checked};
						{ok, {daily, {Hour, Minute, _}, _}} ->
								{checked, Hour, Minute,
								 "", 1, 0,
								 ""};
						{ok, {hourly, {Interval, Minute, _}, _}} ->
								{"", 0, 0,
								 checked, Interval, Minute,
								 ""}
				end,
		Sources = eb_pers_bckp:sources(),
		SrcList =
				if
						Sources =:= [] ->
								h:select([{class, path}, {name, src_list}, multiple, disabled], h:option(""));
						true ->
								h:select([{class, path}, {name, src_list}, multiple],
												 options_list("", false, Sources))
				end,
		SrcWarn =
				if
						Sources =:= [] ->
								h:p(important("Sources are not specified."));
						true ->
								[]
				end,
		Excludes = eb_pers_bckp:excludes(),
		ExclList =
				if
						Excludes =:= [] ->
								h:select([{class, path}, {name, excl_list}, multiple, disabled], h:option(""));
						true ->
								h:select([{class, path}, {name, excl_list}, multiple],
												 options_list("", false, Excludes))
				end,
		Destination =
				case eb_pers_conf:get_value(eb_pers_bckp, destination) of
						not_found ->
								[h:p(important("Destination folder is not specified.")),
								 h:p("It can be folder on any drive: local, network or removable media.")];
						{ok, D} ->
								[h:p(D), h:p(h:input([{type, submit}, {name, submit}, {value, "Delete"}]))]
				end,
		{ok, Snapshots} = eb_pers_conf:get_value(eb_pers_bckp, snapshots, 60),
		{ok, ExecuteBefore} = eb_pers_conf:get_value(eb_pers_bckp, execute_before, ""),
		{ok, ExecuteAfter} = eb_pers_conf:get_value(eb_pers_bckp, execute_after, ""),
		RunModeForm = build_run_mode_form(),
		Text =
				[h:form([{action, update_scheduling},
								 {method, post}],
								h:fieldset([h:legend("Scheduling"),
														h:p([h:input([{type, radio}, DailyChecked, {name, scheduling}, {value, daily}]),
																 " Backup once a day at ",
																 h:select([{name, hour_1}],
																					options_list(HourDaily, true, lists:seq(0, 23))),
																 ":",
																 h:select([{name, minute_1}],
																					options_list(MinuteDaily, true, lists:seq(0, 55, 5)))]),
														h:p([h:input([{type, radio}, HourlyChecked, {name, scheduling}, {value, hourly}]),
																 " Backup every ",
																 h:select([{name, hour_2}],
																					options_list(HourHourly, false, [1,2,3,4,6,8,12])),
																 " hour(s) at ",
																 h:select([{name, minute_2}],
																					options_list(MinuteHourly, true, lists:seq(0, 55, 5))),
																 " minutes"]),
														h:p([h:input([{type, radio}, ManualChecked, {name, scheduling}, {value, manual}]),
																 " Disable scheduling"]),
														h:p([h:input([{type, submit}, {name, submit}, {value, "Update"}]),
																 " ",
																 h:input([{type, submit}, {name, submit}, {value, "Backup now"}])])])),
				 h:form([{action, update_destination},
								 {method, post}],
								 h:fieldset([h:legend("Destination folder"),
														 Destination,
														 h:p(["Enter full path: ",
																	h:input([{class, path}, {type, text}, {name, destination}])]),
														 h:p(h:input([{type, submit}, {name, submit}, {value, "Update"}]))])),
				 h:form([{action, update_sources},
								 {method, post}],
								h:fieldset([h:legend("Sources list"),
														SrcWarn,
														h:p("Backup source folders and files:"),
														h:p(SrcList),
														h:p(h:input([{type, submit}, {name, submit}, {value, "Delete selected"}])),
														h:p(["Enter full path: ",
																h:input([{class, path}, {type, text}, {name, source}])]),
														h:p(h:input([{type, submit}, {name, submit}, {value, "Add to list"}]))])),
				 h:form([{action, update_excludes},
								 {method, post}],
								 h:fieldset([h:legend("Exclude list"),
														 h:p("Exclude following folders and files from sources:"),
														 h:p(ExclList),
														 h:p(h:input([{type, submit}, {name, submit}, {value, "Delete selected"}])),
														 h:p(["Enter full path: ",
																 h:input([{class, path}, {type, text}, {name, exclude}])]),
														 h:p(h:input([{type, submit}, {name, submit}, {value, "Add to list"}]))])),
				 h:form([{action, update_backup},
								 {method, post}],
								h:fieldset([h:legend("Miscellaneous"),
														h:p(["Number of backup snapshots: ",
																 h:select([{name, snapshots}],
																					options_list(Snapshots, false, [1, 7, 10, 30, 60, 90, 180, 365, "unlimited"]))]),
														h:p(["Execute command before backup: ",
																 h:input([{class, command}, {type, text}, {name, execute_before}, {value, ExecuteBefore}])]),
														h:p(["Execute command after backup: ",
																 h:input([{class, command}, {type, text}, {name, execute_after}, {value, ExecuteAfter}])]),
														h:p(h:input([{type, submit}, {name, submit}, {value, "Update"}]))])),
				 RunModeForm],
		page('Settings', Text).

%%--------------------------------------------------------------------
%% update_run_mode/2
%%--------------------------------------------------------------------
update_run_mode(_Env, Input) ->
		Query = httpd:parse_query(Input),
		Mode = list_to_existing_atom(proplists:get_value("mode", Query)),
		update_run_mode_1(eb_pers:get_run_mode(), Mode).

update_run_mode_1(Mode, Mode) ->
		settings();
update_run_mode_1(_, win_service) ->
		eb_pers:set_run_mode(win_service),
		spawn(fun() ->
									timer:sleep(100),
									restart_in_new_mode(win_service)
					end),
		settings();
update_run_mode_1(_, win_user) ->
		spawn(fun() ->
									timer:sleep(100),
									restart_in_new_mode(win_user)
					end),
		page('Settings',
				 [h:p(important("Erlios Backup service will be stopped within 30 seconds.")),
					h:p(["You need to logoff current Windows session and login again, ",
							 "in order to restart Erlios Backup under your login."])]).

restart_in_new_mode(win_service) ->
		file:set_cwd(code:root_dir()),
		os:cmd("eb_pers_win ++ -restart_in_service_mode");
restart_in_new_mode(win_user) ->
		file:set_cwd(code:root_dir()),
		os:cmd("eb_pers_win ++ -restart_in_user_mode").

%%--------------------------------------------------------------------
%% update_scheduling/2
%%--------------------------------------------------------------------
update_scheduling(_Env, Input) ->
		Query = httpd:parse_query(Input),
		case proplists:get_value("scheduling", Query) of
				"daily" ->
						Hour = str_to_num(proplists:get_value("hour_1", Query)),
						Minute = str_to_num(proplists:get_value("minute_1", Query)),
						ok = eb_pers_cron:add_job(backup,
																 {daily, {Hour, Minute, 0}, {eb_pers_bckp, backup, []}});
 				"hourly" ->
						Hour = str_to_num(proplists:get_value("hour_2", Query)),
						Minute = str_to_num(proplists:get_value("minute_2", Query)),
						ok = eb_pers_cron:add_job(backup,
																 {hourly, {Hour, Minute, 0}, {eb_pers_bckp, backup, []}});
 				"manual" ->
						ok = eb_pers_cron:delete_job(backup)
		end,
		case proplists:get_value("submit", Query) of
				"Backup now" ->
						spawn(fun() ->
													eb_pers_bckp:backup()
									end);
				_ ->
						ok
		end,
		settings().

%%--------------------------------------------------------------------
%% update_sources/2
%%--------------------------------------------------------------------
update_sources(_Env, Input) ->
		Query = httpd:parse_query(Input),
		case proplists:get_value("submit", Query) of
				"Add to list" ->
						case proplists:get_value("source", Query) of
								[] ->
										settings();
								Path ->
										case eb_pers_bckp:add_source(Path) of
												ok ->
														settings();
												{error, remote_path} ->
														Text = [h:p(important("Source path belongs to remote drive.")),
																		h:p("Backup of remote drives is not supported.")],
														page('Settings', Text);
												{error, not_absolute} ->
														page('Settings', h:p(important("Path must be full.")));
												Other ->
														page('Settings', h:p(important(io_lib:format("Unxepected error: ~p.", [Other]))))
										end
						end;
				"Delete selected" ->
						lists:foreach(fun(Path) ->
																	ok = eb_pers_bckp:delete_source(Path)
													end,
													proplists:get_all_values("src_list", Query)),
						settings()
		end.

%%--------------------------------------------------------------------
%% update_excludes/2
%%--------------------------------------------------------------------
update_excludes(_Env, Input) ->
		Query = httpd:parse_query(Input),
		case proplists:get_value("submit", Query) of
				"Add to list" ->
						case proplists:get_value("exclude", Query) of
								[] ->
										settings();
								Path ->
										case eb_pers_bckp:add_exclude(Path) of
												ok ->
														settings();
												{error, not_absolute} ->
														page('Settings', h:p(important("Path must be full.")));
												Other ->
														page('Settings', h:p(important(io_lib:format("Unxepected error: ~p.", [Other]))))
										end
						end;
				"Delete selected" ->
						lists:foreach(fun(Path) ->
																	ok = eb_pers_bckp:delete_exclude(Path)
													end,
													proplists:get_all_values("excl_list", Query)),
						settings()
		end.

%%--------------------------------------------------------------------
%% update_backup/2
%%--------------------------------------------------------------------
update_backup(_Env, Input) ->
		Query = httpd:parse_query(Input),
		case proplists:get_value("snapshots", Query) of
				"unlimited" ->
						ok = eb_pers_conf:set_value(eb_pers_bckp, snapshots, "unlimited");
				Other ->
						case str_to_num(Other) of
								Snapshots when Snapshots > 0 ->
										ok = eb_pers_conf:set_value(eb_pers_bckp, snapshots, Snapshots);
								_ ->
										ok
						end
		end,
		ExecuteBefore = proplists:get_value("execute_before", Query),
		ok = eb_pers_conf:set_value(eb_pers_bckp, execute_before, ExecuteBefore),
		ExecuteAfter = proplists:get_value("execute_after", Query),
		ok = eb_pers_conf:set_value(eb_pers_bckp, execute_after, ExecuteAfter),
		settings().

%%--------------------------------------------------------------------
%% update_destination/2
%%--------------------------------------------------------------------
update_destination(_Env, Input) ->
		Query = httpd:parse_query(Input),
		case proplists:get_value("submit", Query) of
				"Update" ->
						Destination = proplists:get_value("destination", Query),
						set_destination(filename:nativename(Destination));
				"Delete" ->
						eb_pers_conf:delete(eb_pers_bckp, destination),
						%% restart backup DB
						spawn(fun() ->
													eb_pers_bckp:restart_db()
									end),
						settings()
		end.

set_destination([]) ->
		settings();
set_destination(Path) ->
		set_destination(Path, filename:pathtype(Path)).

set_destination(Path, absolute) ->
		F = fun() ->
								ok = eb_pers_conf:set_value(eb_pers_bckp, destination, Path),
								%% restart backup DB
								case kdb:get_path_type(eb_pers_conf, Path) of
										undefined ->
												exit(invalid_drive);
										_ ->
												ok = eb_pers_bckp:restart_db()
								end
				end,
		case kdb:transaction(eb_pers_conf, F, hard) of
				{atomic, ok} ->
						settings();
				_ ->
						page('Settings', [h:p(important("Failed.")),
															h:p(["Make sure that destination folder is writable. ",
																	"Or check startup mode - remote destinations are not ",
																	"supported by Windows services."])])
		end;
set_destination(_Path, _) ->
		page('Settings', h:p(important("Path must be full."))).

%%--------------------------------------------------------------------
%% snapshots/3
%%--------------------------------------------------------------------
snapshots(SessionID, _Env, _Input) ->
		snapshots(SessionID).

snapshots(SessionID) ->
		ok = mod_esi:deliver(SessionID, page_open('Snapshots')),
		case eb_pers_conf:get_value(eb_pers_bckp, destination) of
				not_found ->
						ok = mod_esi:deliver(SessionID, lists:flatten(h:p(important("Destination folder is not set."))));
				{ok, Destination} ->
						%% Build Search form
						SearchForm =
								h:form([{action, "search"},
												{method, post}],
											 h:fieldset([h:legend("Search file"),
																	 h:p(["Enter file name or directory to search. ",
																				"Wildcard characters, like * and ? are allowed. ",
																				"For example, \"*.doc\", \"C:\\My Documents\\*\" etc. ",
																				"Search is case insensitive (English only)."]),
																	 h:p([h:input([{class, search}, {type, text}, {name, search}]),
																				" ",
																				h:input([{type, submit}, {name, submit}, {value, "Search"}])])])),
						ok = mod_esi:deliver(SessionID, lists:flatten(SearchForm)),
						StorageUsed =
								case catch eb_pers_bckp:storage_used(Destination) of
										{ok, U} ->
												io_lib:format(", storage used ~s", [format_capacity(U)]);
										_ ->
												[]
								end,
						ok = mod_esi:deliver(SessionID,
																 lists:flatten(
																	 h:fieldset_open([h:legend(["Snapshots list", StorageUsed]),
																										h:table_open(),
																										h:tr([h:th("Status"),
																													h:th("Date"),
																													h:th("Time"),
																													h:th("Folders"),
																													h:th("Files"),
																													h:th("Size"),
																													h:th("Errors")
																												 ])]))),
						case catch list_snapshots(SessionID) of
								ok ->
										ok = mod_esi:deliver(SessionID, lists:flatten(h:table_close()));
								_ ->
										ok = mod_esi:deliver(SessionID,
																				 lists:flatten(
																					 [h:table_close(),
																						h:p(important("Failed to read snapshots list.")),
																						h:p(["Make sure that destination folder is ",
																								 "accessible. Or check startup mode - ",
																								 "remote destinations are not supported by ",
																								 "Windows services."])]))
						end,
						ok = mod_esi:deliver(SessionID, lists:flatten(h:fieldset_close()))
		end,
		ok = mod_esi:deliver(SessionID, page_close()).

%%--------------------------------------------------------------------
%% builds list of snapshots
%%--------------------------------------------------------------------
list_snapshots(SessionID) ->
		list_snapshots(SessionID, -16#ffffffffffffffff).

list_snapshots(SessionID, Prev) ->
		F = fun() ->
								{ok, T} = kdb:open_table(eb_pers_bckp, snapshots),
								{ok, I} = kdb:seek(eb_pers_bckp, T, {snap, Prev}, next),
								Res = list_snapshots(SessionID, I, kdb:next(eb_pers_bckp, I), Prev, 128),
								ok = kdb:close_iter(eb_pers_bckp, I),
								ok = kdb:close_table(eb_pers_bckp, T),
								Res
				end,
		case kdb:transaction(eb_pers_bckp, F) of
				{atomic, eof} ->
						ok;
				{atomic, NewPrev} ->
						list_snapshots(SessionID, NewPrev)
		end.

list_snapshots(_SessionID, _I, _, Prev, 0) ->
		Prev;
list_snapshots(SessionID, I, {ok, {snap, Id}, SnapDescr}, _, Count) ->
		#snap_descr{date = SnapDate, time = SnapTime, status = Status,
								dirs = Dirs, files = Files, size = Size, errors = Errors} = SnapDescr,
		{Year, Month, Day} = SnapDate,
		{Hour, Minute, Second} = SnapTime,
		case Status of
				{delete, _} ->
						ok;
				_ ->
						StatusTxt =
								case Status of
										ok ->
												h:img([{src, "/ok.png"}]);
										error ->
												h:img([{src, "/fail.png"}]);
										{{restore, ok}, _Pid} ->
												h:img([{src, "/ok.png"}]);
										{{restore, error}, _Pid} ->
												h:img([{src, "/fail.png"}]);
										_ ->
												h:img([{src, "/question.png"}])
								end,
						Date = io_lib:format("~2.10.0b.~2.10.0b.~4.10.0b",
																 [Day, Month, Year]),
						Time = io_lib:format("~2.10.0b:~2.10.0b:~2.10.0b",
																 [Hour, Minute, Second]),
						RestoreLink = io_lib:format("restore?id=~p", [-Id]),
						ErrorsTxt =
								if
										Errors > 0 ->
												h:a([{href, io_lib:format("print_log?id=~p", [-Id])}],
														format_big_int(Errors));
										true ->
												"-"
								end,
						Text =
								h:tr([h:td(StatusTxt),
											h:td(Date),
											h:td(Time),
											h:td(format_big_int(Dirs)),
											h:td(format_big_int(Files)),
											h:td(format_capacity(Size)),
											h:td(ErrorsTxt),
											h:td(h:a([{href, RestoreLink}], "Restore"))
										 ]),
						ok = mod_esi:deliver(SessionID, lists:flatten(Text))
		end,
		list_snapshots(SessionID, I, kdb:next(eb_pers_bckp, I), Id, Count - 1);
list_snapshots(_SessionID, _I, _, _Prev, _Count) ->
		eof.

%%--------------------------------------------------------------------
%% search/3
%%--------------------------------------------------------------------
search(SessionID, _Env, Input) ->
		Query = httpd:parse_query(Input),
		Txt = proplists:get_value("search", Query),
		Last = proplists:get_value("last", Query),
		Ref = eb_pers_bckp:notify_cleaner_start_job(false),
		catch search_1(SessionID, Txt, Last),
		eb_pers_bckp:notify_cleaner_end_job(Ref),
		ok.

search_1(SessionID, "", _Last) ->
		snapshots(SessionID);
search_1(SessionID, Txt, Last) ->
		ok = mod_esi:deliver(SessionID, page_open('Snapshots')),
 		{ok, Re} = re:compile(regexp:sh_to_awk(Txt), [caseless]),
		case catch search_1(SessionID, Txt, Re, Last, [], 0) of
				ok ->
						ok;
				_ ->
						ok = mod_esi:deliver(SessionID,
																 lists:flatten(
																	 [h:p(important(["Search failed. Make sure that ",
																									 "destination folder is accessible."])),
																		page_close()]))
		end.

search_1(SessionID, Txt, _Re, _Last, List, 20) ->
		%% Build results table
		TableContent =
				lists:map(fun({Name, Id}) ->
													Link = io_lib:format("list_versions?id=~p", [Id]),
													h:tr(h:td(h:a([{href, Link}], Name)))
									end,
									lists:reverse(List)),
		%% Build next search form
		{NewLast, _} = hd(List),
		NextForm =
				h:form([{action, "search"},
								{method, post}],
							 [h:fieldset([h:legend("Search results"),
														h:table(TableContent)]),
								h:input([{type, hidden}, {name, search}, {value, Txt}]),
								h:input([{type, hidden}, {name, last}, {value, NewLast}]),
								h:table([h:tr(h:td(h:input([{type, submit}, {name, submit}, {value, "Next 20"}])))])]),
		ok = mod_esi:deliver(SessionID, lists:flatten(NextForm)),
		ok = mod_esi:deliver(SessionID, page_close());
search_1(SessionID, Txt, Re, Last00, List00, Length00) ->
		F = fun() ->
								{ok, T} = kdb:open_table(eb_pers_bckp, n2id),
								{ok, I} = kdb:seek(eb_pers_bckp, T, Last00, next),
								Res = search_2(I, Re, kdb:next(eb_pers_bckp, I), List00, Length00, 1024),
								kdb:close_iter(eb_pers_bckp, I),
								kdb:close_table(eb_pers_bckp, T),
								Res
				end,
		KeepAlive = fun() ->
												ok = mod_esi:deliver(SessionID, " ")
								end,
		eb_pers_cron:add_temp_job(search, {periodic, 200, {KeepAlive, []}}),
		{atomic, Res} = kdb:transaction(eb_pers_bckp, F),
		eb_pers_cron:delete_temp_job(search),
		case Res of
				{eof, [], 0, _Last10} ->
						ok = mod_esi:deliver(SessionID,
																 lists:flatten(h:p(important("Not found.")))),
						ok = mod_esi:deliver(SessionID, page_close());
				{eof, List10, _Length10, _Last10} ->
						TableContent =
								lists:map(fun({Name, Id}) ->
																	Link = io_lib:format("list_versions?id=~p", [Id]),
																	h:tr(h:td(h:a([{href, Link}], Name)))
													end,
													List10),
						ok = mod_esi:deliver(SessionID,
																 lists:flatten(
																	 h:fieldset([h:legend("Search results"),
																							 h:table(TableContent)]))),
						ok = mod_esi:deliver(SessionID, page_close());
				{ok, List10, Length10, Last10} ->
						search_1(SessionID, Txt, Re, Last10, List10, Length10)
		end.

search_2(I, Re, {ok, Name, Id}, List00, Found00, Seeks) when is_list(Name) ->
		{List10, Found10} =
				case re:run(Name, Re) of
						{match, _} ->
								{[{Name, Id} | List00], Found00 + 1};
						_ ->
								{List00, Found00}
				end,
		if
				Found10 =:= 20 ->
						{ok, List10, Found10, hd(List10)};
				Seeks =:= 1 ->
						{ok, List10, Found10, Name};
				true ->
						search_2(I, Re, kdb:next(eb_pers_bckp, I), List10, Found10, Seeks - 1)
		end;
search_2(_I, _Re, _, List, Found, _Seeks) ->
		{eof, List, Found, []}.

%%--------------------------------------------------------------------
%% Prints errors
%%--------------------------------------------------------------------
print_log(SessionID, _Env, Input) ->
		Query = httpd:parse_query(Input),
		SnapId = -list_to_integer(proplists:get_value("id", Query)),
		F = fun() ->
								{ok, T} = kdb:open_table(eb_pers_bckp, log),
								{ok, I} = kdb:seek(eb_pers_bckp, T, {SnapId, ""}, next),
								print_log(SessionID, I, SnapId, kdb:next(eb_pers_bckp, I)),
								ok = kdb:close_iter(eb_pers_bckp, I),
								ok = kdb:close_table(eb_pers_bckp, T)
				end,
		ok = mod_esi:deliver(SessionID, page_open('Snapshots')),
		ok = mod_esi:deliver(SessionID,
												 lists:flatten(
													 h:fieldset_open([h:legend("Error log"),
																						h:table_open()]))),
		Ref = eb_pers_bckp:notify_cleaner_start_job(false),
		kdb:transaction(eb_pers_bckp, F),
		eb_pers_bckp:notify_cleaner_end_job(Ref),
		ok = mod_esi:deliver(SessionID,
												 lists:flatten([h:table_close(),
																				h:fieldset_close(),
																				page_close()])).

print_log(SessionID, I, SnapId, {ok, {SnapId, Path}, Error}) ->
		ok = mod_esi:deliver(SessionID,
												 lists:flatten(
													 h:tr([h:td(Path), h:td(Error)]))),
		print_log(SessionID, I, SnapId, kdb:next(eb_pers_bckp, I));
print_log(_SessionID, _I, _SnapId, _) ->
		ok.

%%--------------------------------------------------------------------
%% Lists versions of file
%%--------------------------------------------------------------------
list_versions(SessionID, _Env, Input) ->
		Query = httpd:parse_query(Input),
		Id = list_to_integer(proplists:get_value("id", Query)),
		GetName = fun() ->
											{ok, T} = kdb:open_table(eb_pers_bckp, n2id),
											Res = kdb:lookup(eb_pers_bckp, T, Id),
											kdb:close_table(eb_pers_bckp, T),
											Res
							end,
		{atomic, {ok, FullName}} = kdb:transaction(eb_pers_bckp, GetName),
		BaseName = filename:basename(FullName),
		ListVer = fun() ->
											{ok, T} = kdb:open_table(eb_pers_bckp, ver_info),
											{ok, I} = kdb:seek(eb_pers_bckp, T, {Id, -16#ffffffffffffffff}, next),
											list_versions(SessionID, I, BaseName, Id, kdb:next(eb_pers_bckp, I)),
											ok = kdb:close_iter(eb_pers_bckp, I),
											ok = kdb:close_table(eb_pers_bckp, T)
							end,
		ok = mod_esi:deliver(SessionID, page_open('Snapshots')),
		ok = mod_esi:deliver(SessionID,
												 lists:flatten(
													 h:fieldset_open([h:legend("Available file versions"),
																						h:p(FullName),
																						h:table_open(),
																						h:tr([h:th("Modified"),
																									h:th("Created"),
																									h:th("Size")])]))),
		Ref = eb_pers_bckp:notify_cleaner_start_job(false),
		kdb:transaction(eb_pers_bckp, ListVer),
		eb_pers_bckp:notify_cleaner_end_job(Ref),
		ok = mod_esi:deliver(SessionID,
												 lists:flatten([h:table_close(),
																				h:fieldset_close(),
																				page_close()])).

list_versions(SessionID, I, Name, Id, {ok, {Id, Ver}, VerInfo}) ->
		FileInfo = eb_pers_bckp:ver_info_to_file_info(VerInfo),
		#file_info{size = Size, type = Type, mtime = MTime, ctime = CTime} = FileInfo,
		Status = eb_pers_bckp:ver_info_status(VerInfo),
		if
				Status =:= ok andalso Type =:= regular ->
						{{MYear, MMonth, MDay}, {MHour, MMinute, MSecond}} = MTime,
						MDateTime = io_lib:format("~2.10.0b.~2.10.0b.~4.10.0b, ~2.10.0b:~2.10.0b:~2.10.0b",
																			[MDay, MMonth, MYear, MHour, MMinute, MSecond]),
						{{CYear, CMonth, CDay}, {CHour, CMinute, CSecond}} = CTime,
						CDateTime = io_lib:format("~2.10.0b.~2.10.0b.~4.10.0b, ~2.10.0b:~2.10.0b:~2.10.0b",
																			[CDay, CMonth, CYear, CHour, CMinute, CSecond]),
						GetLink = io_lib:format("get_file?id=~p&ver=~p&name=~s&size=~p",
																		[Id, -Ver, eklib:url_encode(Name), Size]),
						ok = mod_esi:deliver(SessionID,
																 lists:flatten(
																	 h:tr([h:td(MDateTime),
																				 h:td(CDateTime),
																				 h:td(format_big_int(Size)),
																				 h:td(h:a([{href, GetLink}], "Download"))])));
				true ->
						ok
		end,
		list_versions(SessionID, I, Name, Id, kdb:next(eb_pers_bckp, I));
list_versions(_SessionID, _I, _Name, _Id, _) ->
		ok.

%%--------------------------------------------------------------------
%% download file
%%--------------------------------------------------------------------
get_file(SessionID, _Env, Input) ->
		Query = httpd:parse_query(Input),
		Id = list_to_integer(proplists:get_value("id", Query)),
		Ver = -list_to_integer(proplists:get_value("ver", Query)),
		Name = proplists:get_value("name", Query),
		Size = proplists:get_value("size", Query),
		ok = mod_esi:deliver(SessionID,
												 lists:flatten(
													 ["Content-Type: application/octet-stream\r\n",
														"Content-Length: ", Size, "\r\n",
														"Content-Disposition: attachment; filename=", Name,
														"\r\n\r\n"])),
		Ref = eb_pers_bckp:notify_cleaner_start_job(false),
		File = (catch eb_pers_bckp:open_file(Id, Ver)),
		catch get_file_1(SessionID, File, 0, eb_pers_bckp:read_file_block(File, 0)),
		eb_pers_bckp:notify_cleaner_end_job(Ref),
		ok.

get_file_1(SessionID, File, N, {ok, Data}) ->
		ok = mod_esi:deliver(SessionID, [Data]),
		timer:sleep(10),
		get_file_1(SessionID, File, N + 1, eb_pers_bckp:read_file_block(File, N + 1));
get_file_1(_SessionID, File, _N, eof) ->
		eb_pers_bckp:close_file(File).

%%--------------------------------------------------------------------
%% display FAQ
%%--------------------------------------------------------------------
faq(_Env, _Input) ->
		Text =
				[h:h2("Frequently asked questions (FAQ)"),

				 h:fieldset([h:legend("What is Erlios Backup?"),
										 h:p(["Erlios Backup makes reserve copies of your files. ",
													"These copies may be used to restore the original files ",
													"after a data loss event. Erlios Backup stores and presents your ",
													"files as set of snapshots. If some file is modified between backups, ",
													"different snapshots will contain different copies of the file. ",
													"Storage requirements in every backup system are considerable ",
													"and they define cost of your backup system. Erlios Backup ",
													"significantly reduces storage requirements and cost of your ",
													"backup system, using advanced techniques such as incremental backup, ",
													"compression and deduplication."])]),

				 h:fieldset([h:legend("What is snapshot?"),
										 h:p(["Snapshot is a copy of all your files when backup ",
													"is performed. Snapshots allow you restoring ",
													"your system to a selected point of time. You may prefer this "
													"operation in many cases, instead of dealing with individual files."])]),

				 h:fieldset([h:legend("What is deduplication?"),
										 h:p(["Deduplication is a specific form of compression, whereby redundant ",
													"data is eliminated for better storage space utilization."])]),

				 h:fieldset([h:legend("Why deduplication is important for backup?"),
										 h:p(["Consider your e-mail inbox file with thousands of letters and ",
													"100 MB in size. Every day your inbox changes slightly ",
													"because you receive and delete messages. For example, size of ",
													"some inbox file remains the same, but 100 KB of its content had been modified. ",
													"In order to store a new version of this inbox file, you need ",
													"100 MB of additional storage space. With Erlios Backup however, ",
													"you need only 100 KB of it. This is because Erlios Backup ",
													"is smart enough to detect that only 100 KB had been modified. Now you ",
													"decided to keep a monthly history of your inbox file - 30 versions. ",
													"Let's assume that size of the inbox remains constant and average ",
													"daily modification amounts to 100 KB. In order to keep a monthly history without "
													"deduplication you need 3 GB of disk space. However, in order to ",
													"store 30 versions with Erlios Backup, you need only 103 MB."]),
										 h:p(["Another example, you have slightly different or identical files under ",
													"a different path. In this case, Erlios Backup will also detect similarity ",
													"and store specific parts of the files only."])]),

				 h:fieldset([h:legend("How many snapshots do I need?"),
										 h:p(["You need quite a large number of snapshots, in order to make your ",
													"system secure. For example, you configured your backup software ",
													"to store only a single snapshot. Now your computer is attacked by ",
													"malware and some important files are corrupted. If you fail to ",
													"detect the attack in time, the backup software will replace good files ",
													"with the corrupted ones because of a single snapshot policy. Thus, you ",
													"would miss an opportunity to restore your system. Erlios Backup ",
													"uses deduplication technology for best possible storage space utilization ",
													"and allows you to have significantly larger number of snapshots ",
													"compared to other backup solutions."])]),

				 h:fieldset([h:legend("Does Erlios Backup perform compression of stored files?"),
										 h:p("Yes, in addition to deduplication, files are compressed as well.")]),

				 h:fieldset([h:legend("Can I restore single files only?"),
										 h:p(["Yes. Using search capabilities of the snapshots tab, you can ",
													"restore a single file."])]),

				 h:fieldset([h:legend("What are destination disk requirements?"),
										 h:p(["First, the destination disk must have enough free space to store ",
													"a backup database. You can estimate compression ratio of your ",
													"data after first backup. Then you may calculate required space ",
													"based on the compression ratio and rate of your data modification."]),
										 h:p(["Second, Erlios Backup is based on the fault tolerant transactional ",
													"storage engine, which supports rich and complex configuration of ",
													"underlying storage devices. In order to make your life easy, the ",
													"personal edition of Erlios Backup is preconfigured to run on ",
													"top of the regular filesystem. This configuration requires formatting ",
													"of the destination disk with a filesystem that supports ",
													"fault tolerant metadata. NTFS on Windows and ext3 on Linux ",
													"are examples of such filesystems. In this case Erlios Backup ",
													"will be fault tolerant as well."])]),

				 h:fieldset([h:legend("Can I select USB flash drive folder as destination folder?"),
										 h:p(["Yes, if your flash drive is formatted with filesystem that supports ",
													"fault tolerant metadata. NTFS on Windows and ext3 on Linux are ",
													"examples of such filesystems. Usually, new flash drives are formatted ",
													"with the FAT32 filesystem. This filesystem is not fault tolerant and ",
													"you have to re-format the drive with NTFS, for example. ",
													"Please, search internet for \"format flash drive NTFS\" or ",
													"\"format flash drive ext3\" for instructions on how to format your ",
													"flash driver with those filesystems."])]),

				 h:fieldset([h:legend(["I unplugged my destination flash disk while backup was in ",
															 "progress, is my backup still valid?"]),
										 h:p(["Yes, Erlios Backup uses specially developed fault tolerant storage engine. ",
													"So if your flash drive is formatted with recommended filesystem, ",
													"unplugging will just silently terminate the current backup ",
													"session. But the flash disk data will remain consistent."])]),

				 h:fieldset([h:legend("Is personal edition of Erlios Backup secure?"),
										 h:p(["Personal edition assumes that you are the only user of your computer ",
													"and only you have access to the backup destination folder. ",
													"Because of that, there is no annoying password protection and ",
													"encryption. However, it also implies that if ",
													"somebody gains access to your computer, he can explore your ",
													"backed-up files."])]),

				 h:fieldset([h:legend("What is the \"Execute command before backup\" field?"),
										 h:p(["In order to let the backup software do its job, ",
													"your system should be prepared for backup. For example, ",
													"some application must be closed, databases dumped and ",
													"put into read-only mode, e-mail sent etc. ",
													"It is a usual procedure. However, you know better than anybody how to ",
													"prepare your system for backup. You can specify path to your ",
													"command or script and Erlios Backup will run it before every backup."])]),

				 h:fieldset([h:legend("What is the \"Execute command after backup\" field?"),
										 h:p(["It is opposite to \"Execute command before backup\" option and ",
													"allows you to put your system into regular state after the backup completed."])])
				],
		page('FAQ', Text).

%%--------------------------------------------------------------------
%% restore snapshot
%%--------------------------------------------------------------------
restore(_Env, Input) ->
		Query = httpd:parse_query(Input),
		Id = proplists:get_value("id", Query),
		Text =
				h:form([{action, "restore_1"},
								{method, post}],
							 [h:input([{type, hidden}, {name, id}, {value, Id}]),
								h:fieldset([h:legend([h:input([{type, radio}, checked, {name, location}, {value, new}]),
																			" Restore snapshot under new folder"]),
														h:p(["All snapshot files and directories will be restored under new folder. ",
																 "Then you manually copy necessary files and directories to their original ",
																 "location."]),
																		h:p(["Enter full path to new folder: ",
																 h:input([{class, path}, {type, text}, {name, destination}])])]),
								h:fieldset([h:legend([h:input([{type, radio}, {name, location}, {value, original}]),
																			" Restore snapshot to orginal locations"]),
														h:p(["Restore missing files and directories under their original locations. ",
																 "Existing files will not be overwritten."])]),
								h:input([{type, submit}, {name, submit}, {value, "Restore"}])]),
		page('Snapshots', Text).

restore_1(SessionID, _Env, Input) ->
		Query = httpd:parse_query(Input),
		Id = -list_to_integer(proplists:get_value("id", Query)),
		ok = mod_esi:deliver(SessionID, page_open('Snapshots')),
		Prefix =
				case proplists:get_value("location", Query) of
						"new" ->
								case proplists:get_value("destination", Query) of
										[] ->
												"invalid";
										Other ->
												filename:nativename(Other)
								end;
						"original" ->
								""
				end,
		restore_1(SessionID, Id, Prefix, filename:pathtype(Prefix)).

restore_1(SessionID, Id, Prefix, Type) when Prefix =:= "" orelse Type =:= absolute ->
		ok = mod_esi:deliver(SessionID, lists:flatten(h:p("Please wait..."))),
		KeepAlive = fun() ->
												ok = mod_esi:deliver(SessionID, " ")
								end,
		eb_pers_cron:add_temp_job(restore, {periodic, 200, {KeepAlive, []}}),
		Res = (catch eb_pers_bckp:restore(Id, Prefix)),
		eb_pers_cron:delete_temp_job(restore),
		Text =
				case Res of
						ok ->
								h:p("Done.");
						{error, destination_exists} ->
								h:p(important("Folder already exists."));
						{error, restore_running} ->
								h:p(important("Previous restore operation is not completed."));
						{error, snapshot_busy} ->
								h:p(important("Snapshot is busy."));
						Other ->
								h:p(important(io_lib:format("Error: ~p.", [Other])))
				end,
		ok = mod_esi:deliver(SessionID, lists:flatten([Text, page_close()]));
restore_1(SessionID, _Id, _Prefix, _) ->
		ok = mod_esi:deliver(SessionID,
												 lists:flatten([h:p(important("Path must be full.")),
																				page_close()])).

%%--------------------------------------------------------------------
%% about/2
%%--------------------------------------------------------------------
about(_Env, _Input) ->
		Text = [h:p("Erlios Backup, version 2.5.2, internal release"),
						h:p(["Copyright &copy; 2009 - 2010 by ",
								 h:a([{href, "http://www.erlios.com"}, {target, "_blank"}],
										 "Erlios")]),
						h:p(["Technical support: ",
								 h:a([{href, "mailto:support@erlios.com"}],
										 support@erlios.com)])],
		page('About', Text).

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% Returns standard UI page
%%--------------------------------------------------------------------
page(Selected, Text) ->
		lists:flatten(
			[page_open(Selected),
			 Text,
			 page_close()]).

%%--------------------------------------------------------------------
%% Returns first part of standard UI page
%%--------------------------------------------------------------------
page_open(Selected) ->
		lists:flatten(
			[h:html_open(([h:head([h:title(["Erlios Backup - ",
																			h:term_to_list(Selected)]),
														 h:link([{rel, "stylesheet"},
																		 {type, "text/css"},
																		 {href, "/eb_pers.css"},
																		 {media, "screen,projection"}])]),
										 h:body_open(h:div_open([{id, container}],
																						[header(),
																						 navigation(Selected),
																						 subcontent(Selected),
																						 h:div_open([{id, content}],[])]))]))]).

%%--------------------------------------------------------------------
%% Returns last part of standard UI page
%%--------------------------------------------------------------------
page_close() ->
		lists:flatten([h:div_close(),
									 footer(),
									 h:div_close(),
									 h:body_close(),
									 h:html_close()]).

%%--------------------------------------------------------------------
%% Formats important message in HTML
%%--------------------------------------------------------------------
important(Text) ->
		h:span([{class, important}], Text).

%%--------------------------------------------------------------------
%% Returns standard page header
%%--------------------------------------------------------------------
header() ->
		h:div_([{id, header}],
					 [h:h1(h:a([{href, "http://www.erlios.com"}, {target, "_blank"}],
										 "Protected by Erlios Backup")),
						h:h2("Safety of your data is our priority")]).

%%--------------------------------------------------------------------
%% Returns standard page footer
%%--------------------------------------------------------------------
footer() ->
		h:div_([{id, footer}],
					 h:p(["Copyright &copy; 2009 - 2010 by ",
								h:a([{href, "http://www.erlios.com"}, {target, "_blank"}],
										"Erlios")])).

%%--------------------------------------------------------------------
%% Returns navigation bar
%%--------------------------------------------------------------------
navigation(Selected) ->
		h:ul([{id, navigation}],
				 [nav_item(Selected, 'Settings', settings),
					nav_item(Selected, 'Snapshots', snapshots),
					nav_item(Selected, 'FAQ', faq),
					nav_item(Selected, 'About', about)
				 ]).

nav_item(Name, Name, Href) ->
		h:li([{class, selected}], h:a([{href, Href}], Name));
nav_item(_Selected, Name, Href) ->
		h:li(h:a([{href, Href}], Name)).

%%--------------------------------------------------------------------
%% Builds subcontent panel
%%--------------------------------------------------------------------
subcontent(Selected) ->
		h:div_([{id, subcontent}], sub_menu(Selected)).

%%--------------------------------------------------------------------
%% Builds menu
%%--------------------------------------------------------------------
sub_menu(_Selected) ->
		[].

%%--------------------------------------------------------------------
%% Converts string to number
%%--------------------------------------------------------------------
str_to_num(Str) ->
		Stripped = string:strip(Str),
		case catch list_to_float(Stripped) of
				{_, _} ->
						case catch list_to_integer(Stripped) of
								{_, _} ->
										0;
								N ->
										N
						end;
				N ->
						N
		end.

%%--------------------------------------------------------------------
%% builds list of time options
%%--------------------------------------------------------------------
options_list(Selected, ZeroPrefix, List) ->
		options_list(Selected, ZeroPrefix, List, []).

options_list(_Selected, _ZeroPrefix, [], Res) ->
		lists:reverse(Res);
options_list(Selected, ZeroPrefix, [Selected | T], Res) ->
		options_list(Selected, ZeroPrefix, T,
								 [h:option([selected], option_val(ZeroPrefix, Selected)) | Res]);
options_list(Selected, ZeroPrefix, [H|T], Res) ->
		options_list(Selected, ZeroPrefix, T, [h:option(option_val(ZeroPrefix, H)) | Res]).

option_val(false, X) ->
		X;
option_val(true, X) when X > 9 ->
		X;
option_val(true, X) ->
		io_lib:format("0~p", [X]).

%%--------------------------------------------------------------------
%% builds run mode form
%%--------------------------------------------------------------------
build_run_mode_form() ->
		build_run_mode_form(eb_pers:get_run_mode()).

build_run_mode_form(Mode) when Mode =:= win_service orelse Mode =:= win_user ->
		{ServiceChecked, UserChecked} =
				case Mode of
						win_service ->
								{checked, []};
						win_user ->
								{[], checked}
				end,
		h:form([{action, update_run_mode},
						{method, post}],
					 h:fieldset([h:legend("Startup mode"),
											 h:p([h:input([{type, radio}, UserChecked, {name, mode}, {value, win_user}]),
														" Start automatically on first user login with Administrator rights. ",
														"In this mode Erlios Backup supports remote destination folders. ",
														"But Erlios Backup will be terminated upon user log-off."]),
											 h:p([h:input([{type, radio}, ServiceChecked, {name, mode}, {value, win_service}]),
														" Start automatically as Windows service. In that mode Erlios Backup ",
														"performs backups without any user login. But remote destination folders ",
														"are not supported in this mode. This is limitation of a Windows services."]),
											 h:p(h:input([{type, submit}, {name, submit}, {value, "Update and restart"}]))]));
build_run_mode_form(_) ->
		[].

%%--------------------------------------------------------------------
%% formats capacity in human readabe form
%%--------------------------------------------------------------------
format_capacity(Capacity00) ->
		Sign =
				if
						Capacity00 < 0 ->
								"-";
						true ->
								""
				end,
		{Capacity10, Power} = format_capacity(abs(Capacity00), 0),
		if
				is_float(Capacity10) ->
						io_lib:format("~s~.2f ~s", [Sign, Capacity10, get_capacity_suffix(Power)]);
				true ->
						io_lib:format("~s~p ~s", [Sign, Capacity10, get_capacity_suffix(Power)])
		end.

format_capacity(Capacity, Power) when Capacity < 1024 ->
		{Capacity, Power};
format_capacity(Capacity, Power) ->
		format_capacity(Capacity / 1024, Power + 1).

%% byte
get_capacity_suffix(0) ->
		"bytes";
%% kilobyte
get_capacity_suffix(1) ->
		"KB";
%% megabyte
get_capacity_suffix(2) ->
		"MB";
%% gigabyte
get_capacity_suffix(3) ->
		"GB";
%% terabyte
get_capacity_suffix(4) ->
		"TB";
%% petabyte
get_capacity_suffix(5) ->
		"PB";
%% exabyte
get_capacity_suffix(6) ->
		"EB";
%% zettabyte
get_capacity_suffix(7) ->
		"ZB";
%% yottabyte
get_capacity_suffix(8) ->
		"YB".

%%--------------------------------------------------------------------
%% formats big integer numbers with comma
%%--------------------------------------------------------------------
format_big_int(N) ->
		NN = N div 1000,
		R = N rem 1000,
		format_big_int(NN, [R]).

format_big_int(0, [H | T]) ->
		Res = lists:map(fun(R) ->
														io_lib:format(",~3.10.0b", [R])
										end,
										T),
		[io_lib:format("~b", [H]) | Res];
format_big_int(N, Res) ->
		R = N rem 1000,
		NN = N div 1000,
		format_big_int(NN, [R | Res]).
