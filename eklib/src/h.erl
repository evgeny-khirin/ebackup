%%%-------------------------------------------------------------------
%%% Copyright (c) 2009-2010 by Evgeny Khirin.
%%%-------------------------------------------------------------------
%%%-------------------------------------------------------------------
%%% File    : html.erl
%%% Author  : Evgeny Khirin <>
%%% Description :
%%%
%%% Created :  9 Apr 2009 by Evgeny Khirin <>
%%%-------------------------------------------------------------------
-module(h).

%% API
-compile([export_all]).

%%====================================================================
%% Internal functions
%%====================================================================
%%--------------------------------------------------------------------
%% Translates attributes
%%--------------------------------------------------------------------
trans_attrs(Attrs) ->
		trans_attrs(Attrs, []).

trans_attrs([], Str) ->
		lists:reverse(Str);
trans_attrs([{Key, Value} | T], Str) ->
		trans_attrs(T, [trans_single_attr(Key, Value) | Str]);
trans_attrs([H|T], Str) ->
		trans_attrs(T, [[" ", term_to_list(H)] | Str]).

trans_single_attr(Key, Value) ->
		[" ", term_to_list(Key), "=\"", escape_attr_value(term_to_list(Value)), "\""].

%%--------------------------------------------------------------------
%% Translates elang term to list
%%--------------------------------------------------------------------
term_to_list(Term) when is_list(Term) ->
		Term;
term_to_list(Term) when is_atom(Term) ->
		atom_to_list(Term);
term_to_list(Term) when is_integer(Term) ->
		integer_to_list(Term).

%%--------------------------------------------------------------------
%% Builds tag from atom attributes and text
%%--------------------------------------------------------------------
tag(Tag, Txt) ->
		[$<, term_to_list(Tag), $>, term_to_list(Txt), "</", term_to_list(Tag), $>].
tag(Tag, Attrs, Txt) ->
		[$<, term_to_list(Tag), trans_attrs(Attrs), $>, term_to_list(Txt), "</", term_to_list(Tag), $>].

%%--------------------------------------------------------------------
%% Opens tag
%%--------------------------------------------------------------------
open_tag(Tag) ->
		[$<, term_to_list(Tag), $>].
open_tag(Tag, Attrs) ->
		[$<, term_to_list(Tag), trans_attrs(Attrs), $>].

%%--------------------------------------------------------------------
%% Closes tag
%%--------------------------------------------------------------------
close_tag(Tag) ->
		["</", term_to_list(Tag), $>].

%%--------------------------------------------------------------------
%% escapes attribute value
%%--------------------------------------------------------------------
escape_attr_value(V) ->
		escape_attr_value(V, []).

escape_attr_value([$" | T], R) ->
		escape_attr_value(T, ["&quot;" | R]);
escape_attr_value([H | T], R) ->
		escape_attr_value(T, [H | R]);
escape_attr_value([], R) ->
		lists:reverse(R).

%%====================================================================
%% HTML
%%====================================================================
html(T) ->
		["<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">",
		 tag(html, T)].

html_open() ->
		["<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">",
		 open_tag(html)].
html_open(T) ->
		["<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">",
		 open_tag(html), T].

html_close() ->
		close_tag(html).
html_close(T) ->
		[T, close_tag(html)].

%%====================================================================
%% HEAD
%%====================================================================
head(T) ->
		tag(head, [meta([{"http-equiv", "Content-Type"},
										 {content, "text/html;charset=utf-8"}]),
							 T]).

meta(Attrs) ->
		open_tag(meta, Attrs).

title(T) ->
		tag(title, T).

link(Attrs) ->
		open_tag(link, Attrs).

%%====================================================================
%% BODY
%%====================================================================
body(T) ->
		tag(body, T).
body(Attrs, T) ->
		tag(body, Attrs, T).

body_open() ->
		open_tag(body).
body_open(T) ->
		[open_tag(body), T].

body_close() ->
		close_tag(body).
body_close(T) ->
		[T, close_tag(body)].

%%====================================================================
%% FRAMES
%%====================================================================
frameset(T) ->
		tag(frameset, T).
frameset(Attrs, T) ->
		tag(frameset, Attrs, T).

frame(Attrs) ->
		open_tag(frame, Attrs).

noframes(T) ->
		tag(noframes, T).

%%====================================================================
%% TEXT
%%====================================================================
p(T) ->
		tag(p, T).
p(Attrs, T) ->
		tag(p, Attrs, T).

h1(T) ->
		tag(h1, T).
h1(Attrs, T) ->
		tag(h1, Attrs, T).

h2(T) ->
		tag(h2, T).
h2(Attrs, T) ->
		tag(h2, Attrs, T).

h3(T) ->
		tag(h3, T).
h3(Attrs, T) ->
		tag(h3, Attrs, T).

h4(T) ->
		tag(h4, T).
h4(Attrs, T) ->
		tag(h4, Attrs, T).

h5(T) ->
		tag(h5, T).
h5(Attrs, T) ->
		tag(h5, Attrs, T).

h6(T) ->
		tag(h6, T).
h6(Attrs, T) ->
		tag(h6, Attrs, T).

a(Attrs, T) ->
		tag(a, Attrs, T).

pre(T) ->
		tag(pre, T).
pre(Attrs, T) ->
		tag(pre, Attrs, T).

div_(T) ->
		tag('div', T).
div_(Atrrs, T) ->
		tag('div', Atrrs, T).
div_open(Attrs, T) ->
		[open_tag('div', Attrs), T].
div_close() ->
		close_tag('div').
div_close(T) ->
		[T, close_tag('div')].

span(T) ->
		tag(span, T).
span(Atrrs, T) ->
		tag(span, Atrrs, T).

strong(T) ->
		tag(strong, T).
strong(Atrrs, T) ->
		tag(strong, Atrrs, T).

code(T) ->
		tag(code, T).
code(Atrrs, T) ->
		tag(code, Atrrs, T).

%%====================================================================
%% LISTS
%%====================================================================
ul(T) ->
		tag(ul, T).
ul(Atrrs, T) ->
		tag(ul, Atrrs, T).

ol(T) ->
		tag(ol, T).
ol(Atrrs, T) ->
		tag(ol, Atrrs, T).

dl(T) ->
		tag(dl, T).
dl(Atrrs, T) ->
		tag(dl, Atrrs, T).

dt(T) ->
		tag(dt, T).
dt(Atrrs, T) ->
		tag(dt, Atrrs, T).

dd(T) ->
		tag(dd, T).
dd(Atrrs, T) ->
		tag(dd, Atrrs, T).

li(T) ->
		tag(li, T).
li(Attrs, T) ->
		tag(li, Attrs, T).

%%====================================================================
%% FORMS
%%====================================================================
form(Atrrs, T) ->
		tag(form, Atrrs, T).

input(Attrs) ->
		open_tag(input, Attrs).

button(Attrs, T) ->
		tag(button, Attrs, T).

select(Attrs, T) ->
		tag(select, Attrs, T).

optgroup(Attrs, T) ->
		tag(optgroup, Attrs, T).

option(T) ->
		tag(option, T).
option(Attrs, T) ->
		tag(option, Attrs, T).

textarea(Attrs, T) ->
		tag(textarea, Attrs, T).

label(Attrs, T) ->
		tag(label, Attrs, T).

fieldset(T) ->
		tag(fieldset, T).
fieldset(Attrs, T) ->
		tag(fieldset, Attrs, T).
fieldset_open(T) ->
		[open_tag(fieldset), T].
fieldset_close() ->
		close_tag(fieldset).

legend(T) ->
		tag(legend, T).
legend(Attrs, T) ->
		tag(legend, Attrs, T).

%%====================================================================
%% TABLES
%%====================================================================
table(T) ->
		tag(table, T).
table(Attrs, T) ->
		tag(table, Attrs, T).
table_open() ->
		open_tag(table).
table_close() ->
		close_tag(table).

caption(T) ->
		tag(caption, T).
caption(Attrs, T) ->
		tag(caption, Attrs, T).

thead(T) ->
		tag(thead, T).
thead(Attrs, T) ->
		tag(thead, Attrs, T).

tfoot(T) ->
		tag(tfoot, T).
tfoot(Attrs, T) ->
		tag(tfoot, Attrs, T).

tbody(T) ->
		tag(tbody, T).
tbody(Attrs, T) ->
		tag(tbody, Attrs, T).

colgroup(T) ->
		tag(colgroup, T).
colgroup(Attrs, T) ->
		tag(colgroup, Attrs, T).

col(Attrs) ->
		open_tag(col, Attrs).

tr(T) ->
		tag(tr, T).
tr(Attrs, T) ->
		tag(tr, Attrs, T).

th(T) ->
		tag(th, T).
th(Attrs, T) ->
		tag(th, Attrs, T).

td(T) ->
		tag(td, T).
td(Attrs, T) ->
		tag(td, Attrs, T).

%%====================================================================
%% Objects, images, applets
%%====================================================================
img(Attrs) ->
		open_tag(img, Attrs).
