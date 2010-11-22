% A BSON document is a JSON-like object with a standard binary encoding defined at bsonspec.org. This implements version 1.0 of that spec.
-module (bson).

-export_type ([document/0, label/0, value/0]).
-export_type ([bin/0, bfunction/0, uuid/0, md5/0, userdefined/0]).
-export_type ([mongostamp/0, minmaxkey/0]).
-export_type ([regex/0, unixtime/0]).
-export_type ([javascript/0]).
-export_type ([objectid/0]).

-compile (export_all).

%% Document %%

-type document() :: [label() | value()]. % labels and values must alternate
% A document is a list of label-value pairs (a' la associative array, dictionary, record), but the actual pair constructors are elided, in other words, it's a flattened version of [{label(), value()}] where tuple braces are removed.
% This was done to reduce typing and to distinguish an array from a document which are both lists. Some values are tuples. For example, [{uuid, <<"abc">>}] is an array of uuid(), but if document had tuple fields it would also be a document with field name uuid and value "abc". But with flattening a documents first element is always an atom and an array can never have an atom element because we tagged the bson symbol type, ie. {symbol, atom()}.
-type label() :: atom().

-spec doc_foldl (fun((label(), value(), A) -> A), A, document()) -> A.
doc_foldl (_Fun, Acc, []) -> Acc;
doc_foldl (Fun, Acc, [Label, Value | Doc]) ->
	Acc1 = Fun (Label, Value, Acc),
	doc_foldl (Fun, Acc1, Doc).

-spec doc_foldr (fun((label(), value(), A) -> A), A, document()) -> A.
doc_foldr (_Fun, Acc, []) -> Acc;
doc_foldr (Fun, Acc, [Label, Value | Doc]) -> Fun (Label, Value, doc_foldl (Fun, Acc, Doc)).

-spec fields (document()) -> [{label(), value()}].
% Convert document to a list of all its fields
fields ([]) -> [];
fields ([Label, Value | Document]) -> [{Label, Value} | fields (Document)].

-spec document ([{label(), value()}]) -> document().
% Convert list of fields to a document
document ([]) -> [];
document ([{Label, Value} | Fields]) -> [Label, Value | document (Fields)].

-spec is_document ([_]) -> boolean().
% Bson documents and arrays are both lists but documents have an atom as its first element and arrays cannot contain an atom (atom() is not a valid value() type, but symbol() is). If the list is empty we assume array.
is_document ([]) -> false;
is_document ([Elem | _]) -> is_atom (Elem).

-spec lookup (label(), document()) -> {value()} | {}.
% Value of field in document if there
lookup (_, []) -> {};
lookup (Label, [Key, Value | _]) when Label == Key -> {Value};
lookup (Label, [_, _ | Doc]) -> lookup (Label, Doc).

-spec at (label(), document()) -> value().
% Value of field in document, error if missing
at (Label, Document) -> case lookup (Label, Document) of
	{} -> erlang:error (missing_label, [Label, Document]);
	{Value} -> Value end.

-spec include ([label()], document()) -> document().
% Project given fields of document
include (Labels, Document) ->
	Fun = fun (Label, Doc) -> case lookup (Label, Document) of
		{Value} -> [Label, Value | Doc];
		{} -> Doc end end,
	lists:foldr (Fun, [], Labels).

-spec exclude ([label()], document()) -> document().
% Remove given fields from document
exclude (Labels, Document) ->
	Fun = fun (Label, Value, Doc) -> case lists:member (Label, Labels) of
		false -> [Label, Value | Doc];
		true -> Doc end end,
	doc_foldr (Fun, [], Document).

-spec splitat (label(), document()) -> {document(), value(), document()} | {}.
splitat (_, []) -> {};
splitat (Label, [Key, Value | Doc]) when Label == Key -> {[], Value, Doc};
splitat (Label, [Key, Value | Doc]) -> case splitat (Label, Doc) of
	{Prefix, Match, Suffix} -> {[Key, Value | Prefix], Match, Suffix};
	{} -> {} end.

-spec update (label(), value(), document()) -> document().
% Replace field with new value, adding to end if new
update (Label, Value, Document) -> case splitat (Label, Document) of
	{Prefix, _, Suffix} -> Prefix ++ [Label, Value | Suffix];
	{} -> Document ++ [Label, Value] end.

-spec merge (document(), document()) -> document().
% First doc overrides second with new fields added at end
merge (UpDoc, BaseDoc) ->
	Fun = fun (Label, Value, Doc) -> update (Label, Value, Doc) end,
	doc_foldl (Fun, BaseDoc, UpDoc).

% Value %

-type value() ::
	float() |
	utf8() |
	document() |
	arr() |
	bin() |
	bfunction() |
	uuid() |
	md5() |
	userdefined() |
	objectid() |
	boolean() |
	unixtime() |
	null |
	regex() |
	javascript() |
	symbol() |
	integer() |
	mongostamp() |
	minmaxkey().

% List %

-type arr() :: [value()].
% Caution, a string() will be interpreted as an array of integers. You must encode strings as utf8, see below.

% String %

-type utf8() :: unicode:unicode_binary().
% binary() representing a string of characters encoded with UTF-8.
% An Erlang string() is a list of unicode characters (codepoints), but this list must be converted to utf-8 binary for use in Bson. Call utf8/1 to do this, or encode pure ascii literals directly as <<"abc">> and non-pure ascii literals as <<"aßc"/utf8>>.

-spec utf8 (unicode:chardata()) -> utf8().
% Convert string to utf8 binary. string() is a subtype of unicode:chardata().
utf8 (CharData) -> case unicode:characters_to_binary (CharData) of
	{error, _Bin, _Rest} -> erlang:error (unicode_error, [CharData]);
	{incomplete, _Bin, _Rest} -> erlang:error (unicode_incomplete, [CharData]);
	Bin -> Bin end.

-spec str (unicode:chardata()) -> string().
% Convert utf8 binary to string. utf8() is a subtype of unicode:chardata().
str (CharData) -> case unicode:characters_to_list (CharData) of
	{error, _Bin, _Rest} -> erlang:error (unicode_error, [CharData]);
	{incomplete, _Bin, _Rest} -> erlang:error (unicode_incomplete, [CharData]);
	Str -> Str end.

%% Binary %%

-type bin() :: {bin, binary()}.
-type bfunction() :: {bfunction, binary()}.
-type uuid() :: {uuid, binary()}.
-type md5() :: {md5, binary()}.
-type userdefined() :: {userdefined, binary()}.

%% Special %%

-type symbol() :: {symbol, atom()}.
-type mongostamp() :: {mongostamp, integer()}.
-type minmaxkey() :: minkey | maxkey.

-type regex() :: {regex, utf8(), utf8()}.  % pattern and options

%% Datetime %%

-type unixtime() :: {unixtime, integer()}.
% Unix/POSIX time in milliseconds. Ie. UTC milliseconds since Unix epoch (Jan 1, 1970)

-spec timenow () -> unixtime(). % IO
% Current unixtime
timenow() -> erltime_to_unixtime (os:timestamp()).

-type erltime() :: {integer(), integer(), integer()}. % {MegaSecs, Secs, MicroSecs}
% Unix time in Erlang now/os:timstamp format

-spec erltime_to_unixtime (erltime()) -> unixtime().
erltime_to_unixtime ({MegaSecs, Secs, MicroSecs}) ->
	{unixtime, MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000}.

-spec unixtime_to_erltime (unixtime()) -> erltime().
unixtime_to_erltime ({unixtime, MilliSecs}) ->
	{MilliSecs div 1000000000, (MilliSecs div 1000) rem 1000000, (MilliSecs * 1000) rem 1000000}.

-type datetime() :: {date(), time()}.
-type date() :: {integer(), integer(), integer()}. % {Year, Month, Day}
-type time() :: {integer(), integer(), integer()}. % {Hour, Minute, Second}

-define(UnixEpochGregorianSecs, calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})).

-spec datetime_to_unixtime (datetime()) -> unixtime().
datetime_to_unixtime (DateTime) ->
	{unixtime, (calendar:datetime_to_gregorian_seconds(DateTime) - ?UnixEpochGregorianSecs) * 1000}.

-spec unixtime_to_datetime (unixtime()) -> datetime().
unixtime_to_datetime ({unixtime, MilliSecs}) ->
	calendar:gregorian_seconds_to_datetime(MilliSecs div 1000 + ?UnixEpochGregorianSecs).

%% Javascript %%

-type javascript() :: {javascript, document(), utf8()}.  % scope and code

%% ObjectId %%

-type objectid() :: {oid, <<_:96>>}.
% <<Timestamp:32/big, MachineId:24/big, ProcessId:16/big, Count:24/big>>
% Timestamp is seconds since Unix epoch (Jan 1, 1970)

-spec objectid (integer(), <<_:40>>, integer()) -> objectid().
objectid (UnixSecs, MachineAndProcId, Count) ->
	{oid, <<UnixSecs :32/big, MachineAndProcId :5/binary, Count :24/big>>}.

-spec objectid_time (objectid()) -> unixtime().
% Time when object id was generated
objectid_time ({oid, <<UnixSecs:32/big, _:64>>}) -> {unixtime, UnixSecs * 1000}.
