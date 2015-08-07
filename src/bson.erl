%% @doc A BSON document is a JSON-like object with a standard binary encoding defined at bsonspec.org.
%% This implements version 1.0 of that spec.
-module(bson).

-export_type([document/0, label/0, value/0]).
-export_type([arr/0]).
-export_type([bin/0, bfunction/0, uuid/0, md5/0, userdefined/0]).
-export_type([mongostamp/0, minmaxkey/0]).
-export_type([utf8/0, regex/0, unixtime/0]).
-export_type([javascript/0]).
-export_type([objectid/0, unixsecs/0]).

-export([lookup/2, lookup/3, at/2, include/2, exclude/2, update/3, merge/2, merge/3, append/2]).
-export([doc_foldl/3, doc_foldr/3, fields/1, document/1, flatten_map/1]).
-export([utf8/1, str/1]).
-export([timenow/0, ms_precision/1, secs_to_unixtime/1, unixtime_to_secs/1]).
-export([objectid/3, objectid_time/1]).

% Document %

-type document() :: tuple(). % {label(), value(), label(), value(), ...}.
%% Conceptually a document is a list of label-value pairs (associative array, dictionary, record).
%% However, for read/write-ability, it is implemented as a flat tuple, ie. the list becomes a tuple
%% and the pair braces are elided, so you just have alternating labels and values where each value
%% is associated with the previous label.
%% To distinguish a tagged value such as {uuid, _} (see value() type below) from a document with
%% field name 'uuid' we made sure all valid tagged value types have an odd number of elements
%% (documents have even number of elements). So actually only {bin, uuid, _} is a valid value, {uuid, _} is a document.

-type label() :: binary() | atom().

%% @doc Reduce document by applying given function to each field with result of previous field's
%% application, starting with given initial result.
-spec doc_foldl(fun ((label(), value(), A) -> A), A, document() | map()) -> A.
doc_foldl(Fun, Acc, Doc) when is_map(Doc) ->
  maps:fold(Fun, Acc, Doc);
doc_foldl(Fun, Acc, Doc) ->
  doc_foldlN(Fun, Acc, Doc, 0, tuple_size(Doc) div 2).

%% @doc Fold over fields from first index (inclusive) to second index (exclusive), zero-based index.

-spec doc_foldlN(fun ((label(), value(), A) -> A), A, document(), integer(), integer()) -> A.
doc_foldlN(_, Acc, _, High, High) -> Acc;
doc_foldlN(Fun, Acc, Doc, Low, High) ->
  Acc1 = Fun(element(Low * 2 + 1, Doc), element(Low * 2 + 2, Doc), Acc),
  doc_foldlN(Fun, Acc1, Doc, Low + 1, High).

%% @doc Same as doc_foldl/3 except apply fields in reverse order
-spec doc_foldr(fun ((label(), value(), A) -> A), A, document()) -> A.
doc_foldr(Fun, Acc, Doc) -> doc_foldrN(Fun, Acc, Doc, 0, tuple_size(Doc) div 2).

%% @doc Fold over fields from second index (exclusive) to first index (inclusive), zero-based index.
-spec doc_foldrN(fun ((label(), value(), A) -> A), A, document(), integer(), integer()) -> A.
doc_foldrN(_, Acc, _, Low, Low) -> Acc;
doc_foldrN(Fun, Acc, Doc, Low, High) ->
  Acc1 = Fun(element(High * 2 - 1, Doc), element(High * 2, Doc), Acc),
  doc_foldrN(Fun, Acc1, Doc, Low, High - 1).

%% @doc Convert document to a list of all its fields
-spec fields(document()) -> [{label(), value()}].
fields(Doc) -> doc_foldr(fun(Label, Value, List) -> [{Label, Value} | List] end, [], Doc).

%% @doc Convert list of fields to a document
-spec document([{label(), value()}]) -> document().
document(Fields) -> list_to_tuple(flatten(Fields)).

%% @doc Flatten list by removing tuple constructors
-spec flatten([{label(), value()}]) -> [label() | value()].
flatten([]) -> [];
flatten([{Label, Value} | Fields]) -> [Label, Value | flatten(Fields)].

%% @doc Value of field in document if there
-spec lookup(label(), document()) -> value() | {}.
lookup(Label, Doc) ->
  lookup(Label, Doc, {}).

%% @doc Value of field in document if there or default
-spec lookup(label(), document(), value()) -> value().
lookup(Label, Doc, Default) when is_atom(Label) ->
  lookup(atom_to_binary(Label, utf8), Doc, Default);
lookup(Label, Doc, Default) ->
  Parts = binary:split(Label, <<".">>, []),
  case length(Parts) of
    1 ->
      lookup(Parts, Doc, fun(Index) -> element(Index * 2 + 2, Doc) end, Default);
    _ ->
      lookup(Parts, Doc, fun(Index) -> lookup(hd(tl(Parts)), element(Index * 2 + 2, Doc), Default) end, Default)
  end.

%% @doc Index of field in document if there
-spec find(label(), document()) -> integer() | {}.
find(Label, Doc) -> findN(Label, Doc, 0, tuple_size(Doc) div 2).

%% @doc Find field index in document from first index (inclusive) to second index (exclusive).
-spec findN(label(), document(), integer(), integer()) -> integer() | {}.
findN(_Label, _Doc, High, High) -> {};
findN(Label, Doc, Low, High) ->
  case element(Low * 2 + 1, Doc) of
    Label -> Low;
    AtomKey when is_atom(AtomKey) ->
      case atom_to_binary(AtomKey, utf8) =:= Label of
        true -> Low;
        false -> findN(Label, Doc, Low + 1, High)
      end;
    _ -> findN(Label, Doc, Low + 1, High)
  end.

%% @doc Value of field in document, error if missing
-spec at(label(), document()) -> value().
at(Label, Document) when is_atom(Label) ->
  at(atom_to_binary(Label, utf8), Document);
at(Label, Document) ->
  case lookup(Label, Document) of
    {} -> null;
    Value -> Value
  end.

%% @doc Project given fields of document
-spec include([label()], document()) -> document().
include(Labels, Document) ->
  Fun =
    fun(Label, Doc) ->
      case lookup(Label, Document) of
        {} -> Doc;
        Value -> [Label, Value | Doc]
      end
    end,
  list_to_tuple(lists:foldr(Fun, [], Labels)).

%% @doc Remove given fields from document
-spec exclude([label()], document()) -> document().
exclude(Labels, Document) ->
  Fun = fun(Label, Value, Doc) -> case lists:member(Label, Labels) of
                                    false -> [Label, Value | Doc];
                                    true -> Doc end end,
  list_to_tuple(doc_foldr(Fun, [], Document)).

%% @doc Replace field with new value, adding to end if new
-spec update(label(), value(), document()) -> document().
update(Label, Value, Document) when is_atom(Label) ->
  update(atom_to_binary(Label, utf8), Value, Document);
update(Label, Value, Document) ->
  Parts = binary:split(Label, <<".">>, []),
  case length(Parts) of
    1 ->
      update(Parts, Document, Value,
        fun(Index, Val) -> setelement(Index * 2 + 2, Document, Val) end,
        fun(Val) ->
          Doc = erlang:append_element(Document, Label),
          erlang:append_element(Doc, Val)
        end);
    _ ->
      update(Parts, Document, Value,
        fun(Index, Val) ->
          setelement(Index * 2 + 2, Document, update(hd(tl(Parts)), Val, element(Index * 2 + 2, Document)))
        end,
        fun(Val) ->
          Doc = erlang:append_element(Document, hd(Parts)),
          erlang:append_element(Doc, update(hd(tl(Parts)), Val, {}))
        end)
  end.

%% @doc First doc overrides second with new fields added at end of second doc
-spec merge(document(), document()) -> document().
merge(UpDoc, BaseDoc) ->
  Fun = fun(Label, Value, Doc) -> update(Label, Value, Doc) end,
  doc_foldl(Fun, BaseDoc, UpDoc).

-spec merge(document(), document(), fun((label(), value(), value()) -> value())) -> document().
merge(UpDoc, BaseDoc, Fun) ->
  Dict1 = orddict:from_list(bson:fields(UpDoc)),
  Dict2 = orddict:from_list(bson:fields(BaseDoc)),
  bson:document(orddict:merge(Fun, Dict1, Dict2)).

%% @doc Append two documents together
-spec append(document(), document()) -> document().
append(Doc1, Doc2) -> list_to_tuple(tuple_to_list(Doc1) ++ tuple_to_list(Doc2)).

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
atom() |
integer() |
mongostamp() |
minmaxkey().

% Note, No value() can be a tuple with even number of elements because then it would be ambiguous with document(). Therefore all tagged values defined below have odd number of elements.

% Array %

-type arr() :: [value()].
% Caution, a string() will be interpreted as an array of integers. You must supply strings as utf8 binary, see below.

% String %

-type utf8() :: unicode:unicode_binary().
% binary() representing a string of characters encoded with UTF-8.
% An Erlang string() is a list of unicode characters (codepoints), but this list must be converted to utf-8 binary for use in Bson.
%% Call utf8/1 to do this, or encode pure ascii literals directly as `<<"abc">>' and non-pure ascii literals as `<<"a�c"/utf8>>'.

%% @doc Convert string to utf8 binary. string() is a subtype of unicode:chardata().
-spec utf8(unicode:chardata()) -> utf8().
utf8(CharData) ->
  case unicode:characters_to_binary(CharData) of
    {error, _Bin, _Rest} -> erlang:error(unicode_error, [CharData]);
    {incomplete, _Bin, _Rest} -> erlang:error(unicode_incomplete, [CharData]);
    Bin -> Bin
  end.

%% @doc Convert utf8 binary to string. utf8() is a subtype of unicode:chardata().
-spec str(unicode:chardata()) -> string().
str(CharData) ->
  case unicode:characters_to_list(CharData) of
    {error, _Bin, _Rest} -> erlang:error(unicode_error, [CharData]);
    {incomplete, _Bin, _Rest} -> erlang:error(unicode_incomplete, [CharData]);
    Str -> Str
  end.

% Binary %

-type bin() :: {bin, bin, binary()}.
-type bfunction() :: {bin, function, binary()}.
-type uuid() :: {bin, uuid, binary()}.
-type md5() :: {bin, md5, binary()}.
-type userdefined() :: {bin, userdefined, binary()}.

% Special %

-type mongostamp() :: {mongostamp, integer(), integer()}.
% 4-byte increment, 4-byte timestamp. 0 timestamp has special semantics
-type minmaxkey() :: 'MIN_KEY' | 'MAX_KEY'.
% Special values that compare lower/higher than all other bson values

% Regex %

-type regex() :: {regex, utf8(), utf8()}.  % pattern and options

% Datetime %

-type unixtime() :: {integer(), integer(), integer()}. % {MegaSecs, Secs, MicroSecs}
% Unix time in Erlang now/os:timstamp format, but only to millisecond precision when serialized.

% Current unixtime to millisecond precision, ie. MicroSecs is always a multiple of 1000.
-spec timenow() -> unixtime(). % IO
timenow() -> ms_precision(os:timestamp()).

%% @doc Truncate microsecs to millisecs since bson drops microsecs anyway, so time will be equal before and after serialization.
-spec ms_precision(unixtime()) -> unixtime().
ms_precision({MegaSecs, Secs, MicroSecs}) ->
  {MegaSecs, Secs, MicroSecs div 1000 * 1000}.

-type unixsecs() :: integer(). % Unix Time in seconds

-spec secs_to_unixtime(unixsecs()) -> unixtime().
secs_to_unixtime(UnixSecs) -> {UnixSecs div 1000000, UnixSecs rem 1000000, 0}.

-spec unixtime_to_secs(unixtime()) -> unixsecs().
unixtime_to_secs({MegaSecs, Secs, _}) -> MegaSecs * 1000000 + Secs.

% Javascript %

-type javascript() :: {javascript, document(), utf8()}.  % scope and code

% ObjectId %

-type objectid() :: {<<_:96>>}.
% `<<UnixTimeSecs:32/big, MachineId:24/big, ProcessId:16/big, Count:24/big>>'

-spec objectid(unixsecs(), <<_:40>>, integer()) -> objectid().
objectid(UnixSecs, MachineAndProcId, Count) ->
  {<<UnixSecs:32/big, MachineAndProcId:5/binary, Count:24/big>>}.

%% @doc Time when object id was generated
-spec objectid_time(objectid()) -> unixtime().
objectid_time({<<UnixSecs:32/big, _:64>>}) -> secs_to_unixtime(UnixSecs).

%% Flattens map, add dot notation to all nested objects
-spec flatten_map(map()) -> map().
flatten_map(Map) ->
  List = flatten(<<>>, Map, []),
  maps:from_list(List).


%% @private
flatten(Key, Map, Acc) when is_map(Map) ->
  MapList = maps:to_list(Map),
  lists:foldl(fun({K, V}, Res) -> flatten(<<(append_dot(Key))/binary, K/binary>>, V, Res) end, Acc, MapList);
flatten(Key, Value, Acc) ->
  [{Key, Value} | Acc].

%% @private
append_dot(<<>>) -> <<>>;
append_dot(Key) when is_atom(Key) -> append_dot(atom_to_binary(Key, utf8));
append_dot(Key) when is_integer(Key) -> append_dot(integer_to_binary(Key));
append_dot(Key) when is_float(Key) -> append_dot(float_to_binary(Key));
append_dot(Key) when is_list(Key) -> append_dot(list_to_binary(Key));
append_dot(Key) -> <<Key/binary, <<".">>/binary>>.

%% @private
lookup(Parts, Doc, GetFun, Default) ->
  case find(hd(Parts), Doc) of
    {} -> Default;
    Index -> GetFun(Index)
  end.

%% @private
update(Parts, Document, Value, SetFun, AppendFun) ->
  case find(hd(Parts), Document) of
    {} -> AppendFun(Value);
    Index -> SetFun(Index, Value)
  end.