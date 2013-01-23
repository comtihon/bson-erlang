%@doc A BSON document is a JSON-like object with a standard binary encoding defined at bsonspec.org. This implements version 1.0 of that spec.
-module (bson).

-export_type ([maybe/1]).
-export_type ([document/0, label/0, value/0]).
-export_type ([arr/0]).
-export_type ([bin/0, bfunction/0, uuid/0, md5/0, userdefined/0]).
-export_type ([mongostamp/0, minmaxkey/0]).
-export_type ([utf8/0, regex/0, unixtime/0]).
-export_type ([javascript/0]).
-export_type ([objectid/0, unixsecs/0]).

-export ([lookup/2, lookup/3, at/2, include/2, exclude/2, update/3, merge/2, append/2]).
-export ([doc_foldl/3, doc_foldr/3, fields/1, fields_rec/1, document/1, document_rec/1]).
-export ([utf8/1, str/1]).
-export ([timenow/0, timenow_china/0, ms_precision/1, secs_to_unixtime/1, unixtime_to_secs/1]).
-export ([objectid/3, objectid_time/1]).

-type maybe(A) :: {A} | {}.

% Document %

-type document() :: tuple(). % {label(), value(), label(), value(), ...}.
% Conceptually a document is a list of label-value pairs (associative array, dictionary, record). However, for read/write-ability, it is implemented as a flat tuple, ie. the list becomes a tuple and the pair braces are elided, so you just have alternating labels and values where each value is associated with the previous label.
% To distinguish a tagged value such as {uuid, _} (see value() type below) from a document with field name 'uuid' we made sure all valid tagged value types have an odd number of elements (documents have even number of elements). So actually only {bin, uuid, _} is a valid value, {uuid, _} is a document.

-type label() :: atom() | binary().

-spec doc_foldl (fun ((label(), value(), A) -> A), A, document()) -> A.
%@doc Reduce document by applying given function to each field with result of previous field's application, starting with given initial result.
doc_foldl (Fun, Acc, Doc) -> doc_foldlN (Fun, Acc, Doc, 0, tuple_size (Doc) div 2).

-spec doc_foldlN (fun ((label(), value(), A) -> A), A, document(), integer(), integer()) -> A.
%@doc Fold over fields from first index (inclusive) to second index (exclusive), zero-based index.
doc_foldlN (_, Acc, _, High, High) -> Acc;
doc_foldlN (Fun, Acc, Doc, Low, High) ->
	Acc1 = Fun (element (Low * 2 + 1, Doc), element (Low * 2 + 2, Doc), Acc),
	doc_foldlN (Fun, Acc1, Doc, Low + 1, High).

-spec doc_foldr (fun ((label(), value(), A) -> A), A, document()) -> A.
%@doc Same as doc_foldl/3 except apply fields in reverse order
doc_foldr (Fun, Acc, Doc) -> doc_foldrN (Fun, Acc, Doc, 0, tuple_size (Doc) div 2).

-spec doc_foldrN (fun ((label(), value(), A) -> A), A, document(), integer(), integer()) -> A.
%@doc Fold over fields from second index (exclusive) to first index (inclusive), zero-based index.
doc_foldrN (_, Acc, _, Low, Low) -> Acc;
doc_foldrN (Fun, Acc, Doc, Low, High) ->
	Acc1 = Fun (element (High * 2 - 1, Doc), element (High * 2, Doc), Acc),
	doc_foldrN (Fun, Acc1, Doc, Low, High - 1).

-spec fields (document()) -> [{label(), value()}].
%@doc Convert document to a list of all its fields
fields (Doc) -> doc_foldr (fun (Label, Value, List) -> [{Label, Value} | List] end, [], Doc).

fields_rec(Doc) -> 
	if is_tuple(Doc) ->
            case Doc of
                {bin, B1, B2} -> [bin, B1, B2];
                {javascript, J1, J2} -> [javascript, J1, J2];
                {mongostamp, M1, M2} -> [mongostamp, M1, M2];
                {regex, R1, R2} -> [regex, R1, R2];
                {_, _, _} -> unixtime_to_secs(Doc);
                _ ->
                    doc_foldr (fun (Label, Value, List) -> [{Label, fields_rec(Value)} | List] end, [], Doc)
            end;
		true ->
			if is_list(Doc) ->
					case Doc of
						[] -> Doc;
						_ -> [fields_rec(E) || E <- Doc]
					end;
				true -> Doc
			end
	end.

document_rec(Fields) -> flatten_rec(Fields).

flatten_rec(Value, [{Label, Value1} | Fields]) ->
   case Fields of
       [] ->
           list_to_tuple(Value ++ [Label, flatten_rec(Value1)]);
       _ ->
           flatten_rec(Value ++ [Label, flatten_rec(Value1)], Fields)
   end.
    
%% 是不是这种形式hash, 其实和fields_rec一样，要先区分hash和array
flatten_rec ([{Label, Value} | Fields]) ->
	Value1 = [Label, flatten_rec(Value)],

    case Fields of
        [] -> list_to_tuple(Value1);
        _ ->
            flatten_rec(Value1, Fields)
    end;
       
flatten_rec ([]) ->
    [];
flatten_rec (A) ->
    if is_list(A) ->
            [flatten_rec(E) || E <- A];
       true -> A
    end.

-spec document ([{label(), value()}]) -> document().
%@doc Convert list of fields to a document
document (Fields) -> list_to_tuple (flatten (Fields)).

-spec flatten ([{label(), value()}]) -> [label() | value()].
%@doc Flatten list by removing tuple constructors
flatten ([]) -> [];
flatten ([{Label, Value} | Fields]) -> [Label, Value | flatten (Fields)].

-spec lookup (label(), document()) -> maybe (value()).
%@doc Value of field in document if there
lookup (Label, Doc) ->
    {FUN, FUN1} = if is_atom(Label) ->
                          {fun erlang:atom_to_list/1, fun erlang:list_to_atom/1};
                     true ->
                          {fun erlang:binary_to_list/1, fun erlang:list_to_binary/1}
                  end,
    Parts = string:tokens (FUN (Label), "."),
    
	case length (Parts) of
		1 ->
			case find (FUN1 (hd (Parts)), Doc) of
						{Index} -> {element (Index * 2 + 2, Doc)};
						{} -> {} end;
		_ ->
			case find (FUN1 (hd (Parts)), Doc) of
				{Index} -> lookup (FUN1 (string:join (tl (Parts), ".")), element (Index * 2 + 2, Doc));
				{} -> {} end
	end.

-spec lookup (label(), document(), value()) -> value().
%@doc Value of field in document if there or default
lookup (Label, Doc, Default) ->
    {FUN, FUN1} = if is_atom(Label) ->
                          {fun erlang:atom_to_list/1, fun erlang:list_to_atom/1};
                     true ->
                          {fun erlang:binary_to_list/1, fun erlang:list_to_binary/1}
                  end,
    Parts = string:tokens (FUN (Label), "."),

	case length (Parts) of
		1 ->
			case find (FUN1 (hd (Parts)), Doc) of
						{Index} -> element (Index * 2 + 2, Doc);
						{} -> Default end;
		_ ->
			case find (FUN1 (hd (Parts)), Doc) of
				{Index} -> lookup (FUN1 (string:join (tl (Parts), ".")), element (Index * 2 + 2, Doc));
				{} -> Default end
	end.

-spec find (label(), document()) -> maybe (integer()).
%@doc Index of field in document if there
find (Label, Doc) -> findN (Label, Doc, 0, tuple_size (Doc) div 2).

-spec findN (label(), document(), integer(), integer()) -> maybe (integer()).
%@doc Find field index in document from first index (inclusive) to second index (exclusive).
findN (_Label, _Doc, High, High) -> {};
findN (Label, Doc, Low, High) -> case element (Low * 2 + 1, Doc) of
	Label -> {Low};
	_ -> findN (Label, Doc, Low + 1, High) end.

-spec at (label(), document()) -> value().
%@doc Value of field in document, error if missing
at (Label, Document) -> case lookup (Label, Document) of
	% {} -> erlang:error (missing_field, [Label, Document]);
	{} -> null;
	{Value} -> Value end.

-spec include ([label()], document()) -> document().
%@doc Project given fields of document
include (Labels, Document) ->
	Fun = fun (Label, Doc) -> case lookup (Label, Document) of
		{Value} -> [Label, Value | Doc];
		{} -> Doc end end,
	list_to_tuple (lists:foldr (Fun, [], Labels)).

-spec exclude ([label()], document()) -> document().
%@doc Remove given fields from document
exclude (Labels, Document) ->
	Fun = fun (Label, Value, Doc) -> case lists:member (Label, Labels) of
		false -> [Label, Value | Doc];
		true -> Doc end end,
	list_to_tuple (doc_foldr (Fun, [], Document)).

-spec update (label(), value(), document()) -> document().
%@doc Replace field with new value, adding to end if new
update (Label, Value, Document) ->
	{FUN, FUN1} = if is_atom(Label) ->
                          {fun erlang:atom_to_list/1, fun erlang:list_to_atom/1};
                     true ->
                          {fun erlang:binary_to_list/1, fun erlang:list_to_binary/1}
                  end,
    Parts = string:tokens (FUN (Label), "."),

	case length (Parts) of
		1 ->
			case find (FUN1 (hd (Parts)), Document) of
				{Index} -> setelement (Index * 2 + 2, Document, Value);
				{} ->
					Doc = erlang:append_element (Document, Label),
					erlang:append_element (Doc, Value) end;
		_ ->
			case find (FUN1 (hd (Parts)), Document) of
				{Index} -> setelement (Index * 2 + 2, Document, update (FUN1 (string:join (tl (Parts), ".")), Value, element (Index * 2 + 2, Document)));
				{} -> Doc = erlang:append_element (Document, FUN1 (hd (Parts))),
						erlang:append_element (Doc, update (FUN1 (string:join (tl (Parts), ".")), Value, {})) end
	end.


-spec merge (document(), document()) -> document().
%@doc First doc overrides second with new fields added at end of second doc
merge (UpDoc, BaseDoc) ->
	Fun = fun (Label, Value, Doc) -> update (Label, Value, Doc) end,
	doc_foldl (Fun, BaseDoc, UpDoc).

-spec append (document(), document()) -> document().
%@doc Append two documents together
append (Doc1, Doc2) -> list_to_tuple (tuple_to_list (Doc1) ++ tuple_to_list (Doc2)).

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
    binary() |
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
% An Erlang string() is a list of unicode characters (codepoints), but this list must be converted to utf-8 binary for use in Bson. Call utf8/1 to do this, or encode pure ascii literals directly as `<<"abc">>' and non-pure ascii literals as `<<"aßc"/utf8>>'.

-spec utf8 (unicode:chardata()) -> utf8().
%@doc Convert string to utf8 binary. string() is a subtype of unicode:chardata().
utf8 (CharData) -> case unicode:characters_to_binary (CharData) of
	{error, _Bin, _Rest} -> erlang:error (unicode_error, [CharData]);
	{incomplete, _Bin, _Rest} -> erlang:error (unicode_incomplete, [CharData]);
	Bin -> Bin end.

-spec str (unicode:chardata()) -> string().
%@doc Convert utf8 binary to string. utf8() is a subtype of unicode:chardata().
str (CharData) -> case unicode:characters_to_list (CharData) of
	{error, _Bin, _Rest} -> erlang:error (unicode_error, [CharData]);
	{incomplete, _Bin, _Rest} -> erlang:error (unicode_incomplete, [CharData]);
	Str -> Str end.

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

-spec timenow () -> unixtime(). % IO
% Current unixtime to millisecond precision, ie. MicroSecs is always a multiple of 1000.
timenow() -> ms_precision (os:timestamp()).

-spec timenow_china () -> unixtime().
timenow_china() -> ms_precision_china(os:timestamp()).

-spec ms_precision (unixtime()) -> unixtime().
%@doc Truncate microsecs to millisecs since bson drops microsecs anyway, so time will be equal before and after serialization.
ms_precision ({MegaSecs, Secs, MicroSecs}) ->
	{MegaSecs, Secs, MicroSecs div 1000 * 1000}.

-spec ms_precision_china (unixtime()) -> unixtime().
ms_precision_china ({MegaSecs, Secs, MicroSecs}) ->
	{MegaSecs, Secs + 8 * 3600, MicroSecs div 1000 * 1000}.

-type unixsecs() :: integer(). % Unix Time in seconds

-spec secs_to_unixtime (unixsecs()) -> unixtime().
secs_to_unixtime (UnixSecs) -> {UnixSecs div 1000000, UnixSecs rem 1000000, 0}.

-spec unixtime_to_secs (unixtime()) -> unixsecs().
unixtime_to_secs ({MegaSecs, Secs, _}) -> MegaSecs * 1000000 + Secs.

% Javascript %

-type javascript() :: {javascript, document(), utf8()}.  % scope and code

% ObjectId %

-type objectid() :: {<<_:96>>}.
% `<<UnixTimeSecs:32/big, MachineId:24/big, ProcessId:16/big, Count:24/big>>'

-spec objectid (unixsecs(), <<_:40>>, integer()) -> objectid().
objectid (UnixSecs, MachineAndProcId, Count) ->
	{<<UnixSecs :32/big, MachineAndProcId :5/binary, Count :24/big>>}.

-spec objectid_time (objectid()) -> unixtime().
%@doc Time when object id was generated
objectid_time ({<<UnixSecs:32/big, _:64>>}) -> secs_to_unixtime (UnixSecs).
