%@doc Standard binary encoding of Bson documents, version 1.0. See bsonspec.org.
-module(bson_binary).

-export([put_document/1, get_document/1, get_map/1, put_cstring/1, get_cstring/1]).

-include("bson_binary.hrl").

-define(fits_int32(N), -16#80000000 =< N andalso N =< 16#7fffffff).
-define(fits_int64(N), -16#8000000000000000 =< N andalso N =< 16#7fffffffffffffff).

-define(put_tagname(Tag, N), (Tag):8, (put_cstring(N)) / binary).
% Name is expected to be in scope at call site

%% @doc utf8 binary cannot contain a 0 byte.
-spec put_cstring(bson:utf8()) -> binary().
put_cstring(UBin) -> <<UBin/binary, 0:8>>.

-spec get_cstring(binary()) -> {bson:utf8(), binary()}.
get_cstring(Bin) -> % list_to_tuple (binary:split (Bin, <<0>>)).
  {Pos, _Len} = binary:match(Bin, <<0>>), % _Len = 1 but don't match 1 to avoid check
  <<UBin:Pos/binary, 0:8, Rest/binary>> = Bin,
  {UBin, Rest}.

-spec put_document(bson:document()) -> binary().
put_document(Document) ->
  Bin = bson:doc_foldl(fun put_field_accum/3, <<>>, Document),
  <<?put_int32(byte_size(Bin) + 5), Bin/binary, 0:8>>.

-spec get_document(binary()) -> {bson:document(), binary()}.
get_document(<<?get_int32(N), Bin/binary>>) ->
  Size = N - 5,
  <<DocBin:Size/binary, 0:8, Bin1/binary>> = Bin,
  Doc = list_to_tuple(get_fields(DocBin, [])),
  {Doc, Bin1}.

-spec get_map(binary()) -> {map(), binary()}.
get_map(<<?get_int32(N), Bin/binary>>) ->
  Size = N - 5,
  <<DocBin:Size/binary, 0:8, Bin1/binary>> = Bin,
  Doc = get_fields(DocBin, #{}),
  {Doc, Bin1}.


%% @private
put_field_accum(Label, Value, Bin) when is_atom(Label) ->
  <<Bin/binary, (put_field(atom_to_binary(Label, utf8), Value))/binary>>;
put_field_accum(Label, Value, Bin) when is_binary(Label) ->
  <<Bin/binary, (put_field(Label, Value))/binary>>.

%% @private
get_fields(<<>>, Acc) when is_map(Acc) -> Acc;
get_fields(<<>>, Acc) -> lists:reverse(Acc);
get_fields(Bin, Acc) when is_map(Acc) ->
  {Name, Value, Bin1} = get_field(Bin, map),
  get_fields(Bin1, Acc#{Name => Value});
get_fields(Bin, Acc) ->
  {Name, Value, Bin1} = get_field(Bin, normal),
  get_fields(Bin1, [Value, Name | Acc]).

%% @private
-spec put_field(bson:utf8(), bson:value()) -> binary().
put_field(N, false) -> <<?put_tagname(8, N), 0:8>>;
put_field(N, true) -> <<?put_tagname(8, N), 1:8>>;
put_field(N, NULL) when NULL =:= null; NULL =:= undefined -> <<?put_tagname(10, N)>>;
put_field(N, 'MIN_KEY') -> <<?put_tagname(255, N)>>;
put_field(N, 'MAX_KEY') -> <<?put_tagname(127, N)>>;
put_field(N, {Oid}) -> <<?put_tagname(7, N), (put_oid(Oid))/binary>>;
put_field(N, {bin, BinType, Bin}) -> <<?put_tagname(5, N), (put_binary(BinType, Bin))/binary>>;
put_field(N, {regex, Pat, Opt}) -> <<?put_tagname(11, N), (put_cstring(Pat))/binary, (put_cstring(Opt))/binary>>;
put_field(N, {javascript, {}, Code}) -> <<?put_tagname(13, N), (put_string(Code))/binary>>;
put_field(N, {javascript, Env, Code}) -> <<?put_tagname(15, N), (put_closure(Code, Env))/binary>>;
put_field(N, {mongostamp, Inc, Time}) -> <<?put_tagname(17, N), ?put_int32(Inc), ?put_int32(Time)>>;
put_field(N, UnixTime = {_, _, _}) -> <<?put_tagname(9, N), (put_unixtime(UnixTime))/binary>>;
put_field(N, V) when is_float(V) -> <<?put_tagname(1, N), ?put_float(V)>>;
put_field(N, V) when is_binary(V) -> <<?put_tagname(2, N), (put_string(V))/binary>>;
put_field(N, V) when is_tuple(V) -> <<?put_tagname(3, N), (put_document(V))/binary>>;
put_field(N, V) when is_list(V) -> <<?put_tagname(4, N), (put_array(V))/binary>>;
put_field(N, V) when is_map(V) -> <<?put_tagname(3, N), (put_document(V))/binary>>;
put_field(N, V) when is_atom(V) -> <<?put_tagname(14, N), (put_string(atom_to_binary(V, utf8)))/binary>>;
put_field(N, V) when is_integer(V) andalso ?fits_int32(V) -> <<?put_tagname(16, N), ?put_int32(V)>>;
put_field(N, V) when is_integer(V) andalso ?fits_int64(V) -> <<?put_tagname(18, N), ?put_int64(V)>>;
put_field(N, V) when is_integer(V) -> erlang:error(bson_int_too_large, [N, V]);
put_field(N, V) -> erlang:error(bad_bson, [N, V]).

%% @private
get_field(<<1:8, _/binary>>, _, Bin1, _) ->
  <<?get_float(N), Bin2/binary>> = Bin1,
  {N, Bin2};
get_field(<<2:8, _/binary>>, _, Bin1, _) ->
  get_string(Bin1);
get_field(<<3:8, _/binary>>, _, Bin1, map) ->
  get_map(Bin1);
get_field(<<3:8, _/binary>>, _, Bin1, _) ->
  get_document(Bin1);
get_field(<<4:8, _/binary>>, _, Bin1, Type) ->
  get_array(Bin1, Type);
get_field(<<5:8, _/binary>>, _, Bin1, _) ->
  {BinType, Bin, Bin2} = get_binary(Bin1),
  {{bin, BinType, Bin}, Bin2};
get_field(<<6:8, _/binary>>, _, Bin1, _) -> % Treat the deprecated "undefined" value as null, which we call 'undefined'!
  {undefined, Bin1};
get_field(<<7:8, _/binary>>, _, Bin1, _) ->
  {Oid, Bin2} = get_oid(Bin1),
  {{Oid}, Bin2};
get_field(<<8:8, _/binary>>, _, <<Bit:8, Bin2/binary>>, _) ->
  {Bit == 1, Bin2};
get_field(<<9:8, _/binary>>, _, Bin1, _) ->
  get_unixtime(Bin1);
get_field(<<10:8, _/binary>>, _, Bin1, _) ->
  {undefined, Bin1};
get_field(<<11:8, _/binary>>, _, Bin1, _) ->
  {Pat, Bin2} = get_cstring(Bin1),
  {Opt, Bin3} = get_cstring(Bin2),
  {{regex, Pat, Opt}, Bin3};
get_field(<<13:8, _/binary>>, _, Bin1, _) ->
  {Code, Bin2} = get_string(Bin1),
  {{javascript, {}, Code}, Bin2};
get_field(<<14:8, _/binary>>, _, Bin1, _) ->
  {UBin, Bin2} = get_string(Bin1),
  {binary_to_atom(UBin, utf8), Bin2};
get_field(<<15:8, _/binary>>, _, Bin1, _) ->
  {Code, Env, Bin2} = get_closure(Bin1),
  {{javascript, Env, Code}, Bin2};
get_field(<<16:8, _/binary>>, _, Bin1, _) ->
  <<?get_int32(N), Bin2/binary>> = Bin1,
  {N, Bin2};
get_field(<<17:8, _/binary>>, _, Bin1, _) ->
  <<?get_int32(Inc), ?get_int32(Tim), Bin2/binary>> = Bin1,
  {{mongostamp, Inc, Tim}, Bin2};
get_field(<<18:8, _/binary>>, _, Bin1, _) ->
  <<?get_int64(N), Bin2/binary>> = Bin1,
  {N, Bin2};
get_field(<<127:8, _/binary>>, _, Bin1, _) ->
  {'MAX_KEY', Bin1};
get_field(<<255:8, _/binary>>, _, Bin1, _) ->
  {'MIN_KEY', Bin1};
get_field(<<Tag:8, Bin0/binary>>, _, _, _) ->
  erlang:error(unknown_bson_tag, [<<Tag:8, Bin0/binary>>]).

%% @private
-spec get_field(binary(), atom()) -> {bson:utf8(), bson:value(), binary()}.
get_field(R = <<_:8, Bin0/binary>>, Type) ->
  {Name, Bin1} = get_cstring(Bin0),
  {Value, BinRest} = get_field(R, Name, Bin1, Type),
  {Name, Value, BinRest}.

%% @private
-spec put_string(bson:utf8()) -> binary().
put_string(UBin) -> <<?put_int32(byte_size(UBin) + 1), UBin/binary, 0:8>>.

%% @private
-spec get_string(binary()) -> {bson:utf8(), binary()}.
get_string(<<?get_int32(N), Bin/binary>>) ->
  Size = N - 1,
  <<UBin:Size/binary, 0:8, Rest/binary>> = Bin,
  {UBin, Rest}.

%% @private
-spec put_array(bson:arr()) -> binary().
% encoded same as document with labels '0', '1', etc.
put_array(Values) ->
  {_N, Bin} = lists:foldl(fun put_value_accum/2, {0, <<>>}, Values),
  <<?put_int32(byte_size(Bin) + 5), Bin/binary, 0:8>>.

%% @private
put_value_accum(Value, {N, Bin}) ->
  {N + 1, <<Bin/binary, (put_field(bson:utf8(integer_to_list(N)), Value))/binary>>}.

%% @private
-spec get_array(binary(), normal | map) -> {bson:arr(), binary()}.
% encoded same as document with labels '0', '1', etc. which we ignore
get_array(<<?get_int32(N), Bin/binary>>, Type) ->
  Size = N - 5,
  <<DBin:Size/binary, 0:8, Bin1/binary>> = Bin,
  Array = get_values(DBin, [], Type),
  {Array, Bin1}.

%% @private
get_values(<<>>, Acc, _) -> lists:reverse(Acc);
get_values(Bin, Acc, Type) ->
  {_, Value, Bin1} = get_field(Bin, Type),
  get_values(Bin1, [Value | Acc], Type).

-type bintype() :: bin | function | uuid | md5 | userdefined.

%% @private
-spec put_binary(bintype(), binary()) -> binary().
put_binary(BinType, Bin) ->
  Tag = case BinType of bin -> 0; function -> 1; uuid -> 3; md5 -> 5; userdefined -> 128 end,
  <<?put_int32(byte_size(Bin)), Tag:8, Bin/binary>>.

%% @private
-spec get_binary(binary()) -> {bintype(), binary(), binary()}.
get_binary(<<?get_int32(Size), Tag:8, Bin/binary>>) ->
  BinType = case Tag of 0 -> bin; 1 -> function; 3 -> uuid; 5 -> md5; 128 -> userdefined end,
  <<VBin:Size/binary, Bin1/binary>> = Bin,
  {BinType, VBin, Bin1}.

%% @private
-spec put_closure(bson:utf8(), bson:document()) -> binary().
put_closure(Code, Env) ->
  Bin = <<(put_string(Code))/binary, (put_document(Env))/binary>>,
  <<?put_int32(byte_size(Bin) + 4), Bin/binary>>.

%% @private
-spec get_closure(binary()) -> {bson:utf8(), bson:document(), binary()}.
get_closure(<<?get_int32(_), Bin/binary>>) ->
  {Code, Bin1} = get_string(Bin),
  {Env, Bin2} = get_document(Bin1),
  {Code, Env, Bin2}.

%% @private
-spec put_unixtime(bson:unixtime()) -> binary().
put_unixtime({MegaSecs, Secs, MicroSecs}) ->
  <<?put_int64(MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000)>>.

%% @private
-spec get_unixtime(binary()) -> {bson:unixtime(), binary()}.
get_unixtime(<<?get_int64(MilliSecs), Bin/binary>>) ->
  {{MilliSecs div 1000000000, (MilliSecs div 1000) rem 1000000, (MilliSecs * 1000) rem 1000000}, Bin}.

%% @private
-spec put_oid(<<_:96>>) -> <<_:96>>.
put_oid(<<Oid:12/binary>>) -> Oid.

%% @private
-spec get_oid(binary()) -> {<<_:96>>, binary()}.
get_oid(<<Oid:12/binary, Bin/binary>>) -> {Oid, Bin}.