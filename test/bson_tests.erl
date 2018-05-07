-module(bson_tests).

-include_lib("eunit/include/eunit.hrl").

bson_test() ->
  Doc =
    {
      <<"b">>, {x, 2, y, 3},
      <<"a">>, 1,
      <<"c">>, [mon, tue, wed]
    },
  1 = bson:lookup(<<"a">>, Doc),
  {} = bson:lookup(<<"d">>, Doc),
  2 = bson:lookup(<<"d">>, Doc, 2),
  1 = bson:lookup(<<"a">>, Doc, 3),
  1 = bson:at(<<"a">>, Doc),
  2 = bson:at(<<"b.x">>, Doc),
  3 = bson:at(<<"b.y">>, Doc),
  {} = bson:lookup(<<"b.b">>, Doc),
  {} = bson:lookup(<<"b.z.z">>, Doc),
  2 = bson:lookup(<<"b.x">>, Doc, default_value),
  default_value = bson:lookup(<<"b.z.z">>, Doc, default_value),
  null = bson:at(<<"d">>, Doc),
  {<<"a">>, 1} = bson:include([<<"a">>], Doc),
  {} = bson:include([z], Doc),
  {<<"a">>, 1, <<"b.x">>, 2} = bson:include([<<"a">>, <<"b.x">>], Doc),
  {<<"a">>, 1} = bson:exclude([<<"b">>, <<"c">>], Doc),
  {<<"b">>, {x, 2, y, 3}, <<"a">>, 1, <<"c">>, 4.2} = bson:update(<<"c">>, 4.2, Doc),
  {<<"b">>, {x, 13, y, 3}, <<"a">>, 1, <<"c">>, [mon, tue, wed]} = bson:update(<<"b.x">>, 13, Doc),
  {<<"b">>, {x, 2, y, 14}, <<"a">>, 1, <<"c">>, [mon, tue, wed]} = bson:update(<<"b.y">>, 14, Doc),
  {<<"b">>, {x, 2, y, 3, <<"z">>, 0}, <<"a">>, 1, <<"c">>, [mon, tue, wed]} = bson:update(<<"b.z">>, 0, Doc),
  {<<"b">>, {x, 2, y, 3}, <<"a">>, 1, <<"c">>, [mon, tue, wed], <<"d">>, {<<"x">>, 15}} = bson:update(<<"d.x">>, 15, Doc),
  {<<"b">>, 0, <<"a">>, 1, <<"c">>, 2, <<"d">>, 3} = bson:merge({<<"c">>, 2, <<"d">>, 3, <<"b">>, 0}, Doc),
  {<<"a">>, 1, <<"b">>, 2, <<"c">>, 3, d, 4} = bson:append({<<"a">>, 1, <<"b">>, 2}, {<<"c">>, 3, d, 4}),
  [{<<"b">>, {x, 2, y, 3}}, {<<"a">>, 1}, {<<"c">>, [mon, tue, wed]}] = bson:fields(Doc).

time_test() ->
  {MegaSecs, Secs, _} = bson:timenow(),
  {MegaSecs, Secs, 0} = bson:secs_to_unixtime(bson:unixtime_to_secs({MegaSecs, Secs, 0})).

objectid_test() ->
  {<<1:32/big, 2:24/big, 3:16/big, 4:24/big>>} = bson:objectid(1, <<2:24/big, 3:16/big>>, 4),
  UnixSecs = bson:unixtime_to_secs(bson:timenow()),
  UnixTime = bson:objectid_time(bson:objectid(UnixSecs, <<2:24/big, 3:16/big>>, 4)),
  UnixSecs = bson:unixtime_to_secs(UnixTime).

binary_test() ->
  Doc = {'BSON', [<<"awesome">>, 5.05, 1986]},
  Bin = bson_binary:put_document(Doc),
  Bin = <<49, 0, 0, 0, 4, 66, 83, 79, 78, 0, 38, 0, 0, 0, 2, 48, 0, 8, 0, 0, 0, 97, 119, 101, 115, 111, 109, 101, 0, 1, 49, 0, 51, 51, 51, 51, 51, 51, 20, 64, 16, 50, 0, 194, 7, 0, 0, 0, 0>>,
  VBin = <<200, 12, 240, 129, 100, 90, 56, 198, 34, 0, 0>>,
  Time = bson:timenow(),
  Doc1 = {<<"a">>, -4.230845,
    <<"b">>, <<"hello">>,
    <<"c">>, {<<"x">>, -1, <<"y">>, 2.2001},
    <<"d">>, [23, 45, 200],
    <<"eeeeeeeee">>, {bin, bin, VBin},
    <<"f">>, {bin, function, VBin},
    <<"g">>, {bin, uuid, Bin},
    <<"g4">>, {bin, uuid4, Bin},
    <<"h">>, {bin, md5, VBin},
    <<"i">>, {bin, userdefined, Bin},
    <<"j">>, bson:objectid(bson:unixtime_to_secs(Time), <<2:24/big, 3:16/big>>, 4),
    <<"k1">>, false,
    <<"k2">>, true,
    <<"l">>, Time,
    <<"m">>, undefined,
    <<"n">>, {regex, <<"foo">>, <<"bar">>},
    <<"o1">>, {javascript, {}, <<"function(x) = x + 1;">>},
    <<"o2">>, {javascript, {<<"x">>, 0, <<"y">>, <<"foo">>}, <<"function(a) = a + x">>},
    <<"p">>, atom,
    <<"q1">>, -2000444000,
    <<"q2">>, -8000111000222001,
    <<"r">>, {mongostamp, 100022, 995332003},
    <<"s1">>, 'MIN_KEY',
    <<"s2">>, 'MAX_KEY'},
  Bin1 = bson_binary:put_document(Doc1),
  ?assertEqual({Doc1, <<>>}, bson_binary:get_document(Bin1)).

put_document_test() ->
  Doc = {<<"key">>, <<"value">>},
  ?assertEqual({{<<"key">>, <<"value">>}, <<>>}, bson_binary:get_document(bson_binary:put_document(Doc))).

bson_int_too_large_test() ->
  Doc1 = {int, 16#7fffffffffffffff + 1},
  ?assertError(bson_int_too_large, bson_binary:put_document(Doc1)),
  Doc2 = {int, -16#8000000000000000 - 1},
  ?assertError(bson_int_too_large, bson_binary:put_document(Doc2)).

bad_bson_test() ->
  Doc = {function, fun() -> ok end},
  ?assertError(bad_bson, bson_binary:put_document(Doc)).

bson_document_test() ->
  Proplist = [{key1, value1}, {key2, [{key3, value3}]}],
  ?assertEqual({key1, value1, key2, [{key3, value3}]}, bson:document(Proplist)),
  ?assertError(function_clause, bson:document(not_list)).

str_test() ->
  ?assertEqual("test", bson:str(<<"test">>)),
  ?assertError(unicode_incomplete, bson:str(<<"test", 209>>)),
  ?assertError(unicode_error, bson:str(<<1, 2, 255, 255>>)).

utf8_test() ->
  ?assertEqual(<<"test">>, bson:utf8("test")).

maps_put_test() ->
  SimpleMap = #{<<"map">> => true, <<"simple">> => <<"very">>, atom => key, array => [1, 2, 3, 4]},
  Encoded1 = bson_binary:put_document(SimpleMap),
  {Decoded1, <<>>} = bson_binary:get_document(Encoded1),
  ?assertEqual({<<"array">>, [1, 2, 3, 4], <<"atom">>, key, <<"map">>, true, <<"simple">>, <<"very">>}, Decoded1),
  MapWithMap = #{<<"map">> => true, <<"simple">> => <<"not">>, <<"why">> => #{<<"because">> => <<"with map">>, ok => true}},
  Encoded2 = bson_binary:put_document(MapWithMap),
  {Decoded2, <<>>} = bson_binary:get_document(Encoded2),
  ?assertEqual({<<"map">>, true, <<"simple">>, <<"not">>, <<"why">>, {<<"ok">>, true, <<"because">>, <<"with map">>}}, Decoded2).

maps_get_test() ->
  SimpleMap = #{<<"map">> => true, <<"simple">> => <<"very">>, <<"atom">> => key, <<"array">> => [1, 2, 3, 4]},
  Encoded1 = bson_binary:put_document(SimpleMap),
  {GotMap, <<>>} = bson_binary:get_map(Encoded1),
  ?assertEqual(SimpleMap, GotMap),
  AtomMap = #{map => true, simple => <<"very">>, atom => key, array => [1, 2, 3, 4]},
  Encoded2 = bson_binary:put_document(AtomMap),
  {GotMap2, <<>>} = bson_binary:get_map(Encoded2),
  ?assertEqual(SimpleMap, GotMap2),
  ?assertNotEqual(AtomMap, GotMap2),
  MapWithMap = #{<<"map">> => true, <<"simple">> => <<"not">>, <<"why">> => #{<<"because">> => <<"with map">>, <<"ok">> => true}},
  Encoded3 = bson_binary:put_document(MapWithMap),
  {GotMap3, <<>>} = bson_binary:get_map(Encoded3),
  ?assertEqual(MapWithMap, GotMap3).

maps_flattering_test() ->
  Map =
    #{
      <<"user">> => #{<<"login">> => <<"test">>, <<"password">> => 1234},
      <<"personal">> => #{<<"name">> => <<"test">>, <<"country">> => #{<<"name">> => <<"USSR">>, <<"domen">> => <<"su">>}},
      <<"email">> => <<"test@test.su">>
    },
  Result = bson:flatten_map(Map),
  FlattenMap =
    #{
      <<"user.login">> => <<"test">>,
      <<"user.password">> => 1234,
      <<"personal.name">> => <<"test">>,
      <<"personal.country.name">> => <<"USSR">>,
      <<"personal.country.domen">> => <<"su">>,
      <<"email">> => <<"test@test.su">>
    },
  ?assertEqual(FlattenMap, Result).
