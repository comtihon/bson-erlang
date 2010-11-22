-module(bson_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

bson_test() ->
	Doc = [b, [x, 2, y, 3],
		   a, 1,
		   c, [<<"Mon">>, <<"Tue">>, <<"Wed">>] ],
	{1} = bson:lookup (a, Doc),
	{[<<"Mon">>|_]} = bson:lookup (c, Doc),
	{} = bson:lookup (d, Doc),
	1 = bson:at (a, Doc),
	{'EXIT', {missing_label, _}} = (catch bson:at (d, Doc)),
	[a, 1] = bson:include ([a], Doc),
	[a, 1] = bson:exclude ([b,c], Doc),
	[b, [x, 2, y, 3], a, 1, c, 4.2] = bson:update (c, 4.2, Doc),
	[b, 0, a, 1, c, 2, d, 3] = bson:merge ([c, 2, d, 3, b, 0], Doc).

twotime() ->
	% Assumes below are executed so close that both have same time to ms precision
	{Unixtime, {MegaSecs, Secs, MicroSecs}} = {bson:timenow(), os:timestamp()},
	{Unixtime, {MegaSecs, Secs, (MicroSecs div 1000) * 1000}}.

time_test() ->
	{Unixtime, Erltime} = twotime(),
	Unixtime = bson:erltime_to_unixtime (Erltime),
	Erltime = bson:unixtime_to_erltime (Unixtime),
	{MegaSecs, Secs, _} = Erltime,
	Erltime2 = {MegaSecs, Secs, 0},
	Unixtime2 = bson:erltime_to_unixtime (Erltime2),
	DateTime = calendar:now_to_datetime (Erltime2),
	Unixtime2 = bson:datetime_to_unixtime (DateTime),
	DateTime = bson:unixtime_to_datetime (Unixtime2).

objectid_test() ->
	{oid, <<1:32/big, 2:24/big, 3:16/big, 4:24/big>>} = bson:objectid (1, <<2:24/big, 3:16/big>>, 4),
	{unixtime, Millisecs} = bson:timenow(),
	Secs = Millisecs div 1000,
	Millisecs1 = Secs * 1000,
	{unixtime, Millisecs1} = bson:objectid_time (bson:objectid (Secs, <<2:24/big, 3:16/big>>, 4)).

binary_test() ->
	Doc = ['BSON', [<<"awesome">>, 5.05, 1986]],
	Bin = bson_binary:put_document (Doc),
	Bin = <<49,0,0,0,4,66,83,79,78,0,38,0,0,0,2,48,0,8,0,0,0,97,119,101,115,111,109,101,0,1,49,0,51,51,51,51,51,51,20,64,16,50,0,194,7,0,0,0,0>>,
	VBin = <<200,12,240,129,100,90,56,198,34,0,0>>,
	{unixtime, Millisecs} = bson:timenow(),
	Doc1 = [a, -4.230845,
			b, <<"hello">>,
			c, [x, -1, y, 2.2001],
			d, [23, 45, 200],
			eeeeeeeee, {bin, VBin},
			f, {bfunction, VBin},
			g, {uuid, Bin},
			h, {md5, VBin},
			i, {userdefined, Bin},
			j, bson:objectid (Millisecs div 1000, <<2:24/big, 3:16/big>>, 4),
			k1, false,
			k2, true,
			l, {unixtime, Millisecs},
			m, null,
			n, {regex, <<"foo">>, <<"bar">>},
			o1, {javascript, [], <<"function(x) = x + 1;">>},
			o2, {javascript, [x, 0, y, <<"foo">>], <<"function(a) = a + x">>},
			p, {symbol, x},
			q1, -2000444000,
			q2, -8000111000222001,
			r, {mongostamp, 1000222000},
			s1, minkey,
			s2, maxkey],
	Bin1 = bson_binary:put_document (Doc1),
	{Doc1, <<>>} = bson_binary:get_document (Bin1).
