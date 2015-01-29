-module (bson_schema).

-export ([validate/2, validate_value/2]).


-spec validate(bson:document(), bson:document()) -> bson:document().
validate(Document, Spec) ->
	bson:doc_foldl(fun(Key, KeySpec, Acc) ->
		case bson:lookup(Key, Document) of
			{Value} ->
				bson:update(Key, validate_value(Value, KeySpec), Acc);
			{} ->
				case lists:member(required, KeySpec) of
					true -> error(badarg, [Document, Spec]);
					false -> Acc
				end
		end
	end, {}, Spec).


-spec validate_value(term(), list()) -> term().
validate_value(Value, []) ->
	Value;

validate_value(Value, [required | Rest]) ->
	validate_value(Value, Rest);

validate_value({<<_:96>>} = Value, [object_id | Rest]) ->
	validate_value(Value, Rest);
validate_value(Value, [object_id | _] = Spec) ->
	error(badarg, [Value, Spec]);

validate_value(Value, [{atom, Values} | Rest] = Spec) ->
	try lists:foreach(fun(V) ->
		case V == Value orelse atom_to_binary(V, unicode) == Value of
			true -> throw(V);
			false -> false
		end
	end, Values) of
		_ -> error(badarg, [Value, Spec])
	catch
		throw:V -> validate_value(V, Rest)
	end;

validate_value(Value, [utf8 | Rest] = Spec) ->
	case unicode:characters_to_binary(Value) of
		{error, _, _} -> error(badarg, [Value, Spec]);
		{incomplete, _, _} -> error(badarg, [Value, Spec]);
		Data -> validate_value(Data, Rest)
	end;

validate_value(Value, [{length, Min, Max} | Rest] = Spec) when is_list(Value) ->
	case within(length(Value), Min, Max) of
		true -> validate_value(Value, Rest);
		false -> error(badarg, [Value, Spec])
	end;
validate_value(Value, [{length, Min, Max} | Rest] = Spec) when is_binary(Value) ->
	case within(byte_size(Value), Min, Max) of
		true -> validate_value(Value, Rest);
		false -> error(badarg, [Value, Spec])
	end;
validate_value(Value, [{length, _Min, _Max} | _] = Spec) ->
	error(badarg, [Value, Spec]);

validate_value(Value, [{float, Min, Max} | Rest] = Spec) when is_float(Value) ->
	case within(Value, Min, Max) of
		true -> validate_value(Value, Rest);
		false -> error(badarg, [Value, Spec])
	end;
validate_value(Value, [{float, _Min, _Max} | _] = Spec) ->
	error(badarg, [Value, Spec]);

validate_value(Value, [{integer, Min, Max} | Rest] = Spec) when is_integer(Value) ->
	case within(Value, Min, Max) of
		true -> validate_value(Value, Rest);
		false -> error(badarg, [Value, Spec])
	end;
validate_value(Value, [{integer, _Min, _Max} | _] = Spec) ->
	error(badarg, [Value, Spec]);

validate_value({A, B, C} = Value, [timestamp | Rest]) when is_integer(A), is_integer(B), is_integer(C) ->
	validate_value(Value, Rest);
validate_value(Value, [timestamp | _] = Spec) ->
	error(badarg, [Value, Spec]);

validate_value(Value, [{list, Subspec} | Rest]) when is_list(Value) ->
	validate_value([validate_value(V, Subspec) || V <- Value], Rest);
validate_value(Value, [{list, _Subspec} | _] = Spec) ->
	error(badarg, [Value, Spec]);

validate_value(Value, [{object, Subspec} | Rest]) when is_tuple(Value) ->
	validate_value(validate(Value, Subspec), Rest);
validate_value(Value, [{object, _Subspec} | _] = Spec) ->
	error(badarg, [Value, Spec]).


% @private
within(Value, '-infinity', Max) ->
	Value =< Max;
within(Value, Min, Max) ->
	Value >= Min andalso Value =< Max.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
