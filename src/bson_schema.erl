-module(bson_schema).

-export([validate/2, validate_value/2]).


-spec validate(bson:document(), bson:document()) -> bson:document().
validate(Document, Spec) ->
  bson:doc_foldl(
    fun(Key, KeySpec, Acc) ->
      case bson:lookup(Key, Document) of
        {} ->
          case lists:member(required, KeySpec) of
            true -> error(badarg, [Document, Spec]);
            false -> Acc
          end;
        Value ->
          bson:update(Key, validate_value(Value, KeySpec), Acc)
      end
    end, {}, Spec).

-spec validate_value(term(), list()) -> term().
validate_value(Value, []) -> Value;
validate_value(Value, [required | Rest]) ->
  validate_value(Value, Rest);
validate_value({<<_:96>>} = Value, [object_id | Rest]) ->
  validate_value(Value, Rest);
validate_value(Value, Spec = [object_id | _]) ->
  error(badarg, [Value, Spec]);
validate_value(Value, Spec = [{atom, Values} | Rest]) ->
%%   lists:dropwhile(fun(V) -> V == Value orelse atom_to_binary(V, unicode) == Value end, Values),

  try lists:foreach(
    fun(V) ->
      case V == Value orelse atom_to_binary(V, unicode) == Value of
        true -> throw(V);
        false -> false
      end
    end, Values) of
    _ -> error(badarg, [Value, Spec])
  catch
    throw:V -> validate_value(V, Rest)
  end;
validate_value(Value, Spec = [utf8 | Rest]) ->
  case unicode:characters_to_binary(Value) of
    {error, _, _} -> error(badarg, [Value, Spec]);
    {incomplete, _, _} -> error(badarg, [Value, Spec]);
    Data -> validate_value(Data, Rest)
  end;
validate_value(Value, Spec = [{length, Min, Max} | Rest]) when is_list(Value) ->
  case within(length(Value), Min, Max) of
    true -> validate_value(Value, Rest);
    false -> error(badarg, [Value, Spec])
  end;
validate_value(Value, Spec = [{length, Min, Max} | Rest]) when is_binary(Value) ->
  case within(byte_size(Value), Min, Max) of
    true -> validate_value(Value, Rest);
    false -> error(badarg, [Value, Spec])
  end;
validate_value(Value, Spec = [{length, _Min, _Max} | _]) ->
  error(badarg, [Value, Spec]);
validate_value(Value, Spec = [{float, Min, Max} | Rest]) when is_float(Value) ->
  case within(Value, Min, Max) of
    true -> validate_value(Value, Rest);
    false -> error(badarg, [Value, Spec])
  end;
validate_value(Value, Spec = [{float, _Min, _Max} | _]) ->
  error(badarg, [Value, Spec]);
validate_value(Value, Spec = [{integer, Min, Max} | Rest]) when is_integer(Value) ->
  case within(Value, Min, Max) of
    true -> validate_value(Value, Rest);
    false -> error(badarg, [Value, Spec])
  end;
validate_value(Value, Spec = [{integer, _Min, _Max} | _]) ->
  error(badarg, [Value, Spec]);
validate_value({A, B, C} = Value, [timestamp | Rest]) when is_integer(A), is_integer(B), is_integer(C) ->
  validate_value(Value, Rest);
validate_value(Value, Spec = [timestamp | _]) ->
  error(badarg, [Value, Spec]);
validate_value(Value, [{list, Subspec} | Rest]) when is_list(Value) ->
  validate_value([validate_value(V, Subspec) || V <- Value], Rest);
validate_value(Value, Spec = [{list, _Subspec} | _]) ->
  error(badarg, [Value, Spec]);
validate_value(Value, [{object, Subspec} | Rest]) when is_tuple(Value) ->
  validate_value(validate(Value, Subspec), Rest);
validate_value(Value, Spec = [{object, _Subspec} | _]) ->
  error(badarg, [Value, Spec]).

%% @private
within(Value, '-infinity', Max) ->
  Value =< Max;
within(Value, Min, Max) ->
  Value >= Min andalso Value =< Max.