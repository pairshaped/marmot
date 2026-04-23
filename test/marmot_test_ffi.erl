-module(marmot_test_ffi).
-export([rescue/1]).

%% Call a zero-arity function, catching any exception.
%% Returns {ok, nil} on success or {error, Message} on failure.
rescue(Fun) ->
    try
        Fun(),
        {ok, nil}
    catch
        Class:Reason:_Stack ->
            Msg = unicode:characters_to_binary(
                io_lib:format("~p:~p", [Class, Reason])
            ),
            {error, Msg}
    end.
