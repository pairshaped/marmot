-module(marmot_test_ffi).
-define(FIXTURE_CACHE, 'statement_cache_fixture@statement_cache_ffi').
-export([rescue/1, get_env/1, cached_statement/2, cached_statement_count/1,
         run_concurrently/1, run_in_new_process/1]).

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

%% Look up an environment variable. Returns {ok, Value} or {error, nil}.
get_env(Name) ->
    case os:getenv(unicode:characters_to_list(Name)) of
        false -> {error, nil};
        Value -> {ok, unicode:characters_to_binary(Value)}
    end.

%% The statement this process has cached for Sql on Conn, as the generated
%% statement cache stores it in the process dictionary.
cached_statement(Conn, Sql) ->
    case erlang:get({?FIXTURE_CACHE, Conn}) of
        #{Sql := Stmt} -> {ok, Stmt};
        _ -> {error, nil}
    end.

%% How many statements this process has cached for Conn.
cached_statement_count(Conn) ->
    case erlang:get({?FIXTURE_CACHE, Conn}) of
        Statements when is_map(Statements) -> map_size(Statements);
        _ -> 0
    end.

%% Run every function in its own process at the same time and return their
%% results in order.
run_concurrently(Funs) ->
    Parent = self(),
    Refs = [begin
                Ref = make_ref(),
                spawn_link(fun() -> Parent ! {Ref, Fun()} end),
                Ref
            end || Fun <- Funs],
    [receive {Ref, Result} -> Result end || Ref <- Refs].

run_in_new_process(Fun) ->
    hd(run_concurrently([Fun])).
