-module(marmot_ffi).
-export([run_executable/2, find_executable/1, get_env/1, make_tmp_file/2]).

%% Run an executable with args using open_port (no shell interpretation).
%% Waits for the port to close and returns the exit status as an integer.
run_executable(Path, Args) ->
    Port = erlang:open_port(
        {spawn_executable, unicode:characters_to_list(Path)},
        [{args, [unicode:characters_to_list(A) || A <- Args]},
         exit_status, stderr_to_stdout]
    ),
    wait_for_port(Port).

wait_for_port(Port) ->
    receive
        {Port, {exit_status, Status}} -> Status;
        {Port, {data, _}} -> wait_for_port(Port)
    end.

%% Look up a single environment variable by name. Converts the binary name
%% to a charlist for os:getenv/1 (required on OTP 27+) and converts the
%% result back to a binary. Returns {some, Value} or none.
get_env(Name) ->
    case os:getenv(unicode:characters_to_list(Name)) of
        false -> none;
        Value -> {some, unicode:characters_to_binary(Value)}
    end.

%% Create a temp file with a random name and write Content to it atomically.
%% Uses crypto:strong_rand_bytes for an unpredictable suffix and exclusive
%% mode to prevent symlink races. Returns {ok, Path} or {error, Reason}.
make_tmp_file(Dir, Content) ->
    Suffix = binary:encode_hex(crypto:strong_rand_bytes(8)),
    Path = <<Dir/binary, "/marmot_fmt_", Suffix/binary, ".gleam">>,
    case file:write_file(Path, Content, [exclusive]) of
        ok -> {ok, Path};
        {error, _} -> {error, nil}
    end.

%% Find an executable on PATH. Returns {some, Path} or none.
find_executable(Name) ->
    case os:find_executable(unicode:characters_to_list(Name)) of
        false -> none;
        Path -> {some, unicode:characters_to_binary(Path)}
    end.
