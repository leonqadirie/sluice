-module(sluice_ffi).
-export([send_named/2, log_warning/1, monotonic_milliseconds/0]).

%% Send a message to a registered name. A send to a bare pid can not fail,
%% also when the pid is dead. Thus this function first changes the name
%% into a pid. If the name has no process, the message is lost without an
%% error. This is the same result as a message to a dead pid.
send_named(Name, Message) ->
    case whereis(Name) of
        undefined ->
            nil;
        Pid ->
            Pid ! {Name, Message},
            nil
    end.

%% Write a warning to the Erlang logger.
log_warning(Message) ->
    logger:warning(Message),
    nil.

%% A clock for timeout deadlines. Unlike wall time, it never moves
%% backwards or jumps when the system clock changes.
monotonic_milliseconds() ->
    erlang:monotonic_time(millisecond).
