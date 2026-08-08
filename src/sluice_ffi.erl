-module(sluice_ffi).
-export([send_named/2, log_warning/1, safely/1, try_call/1,
         monotonic_milliseconds/0]).

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

%% Run a function and report the success. A pool worker uses this: a
%% failure in the work function must not remove the completion signal.
safely(Run) ->
    try
        Run(),
        true
    catch
        _Class:_Reason -> false
    end.

%% Preserve the value of a call, but turn every exception class into data.
%% Supervisors use this because a failing child start function is a start
%% failure, not a failure of the supervisor itself.
try_call(Run) ->
    try
        {ok, Run()}
    catch
        Class:Reason:Stack -> {error, {Class, Reason, Stack}}
    end.

%% A clock for timeout deadlines. Unlike wall time, it never moves
%% backwards or jumps when the system clock changes.
monotonic_milliseconds() ->
    erlang:monotonic_time(millisecond).
