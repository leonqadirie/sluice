-module(sluice_test_ffi).
-export([silence_supervisor_reports/0]).

%% The lifecycle tests and the supervision tests kill supervised stages.
%% This is intentional. OTP then writes supervisor reports to the log. Some
%% tests also cause warnings from sluice itself, for example the warning
%% about mixed max_demand values. The reports and the warnings are expected
%% test results, not failures. Thus remove them for the full test run.
%% Warnings from other sources stay visible.
silence_supervisor_reports() ->
    Filter = fun
        (#{msg := {report, #{label := {supervisor, _}}}}, _) ->
            stop;
        (#{msg := {string, Message}}, _) ->
            case string:find(unicode:characters_to_binary(Message), <<"sluice:">>) of
                nomatch -> ignore;
                _ -> stop
            end;
        (_, _) ->
            ignore
    end,
    logger:add_primary_filter(sluice_expected_reports, {Filter, ok}),
    nil.
