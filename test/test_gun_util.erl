-module(test_gun_util).

-include_lib("eunit/include/eunit.hrl").

handle_down_normal_test() ->
    Pid = loop_proc(),
    self() ! {gun_up, Pid, http},
    ?assertEqual(connected, gun_util:handle_down(Pid, normal, "", 0)),
    exit(Pid, normal).

handle_down_closed_test() ->
    Pid = loop_proc(),
    self() ! {gun_up, Pid, http},
    ?assertEqual(connected, gun_util:handle_down(Pid, {error, closed}, "", 0)),
    exit(Pid, normal).

handle_down_einval_test() ->
    Pid = loop_proc(),
    self() ! {gun_up, Pid, http},
    ?assertEqual(connected, gun_util:handle_down(Pid, {error, einval}, "", 0)),
    exit(Pid, normal).

handle_down_other_test() ->
    Pid = loop_proc(),
    ?assertEqual(disconnected,
                 gun_util:handle_down(Pid, {error, unknown}, "", 0)),
    exit(Pid, normal).

await_down_test() ->
    Pid = loop_proc(),
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    gun_util:await_down(Pid, MRef).

await_down_no_down_test() ->
    Pid = loop_proc(),
    MRef = monitor(process, Pid),
    gun_util:await_down(Pid, MRef),
    exit(Pid, normal).

await_down_messages_test() ->
    Pid = loop_proc(),
    lists:foreach(fun(X) -> self() ! X end,
                  [{gun_ws, Pid, make_ref(), msg},
                   {gun_response, Pid, make_ref(), fin, 0, []},
                   {gun_data, Pid, make_ref(), fin, <<>>},
                   {gun_down, Pid, http, normal, []}]),
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    gun_util:await_down(Pid, MRef),
    receive
        Msg -> throw({failed, Msg})
    after 0 -> ok
    end.

% helper methods

loop_proc() ->
    spawn(fun Loop() -> receive stop -> ok after 5000 -> Loop() end end).
