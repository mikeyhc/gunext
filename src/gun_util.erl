-module(gun_util).

-include_lib("kernel/include/logger.hrl").

-export([handle_down/4, await_down/2]).

-define(DOWN_TIMEOUT, 100).

-spec handle_down(pid(), any(), iolist(), iolist()) -> connected | disconnected.
handle_down(ConnPid, Reason, Host, Port) ->
    case temporary_reason(Reason) of
        true ->
            ?LOG_DEBUG("~p temporarily disconnected from ~s:~p",
                      [ConnPid, Host, Port]),
            {ok, Protocol} = gun:await_up(ConnPid),
            ?LOG_DEBUG("~p reconnected to ~s:~p with protocol ~p",
              [ConnPid, Host, Port, Protocol]),
            connected;
        false ->
            ?LOG_DEBUG("~p disconnected from ~s:~p: ~p",
                      [ConnPid, Host, Port, Reason]),
            disconnected
    end.

-spec await_down(pid(), reference()) -> ok.
await_down(ConnPid, MRef) ->
    receive
        {'DOWN', MRef, process, ConnPid, shutdown} -> ok
    after ?DOWN_TIMEOUT -> ?LOG_ERROR("didn't receive down message")
    end,
    empty_queue(ConnPid, 0).

temporary_reason(normal) -> true;
temporary_reason({error, closesd}) -> true;
temporary_reason({error, einval}) -> true;
temporary_reason(_Reason) -> false.

empty_queue(ConnPid, Count) ->
    receive
        {gun_ws, ConnPid, _StreamRef, _Msg} -> empty_queue(ConnPid, Count + 1);
        {gun_response, ConnPid, _StreamRef, _Fin, _Status, _Headers} ->
            empty_queue(ConnPid, Count + 1);
        {gun_data, ConnPid, _StreamRef, _Fin, _Data} ->
            empty_queue(ConnPid, Count + 1);
        {gun_down, ConnPid, _Protocol, _Reason, _StreamRefs} ->
            empty_queue(ConnPid, Count + 1)
    after 0 -> ?LOG_DEBUG("dropped ~p messages", [Count])
    end.
