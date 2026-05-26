-module(http_client).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").
-include("types.hrl").

% Public API
-export([start_link/2, get/2, get/3, post/3, post/4, patch/3, patch/4]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(DEFAULT_USER_AGENT, "comictrack/1.0").
-define(DEFAULT_TIMEOUT, 10_000).
-define(DEFAULT_RETRIES, 3).

-record(connection, {pid  :: pid(),
                     mref :: reference()
                    }).

-record(request, {pid              :: pid(),
                  status=undefined :: optional(non_neg_integer()),
                  body=[]          :: [binary()]
                 }).

-record(state, {connection    :: optional(#connection{}),
                requests=#{}  :: #{reference() => #request{}},
                configuration :: configuration()
               }).

-type configuration() :: #{host := string(),
                           port := pos_integer(),
                           user_agent := string(),
                           headers := {string(), string()},
                           query_params := {string(), string()}}.

-type options() :: #{query_params := [{string(), string()}],
                     headers := [{string(), string()}]
                    }.

-type response() :: {ok, {pos_integer(), binary()}} | {error, timeouot}.

% Public API
-spec start_link(Name, Configuration) -> {ok, pid()}
    when Name :: atom(), Configuration :: configuration().
start_link(Name, Configuration) ->
    gen_server:start_link({local, Name}, ?MODULE, [Configuration], []).

-spec get(atom(), string()) -> response().
get(ServerName, Resource) ->
    get(ServerName, Resource, #{}).

-spec get(atom(), string(), options()) -> response().
get(ServerName, Resource, Options) ->
    api_call_(ServerName, get, Resource, Options, ?DEFAULT_RETRIES).

-spec post(atom(), string(), iolist()) -> response().
post(ServerName, Resource, Body) ->
    post(ServerName, Resource, Body, #{}).

-spec post(atom(), string(), iolist(), options()) -> response().
post(ServerName, Resource, Body, Options) ->
    api_call_(ServerName, post, Resource, Body, Options, ?DEFAULT_RETRIES).

-spec patch(atom(), string(), iolist()) -> response().
patch(ServerName, Resource, Body) ->
    patch(ServerName, Resource, Body, #{}).

-spec patch(atom(), string(), iolist(), options()) -> response().
patch(ServerName, Resource, Body, Options) ->
    api_call_(ServerName, patch, Resource, Body, Options, ?DEFAULT_RETRIES).

% gen_server callbacks
init([UserConfiguration]) ->
    Defaults = #{user_agent => ?DEFAULT_USER_AGENT},
    Configuration = maps:merge(Defaults, UserConfiguration),
    {ok, #state{configuration=Configuration}}.

% requests without bodies
handle_call({get, Resource, Options}, {Pid, _Tags},
            State=#state{requests=Reqs}) ->
    Query = maps:get(query_params, Options, []),
    Headers = maps:get(headers, Options, []),
    Conn = get_connection(State),
    QueryString = build_query_params(default_query_params(State) ++ Query),
    FullUrl = Resource ++ QueryString,
    ?LOG_DEBUG("sending GET to ~p", [FullUrl]),
    StreamRef = gun:get(Conn#connection.pid, FullUrl,
                        build_headers(Headers, State)),
    {reply, {ok, StreamRef},
     State#state{connection=Conn,
                 requests=Reqs#{StreamRef => #request{pid=Pid}}}};
% requests with bodies
handle_call({Method, Resource, Body, Options}, {Pid, _Tags},
            State=#state{requests=Reqs}) ->
    Query = maps:get(query_params, Options, []),
    Headers = maps:get(headers, Options, []),
    Conn = get_connection(State),
    QueryString = build_query_params(default_query_params(State) ++ Query),
    FullUrl = Resource ++ QueryString,
    StreamRef = case Method of
                    post ->
                        ?LOG_DEBUG("sending POST to ~p", [FullUrl]),
                        gun:post(Conn#connection.pid, FullUrl,
                                 build_headers(Headers, State),
                                 Body);
                    patch ->
                        ?LOG_DEBUG("sending PATCH to ~p", [FullUrl]),
                        gun:patch(Conn#connection.pid, FullUrl,
                                  build_headers(Headers, State),
                                  Body)
                end,
    {reply, {ok, StreamRef},
     State#state{connection=Conn,
                 requests=Reqs#{StreamRef => #request{pid=Pid}}}}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({gun_down, ConnPid, _Protocol, Reason, _StreamRefs},
            State=#state{configuration=#{host := Host,
                                         port := Port},
                         connection=#connection{pid=ConnPid,
                                                mref=MRef}}) ->
    ?LOG_INFO("Discord API disconnected"),
    case gun_util:handle_down(ConnPid, Reason, Host, Port) of
        connected ->
            ?LOG_INFO("Discord API reconnected"),
            {noreply, State};
        disconnected ->
            gun:close(ConnPid),
            gun_util:await_down(ConnPid, MRef),
            {noreply, State#state{connection=undefined}}
    end;
handle_info({gun_response, ConnPid, StreamRef, Fin, Status, _Headers},
            State=#state{connection=#connection{pid=ConnPid},
                         requests=ActiveRequests}) ->
    ?LOG_DEBUG("headers: ~p", [_Headers]),
    case {Fin, maps:get(StreamRef, ActiveRequests, undefined)} of
        {_Fin, undefined} ->
            ?LOG_ERROR("~p stream ref ~p matches no active requests",
                       [ConnPid, StreamRef]),
            {noreply, State};
        {fin, #request{pid=Pid}} ->
            Pid ! {reply, StreamRef, {Status, no_data}},
            {noreply,
             State#state{requests=maps:remove(StreamRef, ActiveRequests)}};
        {nofin, Request0} ->
            Request1 = Request0#request{status=Status},
            {noreply,
             State#state{requests=ActiveRequests#{StreamRef => Request1}}}
    end;
handle_info({gun_data, ConnPid, StreamRef, Fin, Data},
            State=#state{connection=#connection{pid=ConnPid},
                         requests=ActiveRequests}) ->
    case {Fin, maps:get(StreamRef, ActiveRequests, undefined)} of
        {_Fin, undefined} ->
            ?LOG_ERROR("~p stream ref ~p matches no active requests",
                       [ConnPid, StreamRef]),
            {noreply, State};
        {fin, #request{pid=Pid, status=Status, body=Body}} ->
            Pid ! {reply, StreamRef, {Status, build_body([Data|Body])}},
            {noreply,
             State#state{requests=maps:remove(StreamRef, ActiveRequests)}};
        {nofin, Request0=#request{body=Body}} ->
            Request1 = Request0#request{body=[Data|Body]},
            {noreply,
             State#state{requests=ActiveRequests#{StreamRef => Request1}}}
    end.

% helper functions
api_call_(ServerName, Method, Resource, Options, Retries) ->
    api_call_loop_(ServerName, {Method, Resource, Options}, Retries).

api_call_(ServerName, Method, Resource, Body, Options, Retries) ->
    api_call_loop_(ServerName, {Method, Resource, Body, Options}, Retries).

api_call_loop_(_ServerName, _Msg, 0) -> {error, timeout};
api_call_loop_(ServerName, Msg, Retries) ->
    {ok, Ref} = gen_server:call(ServerName, Msg),
    case await_response(Ref) of
        {error, timeout} ->
            api_call_loop_(ServerName, Msg, Retries - 1);
        Reply -> Reply
    end.

build_body(Parts) ->
    lists:foldl(fun(X, Acc) -> <<X/binary, Acc/binary>> end, <<>>, Parts).

build_headers(Extra, #state{configuration=Configuration}) ->
    #{user_agent := UserAgent} = Configuration,
    Defaults = maps:get(headers, Configuration, []),
    [{<<"user-agent">>, UserAgent}] ++ Defaults ++ Extra.

await_response(Ref) ->  await_response(Ref, ?DEFAULT_TIMEOUT).

await_response(Ref, Timeout) ->
    receive
        {reply, Ref, Response} -> {ok, Response}
    after
        Timeout -> {error, timeout}
    end.

build_query_params([]) -> "";
build_query_params(QueryParams) ->
    Joiner = fun({Key, Val}, Acc) ->
                     uri_string:quote(Key) ++ "=" ++ uri_string:quote(Val)
                     ++ "&" ++ Acc
             end,
    "?" ++ lists:foldl(Joiner, "", QueryParams).

default_query_params(#state{configuration=Configuration}) ->
    maps:get(query_params, Configuration, []).

connect(#state{configuration=#{host := Host, port := Port}}) ->
    {ok, ConnPid} = gun:open(Host, Port, #{}),
    MRef = monitor(process, ConnPid),
    {ok, Protocol} = gun:await_up(ConnPid),
    ?LOG_INFO("~p connected to ~s:~p with protocol ~p",
              [ConnPid, Host, Port, Protocol]),
    #connection{pid=ConnPid, mref=MRef}.

get_connection(State=#state{connection=undefined}) -> connect(State);
get_connection(State) -> State#state.connection.
