%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


% get_topic_metadata_by_kafka


-module(nkkafka_protocol).
-export([connect/1, connect/3, connect/4, find_connection/1]).
-export([send_request/2, send_async_request/2, stop/1]).
-export([get_state/1, get_all/0]).
-export([start_link/3]).
-export([transports/1, default_port/1]).
-export([conn_init/1, conn_parse/3, conn_encode/2, conn_handle_call/4,
         conn_handle_cast/3, conn_handle_info/3, conn_stop/3]).


-define(DEBUG(Txt, Args, State),
    case erlang:get(nkkafka_protocol) of
        true -> ?LLOG(debug, Txt, Args, State);
        _ -> ok
    end).

-define(LLOG(Type, Txt, Args, State),
    lager:Type(
        [
            {srv_id, State#state.srv},
            {broker, State#state.id}
        ],
        "NkKAFKA Client ~s (~p,~p) "++Txt,
        [
            State#state.srv,
            State#state.id,
            self()
            | Args
        ])).


-define(SYNC_CALL_TIMEOUT, 1*60*60*1000).
-define(TIMEOUT, 180000).

-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").
-include("nkkafka.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type tid() :: integer().


%% ===================================================================
%% Public
%% ===================================================================

%% @doc
connect(Pid) when is_pid(Pid) ->
    {ok, Pid};

connect(SrvId) when is_atom(SrvId) ->
    connect({SrvId, 0});

connect({SrvId, BrokerId}) when is_atom(SrvId), is_integer(BrokerId) ->
    connect({SrvId, BrokerId, shared});

connect({SrvId, BrokerId, ConnId}) when is_atom(SrvId), is_integer(BrokerId) ->
    Spec = #{
        id => {BrokerId, ConnId},
        start => {?MODULE, start_link, [SrvId, BrokerId, ConnId]},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    },
    case nkserver_workers_sup:update_child2(SrvId, Spec, #{}) of
        {ok, _, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Starts a new session
-spec connect(nkserver:id(), nkkafka:broker_id(), term()) ->
    {ok, pid()} | {error, term()}.

connect(SrvId, BrokerId, ConnId) ->
    case nkkafka_util:cached_broker(SrvId, BrokerId) of
        undefined ->
            {error, broker_undefined};
        BrokerInfo ->
            connect(SrvId, BrokerId, BrokerInfo, ConnId)
    end.


%% @doc Starts a new session
-spec connect(nkserver:id(), nkkafka:broker_id(), nkkafka_util:broker_info(), term()) ->
    {ok, pid()} | {error, term()}.

connect(SrvId, BrokerId, #{ip:=Ip, port:=Port, protocol:=Proto}=Broker, ConnId) ->
    SrvPid = whereis(SrvId),
    ConnOpts1 = maps:get(protocol_opts, Broker, #{}),
    ConnOpts2 = ConnOpts1#{
        id => {nkkafka_client, SrvId, BrokerId, ConnId},
        class => {nkkafka_client, SrvId},
        monitor => SrvPid,
        tcp_packet => 4,
        send_timeout => ?TIMEOUT,
        send_timeout_close => true,
        idle_timeout => ?TIMEOUT,
        user_state => #{srv=>SrvId, broker=>BrokerId, id=>ConnId}
    },
    Conn = #nkconn{
        protocol = nkkafka_protocol,
        transp = Proto,
        ip = Ip,
        port = Port,
        opts = ConnOpts2
    },
    case nkpacket:connect(Conn, #{}) of
        {ok, #nkport{pid=Pid}} ->
            {ok, Pid};
        {error, Error} ->
            {error, Error}
    end.



%% @doc
find_connection(Pid) when is_pid(Pid) ->
    {ok, Pid};

find_connection(SrvId) when is_atom(SrvId) ->
    find_connection({SrvId, 0});

find_connection({SrvId, BrokerId}) when is_atom(SrvId), is_integer(BrokerId) ->
    find_connection({SrvId, BrokerId, shared});

find_connection({SrvId, BrokerId, ConnId}) when is_atom(SrvId), is_integer(BrokerId) ->
    case nklib_proc:values({?MODULE, SrvId, BrokerId, ConnId}) of
        [{_, Pid}] ->
            {ok, Pid};
        [] ->
            undefined
    end.


%% @private
start_link(SrvId, BrokerId, ConnId) ->
    case connect(SrvId, BrokerId, ConnId) of
        {ok, Pid} ->
            link(Pid),
            {ok, Pid};
        {error, Error} ->
            {error, Error}
    end.



%% @doc
-spec send_request(pid(), tuple()) ->
    {ok, term()} | {error, process_not_found|term()}.

send_request(Pid, Request) when is_tuple(Request) ->
    case nklib_util:call2(Pid, {nkkafka_send_req, Request}, ?SYNC_CALL_TIMEOUT) of
        process_not_found ->
            {error, process_not_found};
        Other ->
            Other
    end.


%% @doc
-spec send_async_request(pid(), tuple()) ->
    ok.

send_async_request(Pid, Request) when is_tuple(Request) ->
    gen_server:cast(Pid, {nkkafka_send_req, Request}).


%% @doc
-spec stop(pid()) ->
    ok | {error, term()}.

stop(Pid) ->
    gen_server:cast(Pid, nkkafka_stop).


%% @doc
get_state(Pid) ->
    gen_server:call(Pid, nkkafka_get_state).


%% @private
get_all() ->
    nklib_proc:values(?MODULE).




%% ===================================================================
%% Protocol
%% ===================================================================

-record(trans, {
    op :: term(),
    api :: integer(),
    timer :: reference(),
    mon :: reference(),
    from :: {pid(), term()} | {async, pid(), term()}
}).


-record(state, {
    srv :: nkservice:id(),
    id :: term(),
    broker :: nkkafka:broker_id(),
    trans = #{} :: #{tid() => #trans{}},
    tid :: integer()
}).


%% @private
-spec transports(nklib:scheme()) ->
    [nkpacket:transport()].

transports(_) -> [tcp].

-spec default_port(nkpacket:transport()) ->
    inet:port_number() | invalid.

default_port(tcp) -> 9092.


%% ===================================================================
%% WS Protocol callbacks
%% ===================================================================

-spec conn_init(nkpacket:nkport()) ->
    {ok, #state{}}.

conn_init(NkPort) ->
    {ok, #{srv:=SrvId, broker:=BrokerId, id:=ConnId}} = nkpacket:get_user_state(NkPort),
    case ConnId of
        {exclusive, _} ->
            ok;
        _ ->
            case nklib_proc:reg({?MODULE, SrvId, BrokerId, ConnId}) of
                true ->
                    ok;
                {false, _} ->
                    error(protocol_already_registered)
            end
    end,
    nklib_proc:put(?MODULE, {SrvId, BrokerId, ConnId}),
    {ok, Remote} = nkpacket:get_remote_bin(NkPort),
    {ok, Opts} = nkpacket:get_opts(NkPort),
    State = #state{
        srv = SrvId,
        broker = BrokerId,
        id = ConnId,
        tid = erlang:phash2(self())
    },
    set_debug(State),
    Idle = maps:get(idle_timeout, Opts),
    self() ! do_heartbeat,
    ?DEBUG("new connection (~p) (~s) (idle:~p)", [self(), Remote, Idle], State),
    {ok, State}.


%% @private
-spec conn_parse(term()|close, nkpacket:nkport(), #state{}) ->
    {ok, #state{}} | {stop, term(), #state{}}.

conn_parse(close, _NkPort, State) ->
    lager:error("NKLOG RECEIVED CLOSE"),
    {ok, State};

conn_parse(<<TId:32/signed-integer, Data/binary>>, _NkPort, State) ->
    case extract_op(TId, State) of
        {#trans{from=undefined}, State2} ->
            %lager:error("NKLOG RECEIVED ANNO"),
            {ok, State2};
        {#trans{from=From, api=API}, State2} ->
            Response = nkkafka_protocol_decode:response(API, Data),
            %lager:error("NKLOG RECEIVED RESP: ~p", [Response]),
            gen_server:reply(From, {ok, Response}),
            {ok, State2};
        not_found ->
            ?LLOG(info,
                "received client response for unknown req: ~p, ~p, ~p",
                [Data, TId, State#state.trans], State),
            {ok, State}
    end.


%% @private
conn_encode(Msg, _NkPort) ->
    {ok, Msg}.


-spec conn_handle_call(term(), {pid(), term()}, nkpacket:nkport(), #state{}) ->
    {ok, #state{}} | {stop, Reason::term(), #state{}}.

conn_handle_call({nkkafka_send_req, Request}, From, NkPort, State) ->
    send_request(Request, From, NkPort, State);

conn_handle_call(get_state, From, _NkPort, State) ->
    gen_server:reply(From, lager:pr(State, ?MODULE)),
    {ok, State}.


-spec conn_handle_cast(term(), nkpacket:nkport(), #state{}) ->
    {ok, #state{}} | {stop, Reason::term(), #state{}}.

conn_handle_cast({nkkafka_send_req, Request}, NkPort, State) ->
    lager:error("NKLOG REQ1"),
    send_request(Request,  undefined, NkPort, State);

conn_handle_cast(nkkafka_stop, _NkPort, State) ->
    ?DEBUG("user stop", [], State),
    {stop, normal, State}.


-spec conn_handle_info(term(), nkpacket:nkport(), #state{}) ->
    {ok, #state{}} | {stop, Reason::term(), #state{}}.

conn_handle_info(do_heartbeat, _NkPort, #state{srv=_SrvId, trans=_Trans}=State) ->
    erlang:send_after(5000, self(), do_heartbeat),
    {ok, State};

conn_handle_info(_Info, _NkPort, State) ->
    {ok, State}.


%% @doc Called when the connection stops
-spec conn_stop(Reason::term(), nkpacket:nkport(), #state{}) ->
    ok.

conn_stop(_Reason, _NkPort, _State) ->
    ok.



%% ===================================================================
%% Util
%% ===================================================================


%% @private
insert_op(TId, API, Req, From, #state{trans=AllTrans}=State) ->
    Time = 5000,
    Trans = #trans{
        op = Req,
        api = API,
        from = From,
        timer = erlang:start_timer(Time, self(), {nkkafka_op_timeout, TId})
    },
    State#state{trans=maps:put(TId, Trans, AllTrans)}.


%% @private
extract_op(TId, #state{trans=AllTrans}=State) ->
    case maps:find(TId, AllTrans) of
        {ok, #trans{mon=Mon, timer=Timer}=OldTrans} ->
            nklib_util:cancel_timer(Timer),
            nklib_util:demonitor(Mon),
            State2 = State#state{trans=maps:remove(TId, AllTrans)},
            {OldTrans, State2};
        error ->
            not_found
    end.


%% @private
send_request(Request, From, NkPort, #state{tid=TId}=State) ->
    {API, Bin} = nkkafka_protocol_encode:request(TId, <<"sync_client">>, Request),
    State2 = State#state{tid=TId+1},
    State3 = case Request of
        #req_produce{ack = none} ->
            gen_server:reply(From, {ok, no_ack}),
            State2;
        _ ->
            insert_op(TId, API, element(1, Request), From, State2)
    end,
    %?LLOG(error, "NKLOG SEND REQ2", [], State),
    case nkpacket_connection:send(NkPort, Bin) of
        ok ->
            {ok, State3#state{tid=TId+1}};
        {error, Error} ->
            ?LLOG(warning, "error sending: ~p", [Error], State),
            gen_server:reply(From, {send_error, Error}),
            {stop, normal, State3}
    end.



set_debug(#state{srv = SrvId}=State) ->
    Debug = lists:member(protocol, nkserver:get_cached_config(SrvId, nkkafka, debug)),
    put(nkkafka_protocol, Debug),
    ?DEBUG("debug system activated", [], State).
