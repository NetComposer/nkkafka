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

%% Reads at 100.000/s with 400byte payload an 8 partitions, saturates 1GB!
%% If store_offsets is activated for each message it drops to 5.000/s

-module(nkkafka_subscriber).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([start/5, get_info/1, get_info/3, get_all/0, make_name/3]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-import(nklib_util, [to_binary/1]).

-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkKAFKA Subscriber (~s:~s:~p) "++Txt,
        [State#state.srv, State#state.topic, State#state.partition | Args])).


%% Offsets are inserted periodically besides after processed message
-define(INSERT_OFFSET_PERIOD, 60000).
-define(FETCH_MAX_BYTES, 1024*1024).

-include("nkkafka.hrl").
-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% Public
%% ===================================================================


%% @doc
-spec start(nkserver:id(), Topic::binary(), Part::integer(), map(), pid()) ->
    {ok, pid()}.

start(SrvId, Topic, Part, Config, LeaderPid) ->
    case nkkafka:get_leader(SrvId, Topic, Part) of
        {ok, BrokerId} ->
            Name = make_name(SrvId, Topic, Part),
            Args = [SrvId, Topic, Part, Config, LeaderPid, BrokerId],
            gen_server:start({local, Name}, ?MODULE, Args, []);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
get_info(SrvId, Topic, Part) ->
    get_info(make_name(SrvId, Topic, Part)).


%% @doc
get_info(Name) ->
    gen_server:call(Name, get_info).



%% @private
get_all() ->
    nklib_proc:values(?MODULE).


make_name(SrvId, Topic, Part) ->
    B = <<
        (to_binary(SrvId))/binary, $_,
        (to_binary(Topic))/binary, $_,
        (to_binary(Part))/binary
    >>,
    erlang:binary_to_atom(B, utf8).


% ===================================================================
%% gen_server behaviour
%% ===================================================================

-record(state, {
    srv :: nkserver:id(),
    topic :: binary(),
    partition :: integer(),
    broker :: nkkafka:broker_id(),
    config :: map(),
    next_offset :: integer(),
    leader_pid :: pid(),
    kafka_pid :: pid(),
    store_offsets :: boolean(),
    share_connection :: boolean(),
    store_group :: binary()
}).


%% @private
init([SrvId, Topic, Part, Config, LeaderPid, BrokerId]) ->
    Name = make_name(SrvId, Topic, Part),
    nklib_proc:put(?MODULE, Name),
    monitor(process, LeaderPid),
    State = #state{
        srv = SrvId,
        topic = Topic,
        partition = Part,
        broker = BrokerId,
        config = Config,
        leader_pid = LeaderPid,
        store_offsets = maps:get(store_offsets, Config, false),
        share_connection = maps:get(share_connection, Config, false),
        store_group = nkserver:get_cached_config(SrvId, nkkafka, store_offsets_group)
    },
    self() ! do_init,
    {ok, State}.


%% @private
handle_call(get_info, _From, State) ->
    #state{next_offset=Offset, store_offsets=Store, store_group=Group} = State,
    Data = #{
        next_offset => Offset,
        first_offset => find_offset(first, State),
        last_offset => find_offset(last, State)-1,
        store_offsets => Store,
        store_group => Group,
        last_stored_offset => find_stored_offset(State)
    },
    {reply, {ok, Data}, State};

handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_cast(Msg, State) ->
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_info(do_init, #state{config=Config, topic=Topic}=State) ->
    case connect(State) of
        {ok, State2} ->
            Start = maps:get(start_at, Config),
            case do_init(Start, State2) of
                {ok, State3} ->
                    erlang:send_after(2000, self(), read_messages),
                    {noreply, State3};
                {error, Error} ->
                    {stop, Error, State2}
            end;
        {error, Error} ->
            ?LLOG(error, "could not start connection for ~s:~p", [Topic, Error], State),
            {stop, connection_error, State}
    end;

handle_info(read_messages, State) ->
    State2 = read_messages(State),
    erlang:send_after(100, self(), read_messages),
    {noreply, State2};

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{leader_pid=Pid}=State) ->
    ?LLOG(notice, "leader is down (~p), stopping", [Reason], State),
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{kafka_pid=Pid}=State) ->
    ?LLOG(notice, "consumer is down (~p), stopping", [Reason], State),
    {stop, normal, State};

handle_info(Info, State) ->
    lager:warning("Module ~p received unexpected info: ~p (~p)", [?MODULE, Info, State]),
    {noreply, State}.


%% @private
-spec code_change(term(), #state{}, term()) ->
    {ok, #state{}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
-spec terminate(term(), #state{}) ->
    ok.

terminate(_Reason, _State) ->
    ok.



% ===================================================================
%% Internal
%% ===================================================================


%% @private
do_init(stored, State) ->
    Next = find_stored_offset(State),
    ?LLOG(notice, "started (~p) (starting at last STORED next offset: ~p)",
          [self(), Next], State),
    {ok, State#state{next_offset=Next}};

do_init(last, State) ->
    Offset = find_offset(last, State),
    ?LLOG(notice, "started (~p) (starting at LAST offset: ~p)", [self(), Offset], State),
    {ok, State#state{next_offset = Offset}};


do_init(first, State) ->
    Offset = find_offset(first, State),
    ?LLOG(notice, "started (~p) (starting at FIRST offset: ~p)", [self(), Offset], State),
    {ok, State#state{next_offset = Offset}}.


connect(State) ->
    #state{
        srv = SrvId,
        broker = BrokerId,
        topic = Topic,
        partition = Partition,
        share_connection = ShareConnections
    } = State,
    ConnId = case ShareConnections of
        false ->
            {consumer, Topic, Partition};
        true ->
            {consumer, Topic}
    end,
    case nkkafka_brokers:get_connection({SrvId, BrokerId, ConnId}) of
        {ok, ConsumerPid} ->
            monitor(process, ConsumerPid),
            {ok, State#state{kafka_pid = ConsumerPid}};
        {error, Error} ->
            ?LLOG(error, "could not start connection for ~s:~p", [Topic, Error], State),
            {error, Error}
    end.


%% @private
find_offset(Pos, #state{srv=SrvId}=State) ->
    #state{topic=Topic, partition=Partition} = State,
    case nkkafka:get_offset(SrvId, Topic, Partition, Pos) of
        {ok, NextOffset} ->
            % Kafka returns next offset
            NextOffset;
        {error, Error} ->
            ?LLOG(error, "could not find last offset: ~p", [Error], State),
            error({last_offset_error, Error})
    end.


%% @private
find_stored_offset(State) ->
    #state{kafka_pid =Pid, topic=Topic, partition=Part, store_group =Group} = State,
    case nkkafka_groups:offset_fetch(Pid, Group, Topic, Part) of
        {ok, _Meta, Offset} ->
            max(Offset, 0);
        {error, Error} ->
            ?LLOG(error, "could not find last stored offset: ~p", [Error], State),
            error({last_offset_error, Error})
    end.


%% @private
read_messages(State) ->
    #state{next_offset = Offset} = State,
    Start = nklib_date:epoch(msecs),
    case nklib_util:do_config_get(nkkafka_subscribers_paused) of
        true ->
            State;
        _ ->
            case fetch(State) of
                {ok, #{total:=0}} ->
                    %Time = nklib_date:epoch(msecs) - Start,
                    %?LLOG(info, "no messages (~p)", [Time], State),
                    State;
                {ok, #{hw_offset:=Last, total:=Total, messages:=Msgs}} ->
                    Time1 = nklib_date:epoch(msecs),
                    State2 = process_messages(Msgs, State),
                    Time2 = nklib_date:epoch(msecs),
                    ?LLOG(notice, "read ~p messages from offset ~p to ~p (up to ~p) (~pmsecs+~pmsecs).",
                          [Total, Offset, State2#state.next_offset-1, Last-1, Time1-Start, Time2-Time1], State),
                    insert_offset(State2),
                    read_messages(State2);
                {error, Error} ->
                    % It the protocol process fails, we will detect and retry later
                    ?LLOG(error, "could not read from offset ~p: ~p", [Offset, Error], State),
                    State
            end
    end.


%% @private
fetch(State) ->
    #state{
        topic = Topic,
        partition = Partition,
        next_offset = Offset,
        kafka_pid = ConsumerPid
    } = State,
    Request = #req_fetch{
        topics = [
            #req_fetch_topic{
                name = Topic,
                partitions = [
                    #req_fetch_partition{
                        partition = Partition,
                        offset = Offset,
                        max_bytes = ?FETCH_MAX_BYTES
                    }
                ]
            }
        ],
        max_wait = 1000,
        min_bytes = 0
    },
    case nkkafka_brokers:send_request(ConsumerPid, Request) of
        {ok, #{Topic:=#{Partition:=#{error:=Error}}}} ->
            {error, {partition_error, Error}};
        {ok, #{Topic:=#{Partition:=Data}}} ->
            {ok, Data};
        {error, Error} ->
            {error, Error}
    end.



%% @private
process_messages([], State) ->
    State;

process_messages([{Offset, Key, Value}|Rest], State) ->
    % Time is msecs
    #state{srv=SrvId, topic=Topic, partition=Part} = State,
    State2 = State#state{next_offset = Offset+1},
    Meta = #{topic=>Topic, partition=>Part, key=>Key, offset=>Offset},
    Fun = fun() -> ?CALL_SRV(SrvId, kafka_message, [Topic, Value, Meta]) end,
    case nklib_util:do_try(Fun) of
        ok ->
            process_messages(Rest, State2);
        {exception, {Class, {Error, Trace}}} ->
            ?LLOG(warning, "error calling kafka message: ~p:~p (~p)", [Class, Error, Trace], State),
            timer:sleep(1000),
            State2;
        Other ->
            ?LLOG(warning, "invalid response from kafka message: ~p", [Other], State),
            timer:sleep(1000),
            State2
    end.


%% @private
insert_offset(#state{store_offsets=true, store_group=Group}=State) ->
    #state{
        kafka_pid = Pid,
        topic = Topic,
        partition = Partition,
        next_offset = Next
    } = State,
    case nkkafka_groups:offset_commit(Pid, Group, Topic, Partition, Next, <<"nkkafka">>) of
        ok ->
            ok;
        {error, Error} ->
            ?LLOG(warning, "could not store offset: ~p", [Error], State)
    end;

insert_offset(_State) ->
    ok.

