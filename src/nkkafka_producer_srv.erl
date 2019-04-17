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

%% Produces 100.000/s with 400byte payload (ack=none)
%% 71.000/s (ack=leader)
%% Slightly better if opens a connection for each partition (removed here)

-module(nkkafka_producer_srv).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([sync/2, async/2, get_all/0]).
-export([find/2, start/2, start_link/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).

-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkKAFKA Producer (~s:~s) "++Txt,
        [State#state.srv, State#state.topic | Args])).


-include("nkkafka.hrl").
-include_lib("nkserver/include/nkserver.hrl").


-define(TIMEOUT, 60000).




%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% Public
%% ===================================================================


%% @doc Starts producer and insert message
sync(Pid, Req) ->
    nklib_util:call2(Pid, Req, infinity).


%% @doc Starts producer and insert message
async(Pid, Req) ->
    gen_server:cast(Pid, Req).


%% @private
get_all() ->
    nklib_proc:values(?MODULE).


%% @doc
find(SrvId, Topic) ->
    case nklib_proc:values({?MODULE, SrvId, Topic}) of
        [{_, Pid}] ->
            {ok, Pid};
        [] ->
            undefined
    end.

%% @doc
start(SrvId, Topic) ->
    lager:notice("NkKAFKA starting producer ~s (~s)", [Topic, SrvId]),
    Spec = #{
        id => {producer, Topic},
        start => {nkkafka_producer_srv, start_link, [SrvId, Topic]},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [nkkafa_producer_srv]
    },
    case nkserver_workers_sup:update_child2(SrvId, Spec, #{}) of
        {ok, _, Pid} ->
            {ok, Pid};
        {error, Error} ->
            {error, Error}
    end.



% ===================================================================
%% gen_server behaviour
%% ===================================================================

-record(state, {
    srv :: nkserver:id(),
    topic :: binary(),
    partitions :: integer(),
    leaders :: #{nkkafka:partition() => nkkafka:broker_id()},
    brokers :: #{nkkafka:broker_id() => pid()},
    base :: #req_produce{},
    max_size :: integer(),
    messages :: map(),
    sizes :: map(),
    offsets :: map(),
    timer :: reference() | undefined
}).


%% @private
init([SrvId, Topic, Leaders]) ->
    true = nklib_proc:reg({?MODULE, SrvId, Topic}),
    nklib_proc:put(?MODULE, {SrvId, Topic}),
    Base = #req_produce{
        ack = nkserver:get_cached_config(SrvId, nkkafka, producer_ack),
        timeout = nkserver:get_cached_config(SrvId, nkkafka, producer_timeout)
    },
    State = #state{
        srv = SrvId,
        topic = Topic,
        partitions = map_size(Leaders),
        leaders = Leaders,
        brokers = #{},
        base = Base,
        max_size = nkserver:get_cached_config(SrvId, nkkafka, producer_queue_size),
        messages = #{},
        sizes = #{},
        offsets = #{}
    },
    self() ! do_send,
    {ok, reset_timeout(State)}.


%% @private
handle_call(get_info, _From, State) ->
    #state{leaders=Leaders, brokers=Brokers, messages=Messages, offsets=Offsets} = State,
    Data = #{
        leaders => Leaders,
        brokers => Brokers,
        messages => Messages,
        offsets => Offsets
    },
    {reply, {ok, Data}, State};

handle_call({insert, Key, Msg}, _From, State) ->
    State2 = insert(Key, Msg, State),
    {reply, ok, reset_timeout(State2)};

handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_cast({insert, Key, Msg}, State) ->
    State2 = insert(Key, Msg, State),
    {noreply, reset_timeout(State2)};

handle_cast(Msg, State) ->
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_info(do_send, State) ->
    State2 = do_send(State),
    erlang:send_after(1000, self(), do_send),
    {noreply, State2};

handle_info(insert_timeout, State) ->
    ?LLOG(notice, "producer timeout", [], State),
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{brokers=Brokers}=State) ->
    Brokers2 = maps:to_list(Brokers),
    case lists:keytake(Pid, 2, Brokers2) of
        {value, {BrokerId, Pid}, Brokers3} ->
            ?LLOG(info, "broker ~p is down (~p)", [BrokerId, Reason], State),
            State2 = State#state{brokers=maps:from_list(Brokers3)},
            {noreply, State2};
        false ->
            lager:warning("Module ~p received unexpected down: ~p", [?MODULE, Pid, State]),
            {noreply, State}
    end;

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
start_link(SrvId, Topic) ->
    case nkkafka:get_topic_leaders(SrvId, Topic) of
        {ok, Leaders} ->
            gen_server:start_link(?MODULE, [SrvId, Topic, Leaders], []);
        {error, Error} ->
            {error, Error}
    end.


%% @private
connect_broker(BrokerId, #state{srv=SrvId, topic=Topic, brokers=Brokers}=State) ->
    case nkkafka_brokers:get_connection({SrvId, BrokerId, {producer, Topic}}) of
        {ok, Pid} ->
            monitor(process, Pid),
            Brokers2 = Brokers#{BrokerId => Pid},
            {ok, Pid, State#state{brokers = Brokers2}};
        {error, Error} ->
            ?LLOG(error, "could not connect to broker ~p: ~p", [BrokerId, Error], State),
            {error, Error}
    end.


%% @private
insert(Key, Msg, State) ->
    #state{
        partitions = NumParts,
        max_size = QueueSize,
        messages = Msgs,
        sizes = Sizes
    } = State,
    Part = nklib_date:epoch(usecs) rem NumParts,
    PartMsgs1 = maps:get(Part, Msgs, []),
    Msg2 = #req_message{key=Key, value=Msg},
    PartMsgs2 = [Msg2|PartMsgs1],
    Msgs2 = Msgs#{Part => PartMsgs2},
    Size = maps:get(Part, Sizes, 0),
    Sizes2 = Sizes#{Part => Size+1},
    State2 = State#state{messages = Msgs2, sizes=Sizes2},
    case Size >= QueueSize of
        true ->
            do_send(State2);
        false ->
            State2
    end.


%% @private
do_send(#state{messages=Msgs}=State) ->
    do_send(maps:to_list(Msgs), State).


%% @private
do_send([], State) ->
    State;

do_send([{_Partition, []}|Rest], State) ->
    do_send(Rest, State);

do_send([{Partition, Msgs}|Rest], #state{messages=Messages, sizes=Sizes, offsets=Offsets}=State) ->
    case do_send_partition(Partition, lists:reverse(Msgs), State) of
        {ok, Offset, State2} ->
            State3 = State2#state{
                messages = Messages#{Partition := []},
                sizes = Sizes#{Partition := 0},
                offsets = Offsets#{Partition => Offset}
            },
            do_send(Rest, State3);
        {error, message_size_too_large, State2} ->
            Total = length(Msgs),
            {Msgs2, Msgs3} = lists:split(Total div 2, Msgs),
            ?LLOG(notice, "splitting set too large (~p)", [Total], State2),
            do_send([{Partition, Msgs2}, {Partition, Msgs3}|Rest], State2);
        {error, Error, State2} ->
            ?LLOG(warning, "could not send to partition ~p: ~p", [Partition, Error], State2),
            do_send(Rest, State2)
    end.


%% @private
do_send_partition(Partition, Msgs, #state{leaders=Leaders, brokers=Brokers}=State) ->
    BrokerId = maps:get(Partition, Leaders),
    case maps:find(BrokerId, Brokers) of
        {ok, Pid} ->
            do_send_partition(Pid, Partition, Msgs, State);
        error ->
            case connect_broker(BrokerId, State) of
                {ok, Pid, State2} ->
                    do_send_partition(Pid, Partition, Msgs, State2);
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @private
do_send_partition(Pid, Partition, Msgs, State) ->
    #state{topic=Topic, base=Base} = State,
    Request = Base#req_produce{
        topics = [
            #req_topic{
                name = Topic,
                partitions = [
                    #req_produce_partition{
                        partition = Partition,
                        messages = Msgs
                    }
                ]
            }
        ]
    },
    Start = nklib_date:epoch(msecs),
    case nkkafka_brokers:send_request(Pid, Request) of
        {ok, #{Topic:=#{Partition:=#{error:=Error}}}} ->
            {error, Error, State};
        {ok, #{Topic:=#{Partition:=#{offset:=Offset}}}} ->
            Time = nklib_date:epoch(msecs) - Start,
            ?LLOG(info, "sent ~p messages in ~pmsecs, partition ~p, offset ~p",
                [length(Msgs), Time, Partition, Offset], State),
            {ok, Offset, State};
        {ok, no_ack} ->
            ?LLOG(info, "sent ~p messages, partition ~p",
                [length(Msgs), Partition], State),
            {ok, unknown, State};
        {error, Error} ->
            {error, Error, State}
    end.



reset_timeout(#state{timer=Timer}=State) ->
    nklib_util:cancel_timer(Timer),
    State#state{timer=erlang:send_after(?TIMEOUT, self(), insert_timeout)}.



