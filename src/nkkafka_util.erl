%% -------------------------------------------------------------------
%%
%% Copyright (c) 2018 Carlos Gonzalez Florido.  All Rights Reserved.
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

%% @doc NkKAFKA application

-module(nkkafka_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([do_produce/2, do_produce/3, do_produce_sync/2, do_produce_sync/3]).
-export([do_consume_ack/2]).
-export([do_subscribe/5, do_unsubscribe/2]).
-export([send_callback_ack/1]).
-export([get_metadata/2, get_metadata/3, resolve_offset/4, resolve_offset/5, fetch/5, fetch/8]).
-export([process_messages/1]).

-include_lib("brod/include/brod.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type offset_time() :: nklib_util:m_timestamp() | earliest | lastest.


%% ===================================================================
%% API
%% ===================================================================


%% @equiv produce(Pid, 0, <<>>, Value)
-spec do_produce(pid(), nkkafka:value()) ->
    {ok, nkkafka:call_ref()} | {error, any()}.

do_produce(Pid, Value) when is_pid(Pid) ->
    do_produce(Pid, _Key = <<>>, Value).


%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is discarded (only the keys in kv-list are sent to kafka).
%% The pid should be a partition producer pid, NOT client pid.
-spec do_produce(pid(), nkkafka:key(), nkkafka:value()) ->
    {ok, nkkafka:call_ref()} | {error, any()}.

do_produce(Pid, Key, Value) when is_pid(Pid)->
    Value2 = case is_map(Value) of
        true ->
            nklib_json:encode(Value);
        false ->
            Value
    end,
    brod_producer:produce(Pid, Key, Value2).


%% @equiv produce_sync(Pid, 0, <<>>, Value)
-spec do_produce_sync(pid(), nkkafka:value()) ->
    ok.

do_produce_sync(Pid, Value) when is_pid(Pid) ->
    do_produce_sync(Pid, _Key = <<>>, Value).


%% @doc Sync version of produce/3
%% This function will not return until a response is received from kafka,
%% however if producer is started with required_acks set to 0, this function
%% will return once the messages is buffered in the producer process.
%% @end
-spec do_produce_sync(pid(), nkkafka:key(), nkkafka:value()) ->
    ok | {error, any()}.

do_produce_sync(Pid, Key, Value) when is_pid(Pid) ->
    case do_produce(Pid, Key, Value) of
        {ok, CallRef} ->
            %% Wait until the request is acked by kafka
            brod:sync_produce_request(CallRef);
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc
-spec do_consume_ack(pid(), nkkafka:offset()) ->
    ok | {error, term()}.

do_consume_ack(ConsumerPid, Offset) ->
    brod:consume_ack(ConsumerPid, Offset).




%% @doc
-spec do_subscribe(pid(), pid(), nkkafka:topic(), nkkafka:partition(), nkkafka:consumer_config()) ->
    {ok, pid()} | {error, term()}.

do_subscribe(ConsumerPid, SubscriberPid, Topic, Partition, ConsumerConfig) ->
    brod:subscribe(ConsumerPid, SubscriberPid, to_bin(Topic), Partition, maps:to_list(ConsumerConfig)).


%% @doc
-spec do_unsubscribe(pid(), pid()) ->
    ok | {error, term()}.

do_unsubscribe(ConsumerPid, SubscriberPid) ->
    brod:unsubscribe(ConsumerPid, SubscriberPid).




%% @doc Must be called from group subscribers callbacks
-spec send_callback_ack(nkkafka:msg()) ->
    ok.

send_callback_ack(#{pid:=Pid, topic:=Topic, partition:=Partition, offset:=Offset}) ->
    nkkafka:group_subscriber_ack(Pid, Topic, Partition, Offset).


%% @doc
-spec get_metadata(nkservice:id(), nkkafka:client_id()) ->
    {ok, map()} | {error, term()}.

get_metadata(SrvId, ClientId) ->
    get_metadata(SrvId, ClientId, []).


%% @doc
-spec get_metadata(nkservice:id(), nkkafka:client_id(), [nkkafka:topic()]) ->
    {ok, map()} | {error, term()}.

get_metadata(SrvId, ClientId, Topics) ->
    case find_hosts(SrvId, ClientId) of
        {ok, Hosts} ->
            case brod:get_metadata(Hosts, [to_bin(T) || T<-Topics]) of
                {ok, Meta} ->
                    {ok, expand_metadata(Meta)};
                Other ->
                    Other
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets last offset for a partition
-spec resolve_offset(nkservice:id(), nkkafka:client(), nkkafka:topic(),
                     nkkafka:partion()) ->
    {ok, integer()} | {error, term()}.

resolve_offset(SrvId, ClientId, Topic, Partition) ->
    case find_hosts(SrvId, ClientId) of
        {ok, Hosts} ->
            brod:resolve_offset(Hosts, to_bin(Topic), Partition);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec resolve_offset(nkservice:id(), nkkafka:client(), nkkafka:topic(),
                     nkkafka:partion(), offset_time()) ->
    {ok, integer()} | {error, term()}.

resolve_offset(SrvId, ClientId, Topic, Partition, Time) ->
    case find_hosts(SrvId, ClientId) of
        {ok, Hosts} ->
            brod:resolve_offset(Hosts, to_bin(Topic), Partition, Time);
        {error, Error} ->
            {error, Error}
    end.



%% @doc
fetch(SrvId, ClientId, Topic, Partition, Offset) ->
    fetch(SrvId, ClientId, Topic, Partition, Offset, 5000, 0, 100000).


%% @doc
fetch(SrvId, ClientId, Topic, Partition, Offset, WaitTime, MinBytes, MaxBytes) ->
    case find_hosts(SrvId, ClientId) of
        {ok, Hosts} ->
            case brod:fetch(Hosts, to_bin(Topic), Partition, Offset, WaitTime, MinBytes, MaxBytes) of
                {ok, List} ->
                    {ok, process_messages(List)};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc
process_messages(List) ->
    process_messages(List, []).








%% ===================================================================
%% Internal
%% ===================================================================

%% @private
expand_metadata(List) ->
    Map1 = maps:from_list(List),
    Map2 = case Map1 of
        #{brokers:=Brokers} ->
            Map1#{brokers:=[maps:from_list(B) || B <- Brokers]};
        _ ->
            Map1
    end,
    Map3 = case Map2 of
        #{topic_metadata:=MetaData} ->
            Map2#{topic_metadata:=[expand_topic_metadata(M) || M <- MetaData]};
        _ ->
            Map2
    end,
    Map3.


%% @private
expand_topic_metadata(List) ->
    case maps:from_list(List) of
        #{partition_metadata:=Meta2}=Map1 ->
            Map1#{partition_metadata:=[maps:from_list(M) || M <-Meta2]};
        Map1 ->
            Map1
    end.


%% @private
process_messages([], Acc) ->
    lists:reverse(Acc);

process_messages([R|Rest], Acc) ->
    #kafka_message{
        offset = Offset,
        key = Key,
        value = Value,
        ts_type = Type,         % create | append | undefined
        ts = Ts
    } = R,
    % lager:notice("P: ~p", [lager:pr(R, ?MODULE)]),
    Msg = #{
        key => Key,
        value => Value,
        offset => Offset,
        ts => Ts,
        ts_type => Type
    },
    process_messages(Rest, [Msg|Acc]).


%% @private
find_hosts(SrvId, ClientId) ->
    #{nkkafka_clients:=Clients} = SrvId:config(),
    case maps:find(to_bin(ClientId), Clients) of
        {ok, #{nodes:=Nodes}} ->
            {ok, Nodes};
        error ->
            {error, client_not_found}
    end.


%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).

