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

%% @doc NkKAFKA application

-module(nkkafka_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([produce/2, produce/3, produce_sync/2, produce_sync/3]).
-export([consume_ack/2]).
-export([do_subscribe/5, do_unsubscribe/2]).
-export([send_callback_ack/1]).
-export([get_metadata/1, resolve_offset/3, resolve_offset/4, fetch/4, fetch/7]).
-export([expand_messages/1]).


-include_lib("nkserver/include/nkserver.hrl").
-include_lib("brod/include/brod.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type offset_time() :: nklib_util:m_timestamp() | earliest | lastest.
-type metadata() ::
    #{
        brokers => [#{host=>binary, port=>integer()}],
        cluster_id => binary,
        topics => #{
            TopicName::binary() => #{
                partitions => #{
                    PartId::integer() => map()
                }
            }
        }

    }.

%% ===================================================================
%% API
%% ===================================================================


%% @doc
-spec get_metadata(nkservice:id()) ->
    {ok, metadata()} | {error, term()}.

get_metadata(SrvId) ->
    Hosts = find_hosts(SrvId),
    case brod:get_metadata(Hosts) of
        {ok, Meta} ->
            expand_metadata(Meta);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets last offset for a partition
-spec resolve_offset(nkservice:id(), nkkafka:topic(), nkkafka:partion()) ->
    {ok, integer()} | {error, unknown_topic_or_partition|term()}.

resolve_offset(SrvId, Topic, Partition) ->
    Hosts = find_hosts(SrvId),
    brod:resolve_offset(Hosts, to_bin(Topic), Partition).


%% @doc
%% For new topics, is 0
%% For time is answer -1!
-spec resolve_offset(nkservice:id(), nkkafka:topic(),
    nkkafka:partion(), offset_time()) ->
    {ok, integer()} | {error, term()}.

resolve_offset(SrvId, Topic, Partition, Time) ->
    Hosts = find_hosts(SrvId),
    brod:resolve_offset(Hosts, to_bin(Topic), Partition, Time).


%% @doc
-spec fetch(nkservice:id(), nkkafka:topic(), nkkafka:partion(), offset_time()) ->
    {ok, [#kafka_message_set{}]} | {error, offset_out_of_range|term()}.

fetch(SrvId, Topic, Partition, Offset) ->
    fetch(SrvId, Topic, Partition, Offset, 5000, 0, 100000).


%% @doc
fetch(SrvId, Topic, Partition, Offset, WaitTime, MinBytes, MaxBytes) ->
    Hosts = find_hosts(SrvId),
    brod:fetch(Hosts, to_bin(Topic), Partition, Offset, WaitTime, MinBytes, MaxBytes).

%% @doc
-spec expand_messages([#kafka_message_set{}]) ->
    map().

expand_messages(List) ->
    expand_messages(List, []).


%% @equiv produce(Pid, 0, <<>>, Value)
-spec produce(pid(), nkkafka:value()) ->
    {ok, nkkafka:call_ref()} | {error, any()}.

produce(Pid, Value) when is_pid(Pid) ->
    produce(Pid, _Key = <<>>, Value).


%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is discarded (only the keys in kv-list are sent to kafka).
%% The pid should be a partition producer pid, NOT client pid.
-spec produce(pid(), nkkafka:key(), nkkafka:value()) ->
    {ok, nkkafka:call_ref()} | {error, any()}.

produce(Pid, Key, Value) when is_pid(Pid)->
    Value2 = case is_map(Value) of
        true ->
            nklib_json:encode(Value);
        false ->
            Value
    end,
    brod_producer:produce(Pid, Key, Value2).


%% @equiv produce_sync(Pid, 0, <<>>, Value)
-spec produce_sync(pid(), nkkafka:value()) ->
    ok.

produce_sync(Pid, Value) when is_pid(Pid) ->
    produce_sync(Pid, _Key = <<>>, Value).


%% @doc Sync version of produce/3
%% This function will not return until a response is received from kafka,
%% however if producer is started with required_acks set to 0, this function
%% will return once the messages is buffered in the producer process.
%% @end
-spec produce_sync(pid(), nkkafka:key(), nkkafka:value()) ->
    ok | {error, any()}.

produce_sync(Pid, Key, Value) when is_pid(Pid) ->
    case produce(Pid, Key, Value) of
        {ok, CallRef} ->
            %% Wait until the request is acked by kafka
            brod:sync_produce_request(CallRef);
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc
-spec consume_ack(pid(), nkkafka:offset()) ->
    ok | {error, term()}.

consume_ack(ConsumerPid, Offset) ->
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




%% ===================================================================
%% Internal
%% ===================================================================


%% @private
expand_metadata(#{topic_metadata:=Topics1}=Meta) ->
    {Topics1, Meta2} = maps:take(topic_metadata, Meta),
    Topics2 = expand_metadata_topics(Topics1, #{}),
    Meta2#{topics => Topics2}.


%% @private
expand_metadata_topics([], Acc) ->
    Acc;

expand_metadata_topics([TopicData|Rest], Acc) ->
    {Topic, Data} = maps:take(topic, TopicData),
    {PartMeta, Data2} = maps:take(partition_metadata, Data),
    PartData = expand_metadata_partitions(PartMeta, #{}),
    expand_metadata_topics(Rest, Acc#{Topic => Data2#{partitions=>PartData}}).


%% @private
expand_metadata_partitions([], Acc) ->
    Acc;

expand_metadata_partitions([PartitionData|Rest], Acc) ->
    {Partition, Data} = maps:take(partition, PartitionData),
    expand_metadata_partitions(Rest, Acc#{Partition => Data}).



%% @private
expand_messages([], Acc) ->
    lists:reverse(Acc);

expand_messages([R|Rest], Acc) ->
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
    expand_messages(Rest, [Msg|Acc]).


%% @private
find_hosts(SrvId) ->
    #{nodes:=Nodes} = ?CALL_SRV(SrvId, config, []),
    [{Node, Port} || #{host:=Node, port:=Port} <- Nodes].


%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).

