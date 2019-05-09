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

%% @doc

-module(nkkafka).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([get_metadata/1, get_partitions/2, get_offset/4]).
-export([produce/5, fetch/5, get_leader/3, get_topic_leaders/2]).
-export([pause_subscribers/2, subscribers_paused/1]).

-include("nkkafka.hrl").
%-include_lib("nkpacket/include/nkpacket.hrl").

-define(LLOG(Type, Txt, Args),lager:Type("NkKAFKA "++Txt, Args)).

%% ===================================================================
%% Types
%% ===================================================================


-type broker_id() :: pos_integer().

-type topic() :: binary() | string() | atom().

-type partition() :: pos_integer().

-type offset() :: integer().

-type topics() ::
    #{topic() := #{partitions:=partitions(), error=>atom()}}.

-type partitions() ::
    #{
        partition() := #{
            leader := broker_id(),
            isr := [broker_id()],
            replicas := [broker_id()],
            error => atom()
        }
    }.

-type key() :: binary() | string() | atom() | integer().

-type value() :: binary() | string() | atom() | integer().

-type message() :: {offset(), key(), value()}.

-type produce_opts() ::
    #{
        key => binary(),                % Default <<>>
        ack => none | leader | all,     % Default leader.
        timeout => integer(),           % Default 10s
        tries => integer()
    }.

-type fetch_opts() ::
    #{
        max_wait => integer(),
        min_bytes => integer(),
        max_bytes => integer()
    }.


%% ===================================================================
%% Public
%% ===================================================================

%% @doc Get full metadata
-spec get_metadata(nkserver:id()) ->
    {ok, nkserver:id(), topics()} | {error, term()}.

get_metadata(SrvId) ->
    nkkafka_util:update_metadata(SrvId).


%% @doc Get the number of partitions for a topic, and stores cache
-spec get_partitions(nkserver:id(), topic()) ->
    {ok, integer()} | {error, term()}.

get_partitions(SrvId, Topic) ->
    Topic2 = to_bin(Topic),
    case nkkafka_util:cached_partitions(SrvId, Topic2) of
        NumParts when is_integer(NumParts) ->
            {ok, NumParts};
        undefined ->
            case get_topic_leaders(SrvId, Topic2) of
                {ok, Parts} ->
                    {ok, map_size(Parts)};
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @doc Get the first or last offset for all partitions of a topic
-spec get_offset(nkserver:id(), topic(), partition(), first|last) ->
    {ok, #{partition() := offset() | {error, integer()}}} | {error, term()}.

get_offset(SrvId, Topic, Partition, Pos) ->
    Topic2 = to_bin(Topic),
    case get_leader(SrvId, Topic2, Partition) of
        {ok, BrokerId} ->
            Req = #req_offset{
                topics = [
                    #req_offset_topic{
                        name = Topic2,
                        partitions = [
                            #req_offset_partition{
                                partition = Partition,
                                time = Pos
                            }
                        ]
                    }
                ]
            },
            % Use shared connection for this broker
            case nkkafka_broker:send_request({SrvId, BrokerId}, Req) of
                {ok, #{Topic2:=#{Partition:=#{error:=Error}}}} ->
                    {ok, Error};
                {ok, #{Topic2:=#{Partition:=#{offsets:=[Offset]}}}} ->
                    {ok, Offset};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Produce a new message. Returns offset and broker pid
%% Its is very inefficient sending messages one by one
%% Use nkkafka_producer:produce()
-spec produce(nkserver:id(), topic(), partition(), value(), produce_opts()) ->
    {ok, {partition(), offset()}} | {error, term()}.

produce(SrvId, Topic, Partition, Value, Opts) when is_atom(SrvId) ->
    Topic2 = to_bin(Topic),
    case get_leader(SrvId, Topic2, Partition) of
        {ok, BrokerId} ->
            Request = #req_produce{
                ack = maps:get(ack, Opts, leader),
                timeout = maps:get(timeout, Opts, 10000),
                topics = [
                    #req_topic{
                        name = Topic2,
                        partitions = [
                            #req_produce_partition{
                                partition = Partition,
                                messages = [
                                    #req_message{
                                        key = to_bin(maps:get(key, Opts, <<>>)),
                                        value = to_bin(Value)
                                    }
                                ]
                            }
                        ]
                    }
                ]
            },
            case nkkafka_broker:send_request({SrvId, BrokerId}, Request) of
                {ok, #{Topic2:=#{Partition:=#{error:=Error}}}} ->
                    {error, Error};
                {ok, #{Topic2:=#{Partition:=#{offset:=Offset}}}} ->
                    {ok, {Partition, Offset}};
                {ok, no_ack} ->
                    {ok, {Partition, unknown}};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% @doc
-spec fetch(nkserver:id(), topic(), partition(), offset(), fetch_opts()) ->
    {ok, #{total:=integer(), messages:=[message()]}} | {error, term()}.

fetch(SrvId, Topic, Partition, Offset, Opts) when is_atom(SrvId) ->
    Topic2 = to_bin(Topic),
    case get_leader(SrvId, Topic2, Partition) of
        {ok, BrokerId} ->
            Request = #req_fetch{
                topics = [
                    #req_fetch_topic{
                        name = Topic2,
                        partitions = [
                            #req_fetch_partition{
                                partition = Partition,
                                offset = Offset,
                                max_bytes = maps:get(max_bytes, Opts, 1024*1024)
                            }
                        ]
                    }
                ],
                max_wait = maps:get(max_wait, Opts, 100),
                min_bytes = maps:get(min_bytes, Opts, 0)
            },
            case nkkafka_broker:send_request({SrvId, BrokerId}, Request) of
                {ok, #{Topic2:=#{Partition:=#{error:=Error}}}} ->
                    {error, {partition_error, Error}};
                {ok, #{Topic2:=#{Partition:=Data}}} ->
                    {ok, Data};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Sets all subscribers in paused mode
pause_subscribers(SrvId, Paused) when is_boolean(Paused) ->
    nklib_util:do_config_put({nkkafka_subscribers_paused, SrvId}, Paused).


%% @doc Gets if subscribers are paused
subscribers_paused(SrvId) ->
    nklib_util:do_config_get({nkkafka_subscribers_paused, SrvId}, false).




%% ===================================================================
%% Internal
%% ===================================================================


%% @private
get_leader(SrvId, Topic, Partition) ->
    case get_topic_leaders(SrvId, to_bin(Topic)) of
        {ok, #{Partition:=BrokerId}} ->
            {ok, BrokerId};
        {ok, #{}} ->
            {error, partition_unknown};
        {error, Error} ->
            {error, Error}
    end.


%% @private It will create the topic
get_topic_leaders(SrvId, Topic) ->
    do_get_topic_leaders(SrvId, Topic, 10).


%% @private It will create the topic
do_get_topic_leaders(SrvId, Topic, Tries) when Tries > 0 ->
    Topic2 = to_bin(Topic),
    case nkkafka_util:cached_leaders(SrvId, Topic2) of
        #{} = Leaders->
            {ok, Leaders};
        undefined ->
            case nkkafka_util:update_metadata(SrvId, Topic2) of
                {ok, {_, #{Topic2:=#{error:=Error}}}} ->
                    lager:notice("error getting topic ~s (~p): retrying", [Topic2, Error]),
                    timer:sleep(500),
                    do_get_topic_leaders(SrvId, Topic2, Tries-1);
                {ok, {_, #{Topic2:=#{partitions:=_}}}} ->
                    do_get_topic_leaders(SrvId, Topic2, Tries-1);
                {error, Error} ->
                    ?LLOG(notice, "error getting topic ~s (~p): retrying", [Topic2, Error]),
                    timer:sleep(500),
                    do_get_topic_leaders(SrvId, Topic2, Tries-1)
            end
    end;

do_get_topic_leaders(_SrvId, _Topic, _Tries) ->
    {error, {topic_leaders_error, _Topic}}.



%% @private
to_bin(R) when is_binary(R) -> R;
to_bin(R) -> nklib_util:to_binary(R).


