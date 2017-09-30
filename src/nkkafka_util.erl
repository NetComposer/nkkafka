%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Carlos Gonzalez Florido.  All Rights Reserved.
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
-export([get_metadata/2, get_metadata/3, resolve_offset/5, fetch/5, fetch/8]).
-export([process_messages/1]).

-include_lib("brod/include/brod.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type offset_time() :: nklib_util:m_timestamp() | earliest | lastest.


%% ===================================================================
%% API
%% ===================================================================

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


fetch(SrvId, ClientId, Topic, Partition, Offset) ->
    fetch(SrvId, ClientId, Topic, Partition, Offset, 1000, 0, 10000).


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
        {ok, #{host:=Host, port:=Port}} ->
            {ok, [{binary_to_list(Host), Port}]};
        error ->
            {error, client_not_found}
    end.


%%%% @private
%%find_client(SrvId, ClientId) ->
%%    Clients = SrvId:config_nkkafka(),
%%    case maps:find(to_bin(ClientId), Clients) of
%%        {ok, Name} ->
%%            {ok, Name};
%%        error ->
%%            {error, client_not_found}
%%    end.


%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).

