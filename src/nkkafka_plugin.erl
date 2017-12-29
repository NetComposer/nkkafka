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

%% @doc NkKAFKA callbacks

-module(nkkafka_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_syntax/0, plugin_config/2, plugin_start/2]).
-export([start_client/2, client_syntax_check/1]).

%% ===================================================================
%% Plugin callbacks
%%
%% These are used when NkKAFKA is started as a NkSERVICE plugin
%% ===================================================================

%% @private
plugin_deps() ->
    [].


%% @private
plugin_syntax() ->
	#{
	    nkkafka =>
            {list, #{
                nodes => {list, #{
                    host => host,
                    port => {integer, 1, 65535},
                    '__defaults' => #{host=><<"127.0.0.1">>, port=>9092}
                }},
                clients => {list, #{
                    id => binary,
                    type => {atom, [raw, producer, consumer_group]},
                    topics => {list, binary},
                    group_id => binary,
                    %% @see nkkafka:client_config()
                    client_config => #{
                        restart_delay_seconds => integer,
                        max_metadata_sock_retry => integer,
                        get_metadata_timeout_seconds => integer,
                        reconnect_cool_down_seconds => integer,
                        allow_topic_auto_creation => boolean,
                        auto_start_producers => boolean,
                        %ssl => boolean() | #{certfile=>binary(), keyfile=>binary(), cacertfile=>binary()},
                        %sasl => undefined | {plain, User::string(), Pass::string()},
                        connect_timeout => integer,
                        request_timeout => integer
                    },
                    %% @see nkkafka:producer_config()
                    producer_config => #{
                        required_acks => {integer, -1, 1},
                        ack_timeout => integer,
                        partition_buffer_limit => integer,
                        partition_onwire_limit => integer,
                        max_batch_size => integer,
                        max_retries => integer,
                        retry_backoff_ms => integer,
                        compression => {atom, [no_compression, gzip, snappy]},
                        min_compression_batch_size => integer,
                        max_linger_ms => integer,
                        max_linger_count => integer

                    },
                    %% @see nkkafka:consumer_config()
                    consumer_config => #{
                        min_bytes => pos_integer,
                        max_bytes => pos_integer,
                        max_wait_time => pos_integer,
                        sleep_timeout => pos_integer,
                        prefetch_count => pos_integer,
                        begin_offset => [{atom, [latest]}, integer],
                        offset_reset_policy => {atom, [reset_by_subscriber, reset_to_earliest, reset_to_latest]},
                        size_stat_window => integer
                    },
                    %% @see nkkafka:group_config()
                    group_config => #{
                        partition_assignment_strategy => {atom, [roundrobin, callback_implemented]},
                        session_timeout_seconds => integer,
                        heartbeat_rate_seconds => integer,
                        max_rejoin_attempts => integer,
                        rejoin_delay_seconds => integer,
                        offset_commit_policy => {atom, [commit_to_kafka_v2, consumer_managed]},
                        offset_commit_interval_seconds => integer,
                        offset_retention_seconds => integer,
                        protocol_name => binary
                    },
                    '__mandatory' => [id, type],
                    '__post_check' => fun ?MODULE:client_syntax_check/1
                }},
                '__mandatory' => [nodes, clients]
           }}
}.


%% @private
plugin_config(#{nkkafka:=List}=Config, #{id:=SrvId}) ->
    case parse_clusters(SrvId, List, #{}) of
        {ok, Clients} ->
            Cache1 = [{Id, BrodId} || {Id, #{brod_client_id:=BrodId}} <- maps:to_list(Clients)],
            Cache2 = maps:from_list(Cache1),
            {ok, Config#{nkkafka_clients=>Clients}, Cache2};
        {error, Error} ->
            {error, Error}
    end;

plugin_config(Config, _Service) ->
    {ok, Config}.


%% @private
%% We cannot start the clients here, since they can start sending messages and
%% the service callback is not yet created

plugin_start(Config, _Service) ->
    {ok, Config}.



%% ===================================================================
%% Util
%% ===================================================================

%% @private
parse_clusters(_SrvId, [], Acc) ->
    {ok, Acc};

parse_clusters(SrvId, [#{clients:=Clients, nodes:=Nodes}|Rest], Acc) ->
    Nodes2 = [{nklib_util:to_list(Host), Port} || #{host:=Host, port:=Port} <- Nodes],
    case do_parse_clusters(SrvId, Clients, Nodes2, Acc) of
        {ok, Acc2} ->
            parse_clusters(SrvId, Rest, Acc2);
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_parse_clusters(_SrvId, [], _Nodes, Acc) ->
    {ok, Acc};

do_parse_clusters(SrvId, [Client|Rest], Nodes, Acc) ->
    {Id, Client2} = maps:take(id, Client),
    case maps:is_key(Id, Acc) of
        false ->
            BrodClientId1 = nklib_util:bjoin([SrvId, "nkkafka", Id], <<"_">>),
            BrodClientId2 = binary_to_atom(BrodClientId1, utf8),
            Data = Client2#{brod_client_id=>BrodClientId2, nodes=>Nodes},
            do_parse_clusters(SrvId, Rest, Nodes, Acc#{Id=>Data});
        true ->
            {error, duplicated_id}
    end.



%% @private
start_client(SrvId, #{brod_client_id:=BrodId, type:=raw, nodes:=Nodes}=Client) ->
    Config = maps:to_list(maps:get(client_config, Client, #{})),
    lager:notice("Service '~s' starting raw Kafka client '~s' (~p) (~p)", [SrvId, BrodId, Nodes, Config]),
    {ok, _} = nkservice_srv:start_proc(SrvId, BrodId, brod_client, [Nodes, BrodId, Config]);

start_client(SrvId, #{brod_client_id:=BrodId, type:=producer, topics:=Topics, nodes:=Nodes}=Client) ->
    Config1 = maps:to_list(maps:get(client_config, Client, #{})),
    Config2 = maps:to_list(maps:get(producer_config, Client, #{})),
    lager:notice("Service '~s' starting producer Kafka client '~s' ~p (~p) (~p) (~p)",
                 [SrvId, BrodId, Topics, Nodes, Config1, Config2]),
    {ok, _} = nkservice_srv:start_proc(SrvId, BrodId, brod_client, [Nodes, BrodId, Config1]),
    timer:sleep(100),
    lists:foreach(
        fun(Topic) -> ok = brod:start_producer(BrodId, Topic, Config2) end,
        Topics);

start_client(SrvId, #{brod_client_id:=BrodId, type:=consumer_group, group_id:=GroupId, topics:=Topics, nodes:=Nodes}=Client) ->
    Config1 = maps:to_list(maps:get(client_config, Client, #{})),
    Config2 = maps:to_list(maps:get(consumer_config, Client, #{})),
    Config3 = maps:to_list(maps:get(group_config, Client, #{})),
    lager:notice("Service '~s' starting consumer group Kafka client '~s' (~s) ~p (~p) (~p) (~p) (~p)",
                 [SrvId, BrodId, GroupId, Topics, Nodes, Config1, Config2, Config3]),
    {ok, _} = nkservice_srv:start_proc(SrvId, BrodId, brod_client, [Nodes, BrodId, Config1]),
    Args = [BrodId, GroupId, Topics, Config3, Config2, message_set, nkkafka_member_callback, {SrvId, #{}}],
    {ok, _} = nkservice_srv:start_proc(SrvId, GroupId, brod_group_subscriber, Args),
    ok.


client_syntax_check(Client) ->
    case maps:from_list(Client) of
        #{type:=producer, topics:=_} ->
            ok;
        #{type:=producer} ->
            {error, {missing_field, <<"topics">>}};
        #{type:=consumer_group, group_id:=_, topics:=_} ->
            ok;
        #{type:=consumer_group, group_id:=_} ->
            {error, {missing_field, <<"topics">>}};
        #{type:=consumer_group} ->
            {error, {missing_field, <<"group_id">>}};
        _ ->
            ok
    end.
