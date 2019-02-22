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

%% @doc Default callbacks for plugin definitions
-module(nkkafka_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_config/3, plugin_cache/3,
         plugin_start/3, plugin_update/4, plugin_stop/3]).

-include("nkkafka.hrl").
-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Default implementation
%% ===================================================================


plugin_deps() ->
	[nkserver].


%% @doc
plugin_config(SrvId, Config, #{class:=?PACKAGE_CLASS_KAFKA}) ->
    Syntax = #{
        odes => {list, #{
            host => host,
            port => {integer, 1, 65535},
            '__defaults' => #{host=><<"127.0.0.1">>, port=>9092}
        }},
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
        %% It will be used by producers by default
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
        %% It will be used by consumers (and subscribers?) by default
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
        consumer_group_id => binary,                % It will start if defined
        consumer_group_topics => {list, binary},
        consumer_group_config => #{
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
        debug => {list, {atom, [processor]}},
        '__mandatory' => [nodes],
        '__post_check' => fun ?MODULE:client_syntax_check/1
    },
    case nkserver_util:parse_config(Config, Syntax) of
        {ok, Config2, _} ->
            BrodClientId1 = nklib_util:bjoin([SrvId, "nkkafka"], $_),
            BrodClientId2 = binary_to_atom(BrodClientId1, utf8),
            {ok, Config2#{brod_client_id => BrodClientId2}};
        {error, Error} ->
            lager:error("NKLOG Config ~p", [Config]),
            {error, Error}
    end.


plugin_cache(_SrvId, Config, _Service) ->
    Cache = #{
        brod_client_id => maps:get(brod_client_id, Config),
        consumer_config => maps:get(consumer_config, Config, #{}),
        producer_config => maps:get(producer_config, Config, #{}),
        debug => maps:get(debug, Config, [])
    },
    {ok, Cache}.


%% @doc
plugin_start(SrvId, Config, Service) ->
    insert(SrvId, Config, Service).


plugin_stop(SrvId, _Config, _Service) ->
    nkserver_workers_sup:remove_all_childs(SrvId).


%% @doc
plugin_update(SrvId, NewConfig, OldConfig, Service) ->
    case NewConfig of
        OldConfig ->
            ok;
        _ ->
            insert(SrvId, NewConfig, Service)
    end.



%% ===================================================================
%% Internal
%% ===================================================================


client_syntax_check(Client) ->
    case maps:from_list(Client) of
        #{consumer_group_id:=_, consumer_group_topics:=_} ->
            ok;
        #{consumer_group_id:=_} ->
            {error, {missing_field, <<"consumer_group_topics">>}};
        _ ->
            ok
    end.


%% @private
insert(SrvId, Config, Service) ->
    #{brod_client_id:=BrodId, nodes:=Nodes} = Config,
    Nodes2 = [
        {nklib_util:to_list(Host), Port}
        || #{host:=Host, port:=Port} <- Nodes
    ],
    Childs1 = case Config of
        #{consumer_group_id:=GroupId, consumer_group_topics:=Topics} ->
            ConsumerConfig1 = maps:get(consumer_config, Config, #{}),
            ConsumerConfig2 = maps:to_list(ConsumerConfig1),
            GroupConfig1 = maps:get(consumer_group_config, Config, #{}),
            GroupConfig2 = maps:to_list(GroupConfig1),
            Args1 = [
                BrodId, GroupId, Topics, GroupConfig2, ConsumerConfig2,
                message_set, nkkafka_processor, {SrvId,  #{}}
            ],
            [
                #{
                    id => {group, GroupId, BrodId},
                    start => {brod_group_subscriber, start_link, Args1}
                }
            ];
        _ ->
            []
    end,
    ClientConfig1 = maps:get(client_config, Config, #{}),
    ClientConfig2 = maps:to_list(ClientConfig1),
    Args2 = [Nodes2, BrodId, ClientConfig2],
    Childs2 =  [
        #{
            id => {client, BrodId},
            start => {brod_client, start_link, Args2}
        }
        | Childs1
    ],
    case nkserver_workers_sup:update_child_multi(SrvId, Childs2, #{}) of
        ok ->
            ?SRV_LOG(info, "servers started", [], Service),
            ok;
        upgraded ->
            ?SRV_LOG(info, "servers upgraded", [], Service),
            ok;
        not_updated ->
            ?SRV_LOG(debug, "servers didn't upgrade", [], Service),
            ok;
        {error, Error} ->
            ?SRV_LOG(notice, "servers start/update error: ~p", [Error], Service),
            {error, Error}
    end.


