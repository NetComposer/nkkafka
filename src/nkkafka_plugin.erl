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

%% @doc NkKAFKA callbacks

-module(nkkafka_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([plugin_deps/0, plugin_api/1, plugin_config/3, plugin_start/4, plugin_update/5]).
-export([client_syntax_check/1]).
-export([syntax/0]).

-include("nkkafka.hrl").

-define(LLOG(Type, Txt, Args),lager:Type("NkKAFKA "++Txt, Args)).


%% ===================================================================
%% Plugin callbacks
%%
%% These are used when NkKAFKA is started as a NkSERVICE plugin
%% ===================================================================

%% @private
plugin_deps() ->
    [].


%% @doc
plugin_api(?PKG_KAFKA) ->
    #{
        luerl => #{
            produce => {nkkafka, luerl_produce}
        }
    };

plugin_api(_Class) ->
    #{}.


%% @private
plugin_config(?PKG_KAFKA, #{id:=Id, config:=Config}=Spec, #{id:=SrvId}) ->
	Syntax = syntax(),
    case nklib_syntax:parse(Config, Syntax) of
        {ok, Parsed, _} ->
            BrodClientId1 = nklib_util:bjoin([SrvId, "nkkafka", Id], $_),
            BrodClientId2 = binary_to_atom(BrodClientId1, utf8),
            CacheMap = #{
                {nkkafka, Id, brod_client} => BrodClientId2,
                {nkkafka, Id, consumer_config} => maps:get(consumerConfig, Parsed, #{}),
                {nkkafka, Id, producer_config} => maps:get(producerConfig, Parsed, #{})
            },
            DebugMap = lists:foldl(
                fun(DebugItem, Acc) -> Acc#{{nkkafka, Id, DebugItem} => true} end,
                #{},
                maps:get(debug, Parsed, [])),
            Spec2 = Spec#{
                config := Parsed#{brod_client_id=>BrodClientId2},
                cache_map => CacheMap,
                debug_map => DebugMap
            },
            {ok, Spec2};
        {error, Error} ->
            {error, Error}
    end;

plugin_config(_Class, _Package, _Service) ->
    continue.



%% @doc
plugin_start(?PKG_KAFKA, #{id:=Id, config:=Config}, Pid, Service) ->
    insert(Id, Config, Pid, Service);

plugin_start(_Id, _Spec, _Pid, _Service) ->
    continue.


%% @doc
%% Even if we are called only with modified config, we check if the spec is new
plugin_update(?PKG_KAFKA, #{id:=Id, config:=NewConfig}, OldSpec, Pid, Service) ->
    case OldSpec of
        #{config:=NewConfig} ->
            ok;
        _ ->
            insert(Id, NewConfig, Pid, Service)
    end;

plugin_update(_Class, _NewSpec, _OldSpec, _Pid, _Service) ->
    ok.




%% ===================================================================
%% Util
%% ===================================================================


%% @private
syntax() ->
    #{
        nodes => {list, #{
            host => host,
            port => {integer, 1, 65535},
            '__defaults' => #{host=><<"127.0.0.1">>, port=>9092}
        }},
        %% @see nkkafka:client_config()
        clientConfig => #{
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
        producerConfig => #{
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
        consumerConfig => #{
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
        consumerGroupId => binary,                % It will start if defined
        consumerGroupTopics => {list, binary},
        consumerGroupConfig => #{
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
    }.


%% @private
insert(Id, Config, SupPid, #{id:=SrvId}) ->
    #{brod_client_id:=BrodId, nodes:=Nodes1} = Config,
    Nodes2 = [{nklib_util:to_list(Host), Port} || #{host:=Host, port:=Port} <- Nodes1],
    Childs1 = case Config of
        #{consumerGroupId:=GroupId, consumerGroupTopics:=Topics} ->
            ConsumerConfig = maps:to_list(maps:get(consumerConfig, Config, #{})),
            GroupConfig = maps:to_list(maps:get(consumerGroupConfig, Config, #{})),
            Args = [
                BrodId, GroupId, Topics, GroupConfig, ConsumerConfig,
                message_set, nkkafka_processor, {SrvId, Id, #{}}
            ],
            [
                #{
                    id => {group, GroupId, BrodId},
                    start => {brod_group_subscriber, start_link, Args}
                }
            ];
        _ ->
            []
    end,
    ClientConfig = maps:to_list(maps:get(clientConfig, Config, #{})),
    Childs2 =  [
        #{
            id => {client, BrodId},
            start => {brod_client, start_link, [Nodes2, BrodId, ClientConfig]}
        }
        | Childs1
    ],
    case nkservice_packages_sup:update_child_multi(SupPid, Childs2, #{}) of
        ok ->
            ?LLOG(debug, "started ~s", [Id]),
            ok;
        not_updated ->
            ?LLOG(debug, "didn't upgrade ~s", [Id]),
            ok;
        upgraded ->
            ?LLOG(info, "upgraded ~s", [Id]),
            ok;
        {error, Error} ->
            ?LLOG(notice, "start/update error ~s: ~p", [Id, Error]),
            {error, Error}
    end.


client_syntax_check(Client) ->
    case maps:from_list(Client) of
        #{consumerGroupId:=_, consumerGroupTopics:=_} ->
            ok;
        #{consumerGroupId:=_} ->
            {error, {missing_field, <<"consumerGroupTopics">>}};
        _ ->
            ok
    end.
