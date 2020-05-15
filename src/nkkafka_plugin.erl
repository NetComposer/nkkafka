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
-export([plugin_deps/0, plugin_meta/0, plugin_config/3, plugin_cache/3,
         plugin_start/3, plugin_update/4, plugin_stop/3]).

-include("nkkafka.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").


%% ===================================================================
%% Default implementation
%% ===================================================================


plugin_deps() ->
	[nkserver].

plugin_meta() ->
    #{use_master => true}.


%% @doc
plugin_config(_SrvId, Config, #{class:=nkkafka}) ->
    Syntax = #{
        brokers => {list, #{
            host => host,
            port => {integer, 1, 65535},
            '__defaults' => #{host=><<"127.0.0.1">>, port=>9092}
        }},
        protocol => {atom, [tcp, tls]},
        protocol_opts => nkpacket_syntax:safe_syntax(),
        subscribers => {list, #{
            topic => binary,
            start_at => {atom, [last, first, stored]},  % last means 'next'
            store_offsets => boolean,
            share_connection => boolean,    % Single connection to kafka for all partitions
            '__mandatory' => [topic],
            '__defaults' => #{start_at => last},
            '__unique_keys' => [topic]
        }},
        subscribers_paused => boolean,
        store_offsets_group => binary,
        producer_ack => {atom, [none, leader, all]},
        producer_timeout => integer,
        producer_queue_size => integer,
        debug => {list, {atom, [protocol,producer,subscriber]}},
        '__mandatory' => [brokers],
        '__defaults' => #{
            protocol => tcp,
            producer_ack => leader,
            producer_timeout => 60000,
            producer_queue_size => 100
        }
    },
    nkserver_util:parse_config(Config, Syntax).


plugin_cache(SrvId, Config, _Service) ->
    DefaultGroup = <<"nkkafka_", (nklib_util:to_binary(SrvId))/binary>>,
    Cache = #{
        debug => maps:get(debug, Config, []),
        store_offsets_group => maps:get(store_offsets_group, Config, DefaultGroup),
        producer_ack => maps:get(producer_ack, Config),
        producer_timeout => maps:get(producer_timeout, Config),
        producer_queue_size => maps:get(producer_queue_size, Config)
    },
    {ok, Cache}.


%% @doc
%% Connections and producers are inserted on demand
plugin_start(SrvId, Config, _Service) ->
    Paused = case Config of
        #{subscribers_paused:=true} -> true;
        _ -> false
    end,
    nkkafka:pause_subscribers(SrvId, Paused),
    ok.


plugin_stop(_SrvId, _Config, _Service) ->
    ok.


%% @doc
plugin_update(SrvId, NewConfig, _OldConfig, _Service) ->
    Paused = case NewConfig of
        #{subscribers_paused:=true} -> true;
        _ -> false
    end,
    nkkafka:pause_subscribers(SrvId, Paused),
    ok.


%% ===================================================================
%% Internal
%% ===================================================================


%%%% @private
%%insert_producers(SrvId, Config) ->
%%    Producers = maps:get(producers, Config, #{}),
%%    do_insert_producers(SrvId, Producers).
%%
%%
%%%% @private
%%do_insert_producers(_SrvId, []) ->
%%    ok;
%%
%%do_insert_producers(SrvId, [#{topic:=Topic}=Config|Rest]) ->
%%    Spec = #{
%%        id => {producer, Topic},
%%        start => {nkkafka_producer, start_link, [SrvId, Topic, Config]},
%%        restart => permanent,
%%        shutdown => 5000,
%%        type => worker,
%%        modules => [nkkafa_producer]
%%    },
%%    case nkserver_workers_sup:update_child2(SrvId, Spec, #{}) of
%%        {_, Pid} when is_pid(Pid) ->
%%            do_insert_producers(SrvId, Rest);
%%        {error, Error} ->
%%            {error, Error}
%%    end.