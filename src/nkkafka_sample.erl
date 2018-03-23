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

%% @doc 
-module(nkkafka_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("nkservice/include/nkservice.hrl").
-define(SRV, kafka_test).

%% ===================================================================
%% Public
%% ===================================================================



%% @doc Starts the service
start() ->
    Spec = #{
        plugins => [?MODULE],
        packages => [
            #{
                id => k1,
                class => 'Kafka',
                config => #{
                    nodes => [#{host => <<"localhost">>}],
                    clientConfig => #{
                        restart_delay_seconds => 10
                    },
                    producerConfig => #{
                        required_acks => -1
                    }
                }
            },
            #{
                id => k2,
                class => 'Kafka',
                config => #{
                    nodes => [#{host => <<"localhost">>}],
                    consumerGroupId => my_group,
                    consumerGroupTopics => "topic6",
                    clientConfig => #{
                        restart_delay_seconds => 11
                    },
                    consumerConfig => #{
                        prefetch_count => 0
                    },
                    consumerGroupConfig => #{
                        session_timeout_seconds => 12
                    },
                    debug => processor
                }
            }
        ],
        modules => [
            #{
                id => s1,
                class => luerl,
                code => s1(),
                debug => true
            }
        ]
    },
    nkservice:start(?SRV, Spec).


%% @doc Stops the service
stop() ->
    nkservice:stop(?SRV).


produce(Key, Val) ->
    nkservice_luerl_instance:call({?SRV, s1, main}, [produce], [topic6, Key, Val]).


s1() -> <<"
    messageCB = function(msg, info)
        log.info('LUERL Incoming Message: ' .. json.encodePretty(msg) .. ' info: ' .. json.encodePretty(info))
    end

    kafkaConfig = {
        nodes = { {host = 'localhost'} },
        consumerGroupId = 'my_group_2',
        consumerGroupTopics = 'topic6',
        clientConfig = { restart_delay_seconds = 13 },
        consumerConfig = { prefetch_count = 5 },
        consumerGroupConfig = { session_timeout_seconds = 15 },
        debug = 'processor',

        consumerGroupCallback = messageCB
    }

    kafka = startPackage('Kafka', kafkaConfig)

    function produce(topic, key, val)
        return kafka.produce(topic, key, val)
    end

">>.


opts() ->
    nkservice_util:get_cache(?SRV, {nkelastic, <<"es1">>, opts}).



get_partitions_count(Topic) ->
    nkkafka:get_partitions_count(?SRV, k1, Topic).


produce(Topic, Key, Val) ->
    nkkafka:produce_sync(?SRV, k1, Topic, 0, to_bin(Key), to_bin(Val)).

produce(Topic, Part, Key, Val) ->
    nkkafka:produce_sync(?SRV, k1, Topic, Part, to_bin(Key), to_bin(Val)).

produce2(Topic, Key, Val) ->
    nkkafka:produce_sync(?SRV, k1, Topic, 0, <<>>, [{nklib_util:m_timestamp(), to_bin(Key), to_bin(Val)}]).


subscribe(Topic) ->
    nkkafka:subscribe(?SRV, k1, self(), Topic, 0).



get_metadata() ->
    nkkafka_util:get_metadata(?SRV, k2).


resolve_offset(Topic, Time) ->
    nkkafka_util:resolve_offset(?SRV, k2, Topic, 0, Time).


fetch(Topic, Offset) ->
    nkkafka_util:fetch(?SRV, k2, Topic, 0, Offset).








%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).
