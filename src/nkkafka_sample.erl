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
                    client_config => #{
                        restart_delay_seconds => 10
                    },
                    producer_config => #{
                        required_acks => -1
                    }
                }
            },
            #{
                id => k2,
                class => 'Kafka',
                config => #{
                    nodes => [#{host => <<"localhost">>}],
                    consumer_group_id => my_group,
                    consumer_group_topics => "topic6",
                    client_config => #{
                        restart_delay_seconds => 11
                    },
                    consumer_config => #{
                        prefetch_count => 0
                    },
                    group_config => #{
                        session_timeout_seconds => 10
                    },
                    debug => processor
                }
            }
        ]
%%        modules => [
%%            #{
%%                id => s1,
%%                class => luerl,
%%                code => s1(),
%%                debug => true
%%            }
%%        ]
    },
    nkservice:start(?SRV, Spec).


%% @doc Stops the service
stop() ->
    nkservice:stop(?SRV).


getRequest() ->
    nkservice_luerl_instance:call({?SRV, s1, main}, [getRequest], []).


%%s1() -> <<"
%%    esConfig = {
%%        targets = {
%%            {
%%                url = 'http://127.0.0.1:9200',
%%                weight = 100,
%%                pool = 5
%%            }
%%        },
%%        resolveInterval = 0,
%%        debug = {'full', 'pooler'}
%%    }
%%
%%    es = startPackage('Elastic', esConfig)
%%
%%    function getRequest()
%%        return es.request('get', '/')
%%    end
%%
%%">>.
%%
%%
%%opts() ->
%%    nkservice_util:get_cache(?SRV, {nkelastic, <<"es1">>, opts}).
%%


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
