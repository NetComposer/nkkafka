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

%% @doc 
-module(nkkafka_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-compile(export_all).

-include_lib("nkservice/include/nkservice.hrl").
-define(SRV, kafka_sample).

%% ===================================================================
%% Public
%% ===================================================================


%% @doc Starts the service
start() ->
    Spec = #{
        callback => ?MODULE,
        nkkafka => [
            #{
                host => <<"localhost">>
            }
        ]
        %debug => [{nkkafka, [full]}]
    },
    nkservice:start(?SRV, Spec).


%% @doc Stops the service
stop() ->
    nkservice:stop(?SRV).


plugin_deps() ->
    [nkkafka].


start_producer(Topic) ->
    nkkafka:start_producer(?SRV, main, Topic).


get_partitions_count(Topic) ->
    nkkafka:get_partitions_count(?SRV, main, Topic).


get_producer(Topic) ->
    nkkafka:get_producer(?SRV, main, Topic, 0).


produce(Topic, Key, Val) ->
    nkkafka:produce_sync(?SRV, main, Topic, 0, to_bin(Key), to_bin(Val)).

produce2(Topic, Key, Val) ->
    nkkafka:produce_sync(?SRV, main, Topic, 0, <<>>, [{nklib_util:m_timestamp(), to_bin(Key), to_bin(Val)}]).


start_consumer(Topic) ->
    nkkafka:start_consumer(?SRV, main, Topic).


get_consumer(Topic) ->
    nkkafka:get_consumer(?SRV, main, Topic, 0).


subscribe(Topic) ->
    nkkafka:subscribe(?SRV, main, whereis(?SRV), Topic, 0, #{}).








get_metadata() ->
    nkkafka_util:get_metadata(?SRV, main).


resolve_offset(Topic, Time) ->
    nkkafka_util:resolve_offset(?SRV, main, Topic, 0, Time).


fetch(Topic, Offset) ->
    nkkafka_util:fetch(?SRV, main, Topic, 0, Offset).








%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).
