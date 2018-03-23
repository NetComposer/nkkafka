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

-module(nkkafka_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([kafka_processor_init/1, kafka_processor_msg/2]).



%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% Offered callbacks
%% ===================================================================

%% @doc Called when the processor for an specific set of service, package, group,
%% topic and partition starts
-spec kafka_processor_init(nkkafka_processor:state()) ->
    {ok, nkkafka_processor:state()}.

kafka_processor_init(State) ->
    {ok, State}.


%% @doc Called when a new message is received in a processor
%% This function is blocking the whole consumer group, so spawn it if
%% it can be slow!
%% However, The moment we replay the offset will be acknowledged
%% (and not received again)

-spec kafka_processor_msg(nkkafka_processor:msg(), nkkafka_processor:state()) ->
    {ok, nkkafka_processor:state()}.

kafka_processor_msg(Msg, State) ->
    lager:notice("Ignoring Kafka message: ~p ~p", [Msg, State]),
    {ok, State}.




