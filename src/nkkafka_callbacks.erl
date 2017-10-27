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

-module(nkkafka_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([kafka_message_set/4]).
-export([service_handle_info/2]).

-include("nkkafka.hrl").
-include_lib("nkservice/include/nkservice.hrl").
-include_lib("brod/include/brod.hrl").



%% ===================================================================
%% Types
%% ===================================================================

% -type continue() :: continue | {continue, list()}.




%% ===================================================================
%% Offered callbacks
%% ===================================================================

kafka_message_set(Topic, Partition, Messages, Opts) ->
    lager:notice("Kafka Message Set (~s, ~p, ~p): ~p", [Topic, Partition, Opts, Messages]),
    ok.


service_handle_info({_SubPid, #kafka_message_set{}=Set}, #{id:=Id}=State) ->
    #kafka_message_set{
        topic = Topic,
        partition = Partition,
        high_wm_offset = HighOffset,
        messages = List
    } = Set,
    Messages = nkkafka_util:process_messages(List),
    Id:kafka_message_set(Topic, Partition, Messages, #{high_offset=>HighOffset}),
    {noreply, State};

service_handle_info(_Msg, _State) ->
    continue.




