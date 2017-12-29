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

-export([kafka_message/3]).
-export([service_init/2, service_handle_info/2]).

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

%% @doc Called when the service starts a subscriber group and a message is received in any
%% topic or partition of the group
%% Must spawn or it will block the processor
%% Must call nkkafka_util:send_callback_ack/1 to acknowledge
-spec kafka_message(nkkafka:group_id(), nkkafka:topic(), nkkafka:msg()) ->
    ok.

kafka_message(_GroupId, _Topic, Data) ->
    spawn_link(
        fun() ->
            lager:notice("Ignoring Kafka message: ~p", [Data])
            % nkkafka_util:send_callback_ack(Data)
        end),
    ok.



%% ===================================================================
%% Implemented callbacks
%% ===================================================================


service_init(Service, #{id:=SrvId}=State) ->
    case Service of
        #{config:=#{nkkafka_clients:=Clusters}} ->
            lists:foreach(
                fun(Client) -> nkkafka_plugin:start_client(SrvId, Client) end,
                maps:values(Clusters));
        _ ->
            ok
    end,
    {ok, State}.


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


