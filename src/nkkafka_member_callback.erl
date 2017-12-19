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

%% @doc NkKAFKA callback module for subscriber groups

-module(nkkafka_member_callback).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([init/2, handle_message/4]).

-include_lib("brod/include/brod.hrl").
-include_lib("nkservice/include/nkservice.hrl").


%% ===================================================================
%% Plugin callbacks
%%
%% These are used when NkKAFKA is started as a NkSERVICE plugin
%% ===================================================================


%% @doc
-spec init(nkkafka:group_id(), term()) ->
    {ok, term()}.

init(GroupId, {SrvId, Config}) ->
    {ok, {SrvId, GroupId, Config}}.


%% Handle a message. Return one of:
%%
%% {ok, NewCallbackState}:
%%   The subscriber has received the message for processing async-ly.
%%   It should call brod_group_subscriber:ack/4 to acknowledge later.
%%
%% {ok, ack, NewCallbackState}
%%   The subscriber has completed processing the message.
%%
%% While this callback function is being evaluated, the fetch-ahead
%% partition-consumers are fetching more messages behind the scene
%% unless prefetch_count is set to 0 in consumer config.
%%
-spec handle_message(nkkafka:topic(), nkkafka:partition(), nkkafka:message(),term()) ->
    {ok, term()} | {ok, ack, term()}.

handle_message(Topic, Partition, MessageSet, {SrvId, GroupId, _Config}=State) ->
    #kafka_message_set{
        high_wm_offset = _Max,      %% max offset of the partition
        messages = Messages
    } = MessageSet,
    Data = #{
        srv_id => SrvId,
        group_id => GroupId,
        topic => Topic,
        partition => Partition,
        pid => self()
    },
    [process_message(SrvId, GroupId, Topic, Data, Message) || Message <- Messages],
    {ok, State}.


%% @private
process_message(SrvId, GroupId, Topic, Data, Message) ->
    #kafka_message{
        offset = Offset,
        key = Key,
        value = Value,
        ts = TS
    } = Message,
    Data2 = Data#{
        offset => Offset,
        key => Key,
        value => Value,
        ts => TS
    },
    ?CALL_SRV(SrvId, kafka_message, [GroupId, Topic, Data2]),
    nkkafka_util:send_callback_ack(Data2).



