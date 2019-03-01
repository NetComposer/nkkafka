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

%% @doc
-module(nkkafka_master).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([get_topics/1]).
-export([init/2, handle_call/4, handle_info/3, timed_check/3, start_topics/3]).

-include("nkkafka.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(LLOG(Type, Txt, Args), lager:Type("NkKAFKA Master "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================


-type state() ::
    #{
        {topic, Topic::binary(), Partition::integer()} => pid(),
        {pid, pid()} => {topic, Topic::binary(), Partition::integer()}
    }.


%% ===================================================================
%% Public
%% ===================================================================

get_topics(SrvId) ->
    nkserver_master:call_leader(SrvId, nkkafka_get_topics).


%% ===================================================================
%% Master callbacks
%% ===================================================================

%% @doc
init(_SrvId, _State) ->
    continue.


%% @doc
handle_call(nkkafka_get_topics, _From, _SrvId, State) ->
    Topics = [
        {Topic, Partition, Pid}
        || {{topic, Topic, Partition}, Pid} <- maps:to_list(State)
    ],
    {reply, {ok, Topics}, State};

handle_call(_Msg, _From, _SrvId, _State) ->
    continue.


%% @doc
handle_info({'DOWN', _Ref, process, Pid, Reason}, _SrvId, State) ->
    case maps:take({pid, Pid}, State) of
        {{topic, Topic, Part}, State2} ->
            ?LLOG(warning, "partition ~s:~p has failed! (~p)", [Topic, Part, Reason]),
            State3 = maps:remove({topic, Topic, Part}, State2),
            {noreply, State3};
        error ->
            continue
    end;

handle_info(_Msg, _SrvId, _State) ->
    continue.


%% @doc
timed_check(true, SrvId, State) ->
    Topics = get_topics(SrvId, 10),
    State2 = start_topics(Topics, SrvId, State),
    {continue, [true, SrvId, State2]};

timed_check(false, _SrvId, _State) ->
    continue.


%% ===================================================================
%% Internal
%% ===================================================================


get_topics(SrvId, Tries) when Tries > 0 ->
    Topics = nkserver:get_cached_config(SrvId, nkkafka, process_topics),
    case Topics of
        [Topic|_] ->
            case nkkafka:get_partitions_count(SrvId, Topic) of
                {ok, _} ->
                    Topics;
                {error, client_down} ->
                    ?LLOG(notice, "waiting for kafka...", []),
                    timer:sleep(1000),
                    get_topics(SrvId, Tries-1)
            end;
        [] ->
            []
    end;

get_topics(_SrvId, _Tries) ->
    error(could_not_connect_to_kafka).



%% @private
-spec start_topics([binary()], nkserver:id(), state()) ->
    state().

start_topics([], _SrvId, State) ->
    State;

start_topics([Topic|Rest], SrvId, State) ->
    State2 = case nkkafka:get_partitions_count(SrvId, Topic) of
        {ok, Num} ->
            start_topic_partitions(Num-1, Topic, SrvId, State);
        {error, Error} ->
            ?LLOG(error, "error reading topic ~s: ~p", [Topic, Error]),
            State
    end,
    start_topics(Rest, SrvId, State2).


%% @private
start_topic_partitions(Part, Topic, SrvId, State) when Part >= 0 ->
    State2 = case maps:is_key({topic, Topic, Part}, State) of
        true ->
            State;
        false ->
            {ok, Pid} = nkkafka_partition:start(SrvId, Topic, Part),
            monitor(process, Pid),
            State#{
                {topic, Topic, Part} => Pid,
                {pid, Pid} => {topic, Topic, Part}
            }
    end,
    start_topic_partitions(Part-1, Topic, SrvId, State2);

start_topic_partitions(_Part, _Topic, _SrvId, TopicData) ->
    TopicData.


