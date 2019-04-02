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
-export([get_subscribers/1, get_info/1]).
-export([init/2, handle_call/4, handle_info/3, timed_check/3]).

-include("nkkafka.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(LLOG(Type, Txt, Args), lager:Type("NkKAFKA Master "++Txt, Args)).
-define(BROKER_CHECK_PERIOD, 60*1000).

%% ===================================================================
%% Types
%% ===================================================================


-type state() ::
    #{
        brokers => map(),
        topics => map(),
        subscribers => #{{Topic::binary(), Partition::integer()} => pid()},
        pids => #{pid() => {subscriber, Topic::binary(), Partition::integer()}},
        last_broker_check => nklib_date:epoch(msecs)
    }.


%% ===================================================================
%% Public
%% ===================================================================

get_info(SrvId) ->
    nkserver_master:call_leader(SrvId, nkkafka_get_info).


get_subscribers(SrvId) ->
    case get_info(SrvId) of
        {ok, #{subscribers:=Subscribers}} ->
            Info = lists:map(
                fun({Name, _Topic, _Part, Pid}) ->
                    case nkkafka_subscriber:get_info(Pid) of
                        {ok, PartInfo} ->
                            {Name, PartInfo};
                        {error, _Error} ->
                            {Name, error}
                    end
                end,
                Subscribers),
            {ok, Info};
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% Master callbacks
%% ===================================================================

%% @doc
init(SrvId, State) ->
    State2 = State#{
        brokers => #{},
        topics => #{},
        subscribers => #{},
        pids => #{},
        last_broker_check => 0
    },
    {continue, [SrvId, State2]}.


%% @doc
handle_call(nkkafka_get_info, _From, SrvId, #{subscribers:=Subscribers}=State) ->
    Subscribers2 = [
        {nkkafka_subscriber:make_name(SrvId, Topic, Part), Topic, Part, Pid}
        || {{Topic, Part}, Pid} <- maps:to_list(Subscribers)
    ],
    #{brokers:=Brokers, topics:=Topics} = State,
    Reply = #{brokers=>Brokers, topics=>Topics, subscribers=>Subscribers2},
    {reply, {ok, Reply}, State};

handle_call(_Msg, _From, _SrvId, _State) ->
    continue.


%% @doc
handle_info({'DOWN', _Ref, process, Pid, Reason}, _SrvId, #{pids:=Pids}=State) ->
    case maps:take(Pid, Pids) of
        {{subscriber, Topic, Part}, Pids2} ->
            ?LLOG(warning, "partition ~s:~p has failed! (~p)", [Topic, Part, Reason]),
            #{subscribers:=Subscribers} = State,
            Subscribers2 = maps:remove({Topic, Part}, Subscribers),
            {noreply, State#{subscribers:=Subscribers2, pids:=Pids2}};
        error ->
            continue
    end;

handle_info(_Msg, _SrvId, _State) ->
    continue.


%% @doc
timed_check(true, SrvId, #{last_broker_check:=Last}=State) ->
    case nklib_date:epoch(msecs) > Last + ?BROKER_CHECK_PERIOD of
        true ->
            State2 = start_subscribers(SrvId, State),
            {continue, [true, SrvId, State2]};
        false ->
            continue
    end;

timed_check(false, _SrvId, _State) ->
    continue.


%% ===================================================================
%% Internal
%% ===================================================================


%% @private
-spec start_subscribers(nkserver:id(), state()) ->
    state().

start_subscribers(SrvId, State) ->
    State2 = get_topics(SrvId, State, 10),
    Config = nkserver:get_config(SrvId),
    Subscribers = maps:get(subscribers, Config, []),
    start_subscribers(Subscribers, SrvId, State2).


%% @private
get_topics(SrvId, State, Tries) when Tries > 0 ->
    case nkkafka_util:update_metadata(SrvId) of
        {ok, {Brokers, Topics}} ->
            Now = nklib_date:epoch(msecs),
            State#{brokers=>Brokers, topics=>Topics, last_broker_check:=Now};
        {error, Error} ->
            ?LLOG(notice, "waiting for kafka... (~p, ~p tries left)", [Error, Tries]),
            timer:sleep(1000),
            get_topics(SrvId, State, Tries-1)
    end;

get_topics(_SrvId, _State, _Tries) ->
    error(could_not_connect_to_kafka).


%% @private
start_subscribers([], _SrvId, State) ->
    State;

start_subscribers([#{topic:=Topic}=Config|Rest], SrvId, State) ->
    State2 = case nkkafka:get_partitions(SrvId, Topic) of
        {ok, NumParts} ->
            start_topic_partitions(NumParts-1, Topic, Config, SrvId, State);
        {error, Error} ->
            ?LLOG(error, "topic '~s' is not available: ~p", [Topic, Error]),
            State
    end,
    start_subscribers(Rest, SrvId, State2).


%% @private
start_topic_partitions(Part, Topic, Config, SrvId, State) when Part >= 0 ->
    #{subscribers:=Subscribers, pids:=Pids} = State,
    case maps:is_key({Topic, Part}, Subscribers) of
        true ->
            start_topic_partitions(Part-1, Topic, Config, SrvId, State);
        false ->
            SrvPid = nkserver_srv:get_random_instance(SrvId),
            Cmd = {nkkafka_start_subscriber, Topic, Part, Config, self()},
            case gen_server:call(SrvPid, Cmd) of
                {ok, SubPid} ->
                    monitor(process, SubPid),
                    Subscribers2 = Subscribers#{{Topic, Part} => SubPid},
                    Pids2 = Pids#{SubPid => {subscriber, Topic, Part}},
                    State2 = State#{subscribers:=Subscribers2, pids:=Pids2},
                    start_topic_partitions(Part-1, Topic, Config, SrvId, State2);
                {error, Error} ->
                    ?LLOG(error, "could not start subscriber for ~s (~p) at ~p: ~p",
                          [Topic, Part, node(SrvPid), Error]),
                    State
            end
    end;

start_topic_partitions(_Part, _Topic, _Config, _SrvId, State) ->
    State.

