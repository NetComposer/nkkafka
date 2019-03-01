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

-module(nkkafka_partition).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([start/3, get_all/0, find_last_offset/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export_type([state/0, msg/0]).


-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkKAFKA Part (~s:~s:~p) "++Txt,
        [State#state.srv, State#state.topic, State#state.partition | Args])).


-include("nkkafka.hrl").
-include_lib("brod/include/brod.hrl").
-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Types
%% ===================================================================

-type state() ::
    #{
        srv_id => nkservice:id(),
        package_id => nkservice:package_id(),
        group_id => nkkafka:group_id(),
        topic => nkkafka:topic(),
        partition => nkkafka:partition()
    }.


-type msg() ::
    #{
        offset => integer(),
        max_offset => binary(),
        key => binary(),
        value => binary(),
        ts => integer()
    }.


%% ===================================================================
%% Public
%% ===================================================================


%% @doc
-spec start(nkserver:id(), Topic::binary(), Part::integer()) ->
    {ok, pid()}.

start(SrvId, Topic, Part) ->
    gen_server:start(?MODULE, [SrvId, Topic, Part, self()], []).


find_last_offset(Pid) ->
    gen_server:call(Pid, find_last_offset).


%% @private
get_all() ->
    [
        nklib_proc:values(?MODULE)
    ].


% ===================================================================
%% gen_server behaviour
%% ===================================================================

-record(state, {
    srv :: nkserver:id(),
    topic :: binary(),
    partition :: integer(),
    leader_pid :: pid(),
    consumer_pid :: pid(),
    offsets_topic :: binary() | undefined,
    offsets_key :: binary(),
    offsets_pid :: pid() | undefined,
    offset :: integer()
}).


%% @private
init([SrvId, Topic, Part, LeaderPid]) ->
    nklib_proc:put(?MODULE, {SrvId, Topic, Part}),
    monitor(process, LeaderPid),
    case nkkafka:subscribe(SrvId, self(), Topic, Part) of
        {ok, ConsumerPid} ->
            monitor(process, ConsumerPid),
            OffsetsTopic = nkserver:get_cached_config(SrvId, nkkafka, offsets_topic),
            OffsetsKey = <<Topic/binary, $:, (nklib_util:to_binary(Part))/binary>>,
            State1 = #state{
                srv = SrvId,
                topic = Topic,
                partition = Part,
                leader_pid = LeaderPid,
                consumer_pid = ConsumerPid,
                offsets_topic = OffsetsTopic,
                offsets_key = OffsetsKey
            },
            State2 = start_producer(10, State1),
            ?LLOG(info, "started (~p) (consumer:~p) (producer:~p)",
                  [self(), ConsumerPid, State2#state.offsets_pid], State2),
            process_last_messages(State2),
            {ok, State2};
        {error, Error} ->
            lager:error("NkKAFKA Could not start partition for ~s:~s:~p: ~p",
                        [SrvId, Topic, Part, Error]),
            {stop, Error}
    end.


%% @private
handle_call(find_last_offset, _From, State) ->
    {reply, do_find_last_offset(State), State};

handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
handle_cast(Msg, State) ->
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_info({Pid, #kafka_message_set{}=Msg}, #state{consumer_pid=Pid}=State) ->
    #state{topic = Topic, partition = Part} = State,
    #kafka_message_set{
        topic = Topic,
        partition = Part,
        high_wm_offset = _HighOffset,
        messages = Messages
    } = Msg,
    {noreply, process_messages(Messages, State)};

handle_info({Pid, #kafka_fetch_error{}=Error}, #state{consumer_pid=Pid}=State) ->
    #kafka_fetch_error{error_code=Code, error_desc=Desc} = Error,
    ?LLOG(warning, "error retrieving messages (~p, ~p), stopping", [Code, Desc], State),
    {noreply, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{leader_pid=Pid}=State) ->
    ?LLOG(notice, "leader is down (~p), stopping", [Reason], State),
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{consumer_pid=Pid}=State) ->
    ?LLOG(notice, "consumer is down (~p), stopping", [Reason], State),
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{offsets_pid=Pid}=State) ->
    ?LLOG(notice, "producer is down (~p), stopping", [Reason], State),
    {stop, normal, State};

handle_info(Info, State) ->
    lager:warning("Module ~p received unexpected info: ~p (~p)", [?MODULE, Info, State]),
    {noreply, State}.


%% @private
-spec code_change(term(), #state{}, term()) ->
    {ok, #state{}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
-spec terminate(term(), #state{}) ->
    ok.

terminate(_Reason, _State) ->
    ok.



% ===================================================================
%% Internal
%% ===================================================================


%% @private
start_producer(Tries, #state{srv=SrvId, offsets_topic=Topic}=State) when Tries > 0 ->
    case is_binary(Topic) of
        true ->
            case nkkafka:start_producer(SrvId, Topic) of
                ok ->
                    {ok, Pid} = nkkafka:get_producer(SrvId, Topic, 0),
                    monitor(process, Pid),
                    State#state{offsets_pid = Pid};
                {error, unknown_topic_or_partition} ->
                    ?LLOG(notice, "could not start producer for ~s, retrying", [Topic], State),
                    timer:sleep(1000),
                    start_producer(Tries-1, State);
                {error, Error} ->
                    ?LLOG(error, "could not start producer for ~s: ~p", [Topic, Error], State),
                    error(could_not_start_producer)
            end;
        false ->
            ?LLOG(warning, "no offsets defined!", [], State),
            State
    end;

start_producer(_Tries, #state{offsets_topic=Topic}=State) ->
    ?LLOG(error, "could not start producer for ~s: too_many_retries", [Topic], State),
    error(could_not_start_producer).


%% @private
process_last_messages(State) ->
    case do_find_last_offset(State) of
        {ok, Offset} ->
            ?LLOG(info, "last stored offset is ~p", [Offset], State),
            launch_new_messages(Offset, State);
        not_found ->
            ?LLOG(warning, "last stored offset NOT FOUND", [], State)
    end.


%% @private
process_messages([], State) ->
    State;

process_messages([Msg|Rest], State) ->
    % Time is msecs
    #kafka_message{offset=Offset, key=Key, value=Value, ts=TS} = Msg,
    #state{
        srv = SrvId,
        topic = Topic,
        partition = Part,
        offsets_key = OffsetsKey,
        offsets_pid = Pid
    } = State,
    ok = ?CALL_SRV(SrvId, kafka_message, [Topic, Part, Key, Offset, TS, Value]),
    case is_pid(Pid) of
        true ->
            ok = nkkafka_util:produce_sync(Pid, OffsetsKey, nklib_util:to_binary(Offset));
        false ->
            ok
    end,
    process_messages(Rest, State#state{offset=Offset}).


%% @private
do_find_last_offset(#state{srv=SrvId, offsets_topic=Topic}=State) when is_binary(Topic) ->
    case nkkafka_util:resolve_offset(SrvId, Topic, 0) of
        {ok, Last} ->
            do_find_last_offset(Last, State);
        {error, Error} ->
            ?LLOG(error, "could not find last offset: ~p", [Error], State),
            error(last_offset_error)
    end;

do_find_last_offset(_State) ->
    ok.


%% @private
do_find_last_offset(Offset, #state{srv=SrvId, offsets_topic=Topic}=State) ->
    case nkkafka_util:fetch(SrvId, Topic, 0, Offset) of
        {ok, Msgs} ->
            case do_find_last_offset_msgs(Msgs, State) of
                not_found ->
                    do_find_last_offset(Offset-1, State);
                {ok, SavedOffset} ->
                    {ok, SavedOffset}
            end;
        {error, offset_out_of_range} ->
            not_found;
        {error, Error} ->
            lager:error("NKLOG error ~p", [Error]),
            {error, Error}
    end.


%% @private
do_find_last_offset_msgs([], _State) ->
    not_found;

do_find_last_offset_msgs([#kafka_message{key=Key, value=Value}|_], #state{offsets_key=Key}) ->
    {ok, binary_to_integer(Value)};

do_find_last_offset_msgs([_Msg|Rest], State) ->
    do_find_last_offset_msgs(Rest, State).


%% @private
launch_new_messages(Offset, State) ->
    #state{srv=SrvId, topic=Topic, partition=Part, consumer_pid=Pid} = State,
    case nkkafka_util:fetch(SrvId, Topic, Part, Offset) of
        {ok, []} ->
            launch_new_messages(Offset+1, State);
        {ok, Msgs} ->
            ?LLOG(warning, "re-launching messages for offset ~p", [Offset], State),
            Msg = #kafka_message_set{
                topic = Topic,
                partition = Part,
                high_wm_offset = 0,
                messages = Msgs
            },
            self() ! {Pid, Msg},
            launch_new_messages(Offset+1, State);
        {error, offset_out_of_range} ->
            ok;
        {error, Error} ->
            ?LLOG(warning, "error calling fetch for ~p: ~p", [Offset, Error], State),
            ok
    end.


