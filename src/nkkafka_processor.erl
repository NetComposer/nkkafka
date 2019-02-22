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

-module(nkkafka_processor).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([init/2, handle_message/4]).
-export([start_link/1, find_pid/1, get_all/0]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).
-export_type([state/0, msg/0]).

-define(LLOG(Type, Txt, Args), lager:Type("NkKAFKA Processor "++Txt, Args)).

-define(LLOG(Type, Txt, Args, Id),
    lager:Type("NkKAFKA Processor (~s:~s:~p) (~s:~s) "++Txt,
        [Id#processor_id.group_id, Id#processor_id.topic, Id#processor_id.partition,
            Id#processor_id.srv_id, Id#processor_id.package_id | Args])).

-define(DEBUG(Txt, Args, State),
    case State#state.debug of
        true ->
            ?LLOG(debug, Txt, Args, State#state.id);
        false ->
            ok
    end).



-include("nkkafka.hrl").
-include_lib("brod/include/brod.hrl").
-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Types
%% ===================================================================

-record(processor_id, {
    srv_id :: nkservice:id(),
    package_id :: nkservice:package_id(),
    group_id :: nkkafka:group_id(),
    topic :: nkkafka:topic(),
    partition :: nkkafka:partition()
}).


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
-spec init(nkkafka:group_id(), term()) ->
    {ok, term()}.

init(GroupId, {SrvId, PackageId, Config}) ->
    {ok, {SrvId, PackageId, GroupId, self(), Config}}.


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
%% If we block this function, no more messages will be received for
%% any partition or topic.
%%
%% Calling brod_group_subscriber:ack/4 has nothing to do with receiving more
%% messages, it only marks the offset as 'processed' so that it will not be
%% received again if we restart
%%
%% When we restart, by default we receive:
%% - if we acknowledged any offset, the next one
%% - if we didn't acknowledge anything, nothing

-spec handle_message(nkkafka:topic(), nkkafka:partition(), nkkafka:message(),term()) ->
    {ok, term()} | {ok, ack, term()}.

handle_message(Topic, Partition, MessageSet, State) ->
    {SrvId, PackageId, GroupId, Pid, _Config} = State,
    Id = #processor_id{
        srv_id = SrvId,
        package_id = PackageId,
        group_id = GroupId,
        topic = Topic,
        partition = Partition
    },
    #kafka_message_set{
        high_wm_offset = Max,      %% max offset of the partition
        messages = Messages
    } = MessageSet,
    lists:foreach(
        fun(Message) ->
            #kafka_message{
                offset = Offset,
                key = Key,
                value = Value,
                ts = TS
            } = Message,
            Message2 = #{
                offset => Offset,
                max_offset => Max,
                key => Key,
                value => Value,
                ts => TS
            },
            process(Id, Pid, Message2)
        end,
        Messages),
    {ok, State}.




%% ===================================================================
%% Internal
%% ===================================================================


start(ProcessorId) ->
    Child = #{
        id => ProcessorId,
        start => {?MODULE, start_link, [ProcessorId]}
    },
    #processor_id{srv_id=SrvId, package_id=PackageId} = ProcessorId,
    nkservice_packages_sup:start_child(SrvId, PackageId, Child).


%% @doc
-spec start_link(#processor_id{}) ->
    {ok, pid()} | {error, term()}.

start_link(ProcessorId) ->
    gen_server:start_link(?MODULE, ProcessorId, []).


%% @doc
process(ProcessorId, Pid, Msg) ->
    process(ProcessorId, Pid, Msg, 5).


%% @private
%% Starts a new processor for each topic/partition and tries to
%% insert the message. We are blocking the whole consumer group
%% until the processor answers!
process(ProcessorId, Pid, #{offset:=Offset}=Msg, Tries) when Tries > 0 ->
    case find_pid(ProcessorId) of
        ProcPid when is_pid(ProcPid) ->
            case nklib_util:call(ProcPid, {process, Pid, Msg}, 30000) of
                ok ->
                    ok;
                {error, Error} ->
                    ?LLOG(notice, "error processing ~p: ~p, retrying (~p)",
                          [Offset, Error, Tries], ProcessorId),
                    timer:sleep(1000),
                    process(ProcessorId, Pid, Msg, Tries-1)
            end;
        undefined ->
            case start(ProcessorId) of
                {ok, _} ->
                    process(ProcessorId, Pid, Msg, Tries-1);
                {error, Error} ->
                    ?LLOG(notice, "error starting ~p: ~p, retrying (~p)",
                          [Offset, Error, Tries], ProcessorId),
                    timer:sleep(1000),
                    process(ProcessorId, Pid, Msg, Tries-1)
            end
    end;

process(ProcessorId, Pid, #{offset:=Offset}, _Tries) ->
    ?LLOG(notice, "too many process retries for ~p", [Offset], ProcessorId),
    send_ack(ProcessorId, Pid, Offset).


%% @private
find_pid(ProcessorId) ->
    case nklib_proc:values({?MODULE, ProcessorId}) of
        [{_, Pid}] -> Pid;
        [] -> undefined
    end.


%% @private
get_all() ->
    [
        nklib_proc:values(?MODULE)
    ].


% ===================================================================
%% gen_server behaviour
%% ===================================================================

-record(state, {
    id :: #processor_id{},
    offset :: integer(),
    luerl_pid :: pid() | undefined,
    luerl_spec :: map() | undefined,
    user :: map(),
    debug :: boolean()
}).


%% @private
-spec init(term()) ->
    {ok, tuple()} | {ok, tuple(), timeout()|hibernate} |
    {stop, term()} | ignore.

init(ProcessorId) ->
    true = nklib_proc:reg({?MODULE, ProcessorId}),
    nklib_proc:put(?MODULE, ProcessorId),
    ?LLOG(notice, "started (~p)", [self()], ProcessorId),
    #processor_id{
        srv_id = SrvId,
        package_id = PackageId,
        group_id = GroupId,
        topic = Topic,
        partition = Partition
    } = ProcessorId,
    User = #{
        srv_id => SrvId,
        package_id => PackageId,
        group_id => GroupId,
        topic => Topic,
        partition => Partition
    },
    Debug = nkservice_util:get_debug(SrvId, {nkkafka, PackageId, processor}),
    {ok, User2} = ?CALL_SRV(SrvId, kafka_processor_init, [User]),
    State1 = #state{
        id = ProcessorId,
        offset = -1,
        user = User2,
        debug = Debug == true
    },
    %State2 = start_luerl(State1),
    {ok, State1}.


%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}} |
    {stop, Reason::term(), #state{}} | {stop, Reason::term(), Reply::term(), #state{}}.

handle_call({process, Pid, Msg}, From, State) ->
    #{offset:=Offset} = Msg,
    #state{
        id = Id,
        offset = Current,
        luerl_pid = LuerlPid,
        luerl_spec = LuerlSpec,
        user = User
    } = State,
    % Allow next message
    gen_server:reply(From, ok),
    ?DEBUG("processing (~p, kakfka:~p) ~p", [self(), Pid, Msg], State),
    State2 = case Offset > Current of
        true ->
            User2 = try
                #processor_id{srv_id=SrvId} = Id,
                case ?CALL_SRV(SrvId, kafka_processor_msg, [Msg, User]) of
                    continue when LuerlPid == undefined ->
                        lager:notice("Ignoring Kafka message: ~p ~p", [Msg, State]),
                        User;
                    continue ->
                        ?DEBUG("calling luerl callback", [], State),
                        case nkservice_luerl_instance:call_callback(LuerlPid, LuerlSpec, [Msg, User]) of
                            {ok, _} ->
                                ok;
                            {error, Error} ->
                                ?LLOG(warning, "error calling kafka_processor luerl (~p): ~p",
                                    [Offset, Error], Id)
                        end,
                        User;
                    {ok, NewUser} ->
                        NewUser;
                    Other ->
                        ?LLOG(warning, "error calling kafka_processor_msg (~p): ~p",
                              [Offset, Other], Id),
                        User
                end
            catch
                error:CError:Trace ->
                    ?LLOG(warning, "error calling kafka_processor_msg (~p): ~p (~p)",
                        [Offset, CError, Trace], Id),
                    User
            end,
            State#state{offset=Offset, user=User2};
        false ->
            % When a node is added or removed and partitions reassigned,
            % non-acknowledged offsets will be sent again
            ?LLOG(warning, "repeating offset ~p (current:~p), skipping",
                  [Offset, Current], Id),
            State
    end,
    send_ack(Id, Pid, Offset),
    {noreply, State2};

handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_cast(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_cast(Msg, State) ->
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_info({'DOWN', _Ref, process, Pid, Reason}, #state{id=Id, luerl_pid=Pid}=State) ->
    ?LLOG(notice, "luerl instance down: ~p", [Reason], Id),
    %State2 = start_luerl(State),
    {noreply, State};

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

send_ack(Id, Pid, Offset) ->
    #processor_id{topic=Topic, partition=Partition} = Id,
    %lager:error("NKLOG nkkafka:group_subscriber_ack(~p, ~p, ~p, ~p). ",
    %            [Pid, Topic, Partition, Offset]),
    nkkafka:group_subscriber_ack(Pid, Topic, Partition, Offset),
    ok.


%%start_luerl(#state{id=#processor_id{srv_id=SrvId, package_id=PackageId}}=State) ->
%%    case nkservice_util:get_callback(SrvId, ?PKG_KAFKA, PackageId, consumerGroupCallback) of
%%        #{
%%            class := luerl,
%%            module_id := ModuleId
%%        } = Spec ->
%%            {ok, Pid} =
%%                nkservice_luerl_instance:start(SrvId, ModuleId, make_ref(), #{monitor=>self()}),
%%            ?DEBUG("started luerl instance: ~p", [Pid], State),
%%            monitor(process, Pid),
%%            State#state{luerl_pid=Pid, luerl_spec=Spec};
%%        _ ->
%%            State
%%    end.



%%%% @private
%%to_bin(R) when is_binary(R) -> R;
%%to_bin(R) -> nklib_util:to_binary(R).
