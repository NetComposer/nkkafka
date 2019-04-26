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

-module(nkkafka_producers).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([produce/4, produce_async/4, get_info/2]).
-export([start_link/0, get_producer/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).


%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% Public
%% ===================================================================

%% @doc Starts producer and insert message
%% If key is <<>>, a random partition will be selected
%% For any other, same partition will be use for same key
produce(SrvId, Topic, Key, Msg) ->
    do_call(SrvId, to_bin(Topic), {insert, to_bin(Key), to_bin(Msg)}).


%% @doc Starts producer and insert message
produce_async(SrvId, Topic, Key, Msg) ->
    do_cast(SrvId, to_bin(Topic), {insert, to_bin(Key), to_bin(Msg)}).


%% @doc
get_info(SrvId, Topic) ->
    do_call(SrvId, to_bin(Topic), get_info).


%% @private
do_call(SrvId, Topic,  Msg) ->
    do_call(SrvId, Topic, Msg, 5).


%% @private
do_call(SrvId, Topic, Msg, Tries) when Tries > 0 ->
    case get_producer(SrvId, Topic) of
        {ok, Pid} ->
            case nkkafka_producer_srv:sync(Pid, Msg) of
                {error, process_not_found} when Tries > 1 ->
                    lager:notice("NkKAFKA producer failed, retrying"),
                    timer:sleep(100),
                    do_call(SrvId, Topic, Msg, Tries-1);
                Other ->
                    Other
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_cast(SrvId, Topic, Msg) ->
    case get_producer(SrvId, Topic) of
        {ok, Pid} ->
            nkkafka_producer_srv:async(Pid, Msg);
        {error, Error} ->
            {error, Error}
    end.


%% @private
get_producer(SrvId, Topic) ->
    case nkkafka_producer_srv:find(SrvId, Topic) of
        {ok, Pid} ->
            {ok, Pid};
        undefined ->
            gen_server:call(?MODULE, {get_producer, SrvId, Topic}, infinity)
    end.


%% @private
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).




% ===================================================================
%% gen_server behaviour
%% ===================================================================

-record(state, {
}).


%% @private
init([]) ->
    {ok, #state{}}.


%% @private
handle_call({get_producer, SrvId, Topic}, _From, State) ->
    case nkkafka_producer_srv:find(SrvId, Topic) of
        {ok, Pid} ->
            {reply, {ok, Pid}, State};
        _ ->
            Reply = nkkafka_producer_srv:start(SrvId, Topic),
            {reply, Reply, State}
    end;

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



%% @private
to_bin(K) when is_binary(K) -> K;
to_bin(K) -> nklib_util:to_binary(K).
