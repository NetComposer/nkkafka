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

%% Started from main supervisor to serialize connections to brokers started protocol servers
-module(nkkafka_broker).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([send_request/2, send_async_request/2, stop/1]).
-export([start_link/0, get_connection/1]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).


%% ===================================================================
%% Types
%% ===================================================================

-type conn_id() ::
    pid() |
    nkserver:id() |
    {nkserver:id(), nkkafka:broker_id()} |
    {nkserver:id(), nkkafka:broker_id(), term()}.


%% ===================================================================
%% Public
%% ===================================================================


%% @doc
-spec send_request(conn_id(), tuple()) ->
    {ok, term()} | {error, term()}.

send_request(ConnId, Request) when is_tuple(Request) ->
    do_call(ConnId, Request).


%% @doc
-spec send_async_request(conn_id(), tuple()) ->
    ok | {error, term()}.

send_async_request(ConnId, Request) when is_tuple(Request) ->
    do_cast(ConnId, Request).


%% @doc
-spec stop(conn_id()) ->
    ok | {error, term()}.

stop(ConnId) ->
    do_cast(ConnId, nkkafka_stop).


%% @private
do_call(Pid, Msg) when is_pid(Pid) ->
    nkkafka_protocol:send_request(Pid, Msg);

do_call(Id, Msg) ->
    do_call(Id, Msg, 5).


%% @private
do_call(Id, Msg, Tries) when Tries > 0 ->
    case get_connection(Id) of
        {ok, Pid} when is_pid(Pid) ->
            case do_call(Pid, Msg) of
                {error, process_not_found} when Tries > 1 ->
                    lager:notice("NkKAFKA Protocol failed, retrying"),
                    timer:sleep(100),
                    do_call(Id, Msg, Tries-1);
                Other ->
                    Other
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_cast(Id, Msg) ->
    case get_connection(Id) of
        {ok, Pid} ->
            gen_server:cast(Pid, Msg);
        {error, Error} ->
            {error, Error}
    end.


%% @private
get_connection(Id) ->
    case nkkafka_protocol:find_connection(Id) of
        {ok, Pid} ->
            {ok, Pid};
        undefined ->
            gen_server:call(?MODULE, {get_connection, Id}, infinity)
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
handle_call({get_connection, Id}, _From, State) ->
    case nkkafka_protocol:find_connection(Id) of
        {ok, Pid} ->
            {reply, {ok, Pid}, State};
        _ ->
            Reply = nkkafka_protocol:connect(Id),
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


