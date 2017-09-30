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
-export([plugin_deps/0, plugin_syntax/0, plugin_config/2, plugin_start/2]).
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



%% ===================================================================
%% Plugin callbacks
%%
%% These are used when NkKAFKA is started as a NkSERVICE plugin
%% ===================================================================


plugin_deps() ->
    [].


plugin_syntax() ->
	#{
	    nkkafka =>
            {list, #{
                id => binary,
                host => host,
                port => {integer, 1, 65535},
                '__mandatory' => [host],
                '__defaults' => #{port => 9092}
           }}
}.


plugin_config(#{nkkafka:=List}=Config, #{id:=SrvId}) ->
    case parse_clients(SrvId, List, #{}) of
        {ok, ParsedMap} ->
            Cache = lists:foldl(
                fun({Id, #{client_id:=ClientId}}, Acc) ->
                    Acc#{Id => ClientId}
                end,
                #{},
                maps:to_list(ParsedMap)),
            {ok, Config#{nkkafka_clients=>ParsedMap}, Cache};
        {error, Error} ->
            {error, Error}
    end;

plugin_config(Config, _Service) ->
    {ok, Config}.


plugin_start(#{nkkafka_clients:=Clients}=Config, #{id:=SrvId}) ->
    lists:foreach(
        fun(#{host:=Host, port:=Port, client_id:=ClientId}) ->
            start_client(SrvId, ClientId, Host, Port, [])
        end,
        maps:values(Clients)
    ),
    {ok, Config};

plugin_start(Config, _Service) ->
    {ok, Config}.


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






%% ===================================================================
%% Util
%% ===================================================================

parse_clients(_SrvId, [], Acc) ->
    {ok, Acc};

parse_clients(SrvId, [Map|Rest], Acc) ->
    Id = maps:get(id, Map, <<"main">>),
    case maps:is_key(Id, Acc) of
        false ->
            ClientId1 = nklib_util:bjoin([SrvId, "nkkafka", Id], <<"_">>),
            ClientId2 = binary_to_atom(ClientId1, utf8),
            Data1 = maps:without([id], Map),
            Data2 = Data1#{client_id=>ClientId2},
            parse_clients(SrvId, Rest, Acc#{Id=>Data2});
        true ->
            {error, duplicated_id}
    end.


start_client(SrvId, Id, Host, Port, Config) ->
    lager:notice("Service '~s' starting Kafka client '~s' (~s:~p)", [SrvId, Id, Host, Port]),
    EndPoints = [{nklib_util:to_list(Host), Port}],
    {ok, _} = nkservice_srv:start_proc(SrvId, Id, brod_client, [EndPoints, Id, Config]).

