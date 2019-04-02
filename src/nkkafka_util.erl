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

-module(nkkafka_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([cached_broker/2, cached_leaders/2, cached_partitions/2]).
-export([update_metadata/1, update_metadata/2]).

-include("nkkafka.hrl").


%% ===================================================================
%% Types
%% ===================================================================


%% @doc
-spec cached_broker(nkserver:id(), nkkafka:broker_id()) ->
    {inet:ip_address(), inet:port_number()} | undefined.

cached_broker(SrvId, BrokerId) ->
    nkserver:get(SrvId, {broker, BrokerId}).


%% @doc
-spec cached_leaders(nkserver:id(), nkkafka:topic()) ->
    integer() | undefined.

cached_leaders(SrvId, Topic) ->
    nkserver:get(SrvId, {topic_leaders, to_bin(Topic)}).


%% @doc
-spec cached_partitions(nkserver:id(), nkkafka:topic()) ->
    #{nkkafka:partition() => nkkafka:broker_id()}.

cached_partitions(SrvId, Topic) ->
    nkserver:get(SrvId, {topic_partitions, to_bin(Topic)}).


%% @doc Gets metadata opening a new connection to brokers
update_metadata(SrvId) ->
    Config = nkserver:get_config(SrvId),
    Brokers = nklib_util:randomize(maps:get(brokers, Config, [])),
    update_metadata(SrvId, Brokers, #req_metadata{}).


%% @doc Gets metadata opening a new connection to brokers
update_metadata(SrvId, Topic) ->
    Config = nkserver:get_config(SrvId),
    Brokers = nklib_util:randomize(maps:get(brokers, Config, [])),
    update_metadata(SrvId, Brokers, #req_metadata{topics=[to_bin(Topic)]}).


%% @private
update_metadata(_SrvId, [], _Req) ->
    {error, no_available_brokers};

update_metadata(_SrvId, [{error, Error}|_], _Req) ->
    {error, Error};

update_metadata(SrvId, [#{host:=Host, port:=Port}|Rest], Req) ->
    case nkpacket_dns:ips(Host) of
        [Ip|_] ->
            ConnId = {metadata, nklib_date:epoch(usecs)},
            case nkkafka_protocol:connect(SrvId, 0, Ip, Port, {exclusive, metadata}) of
                {ok, Pid} ->
                    case nkkafka_protocol:send_request(Pid, Req) of
                        {ok, {Brokers, Topics}} ->
                            nkkafka_protocol:stop(Pid),
                            update_brokers(SrvId, maps:to_list(Brokers)),
                            update_topics(SrvId, maps:to_list(Topics)),
                            {ok, {Brokers, Topics}};
                        {error, Error} ->
                            update_metadata(SrvId, Rest++[{error, Error}], Req)
                    end;
                {error, Error} ->
                    update_metadata(SrvId, Rest++[{error, Error}], Req)
            end;
        [] ->
            update_metadata(SrvId, Rest, Req)
    end.



%% @private
update_brokers(_SrvId, []) ->
    ok;

update_brokers(SrvId, [{BrokerId, #{host:=Host, port:=Port}}|Rest]) ->
    case nkpacket_dns:ips(Host) of
        [Ip|_] ->
            nkserver:put(SrvId, {broker, BrokerId}, {Ip, Port});
        [] ->
            lager:warning("Could not resolve broker '~s'", [Host])
    end,
    update_brokers(SrvId, Rest).


%% @private
update_topics(_SrvId, []) ->
    ok;

update_topics(SrvId, [{Topic, #{error:=_Error}}|Rest]) ->
    nkserver:del(SrvId, {topic_leaders, Topic}),
    nkserver:del(SrvId, {topic_partitions, Topic}),
    update_topics(SrvId, Rest);

update_topics(SrvId, [{Topic, #{partitions:=Parts}}|Rest]) ->
    Leaders = [{PartId, Leader} || {PartId, #{leader:=Leader}} <- maps:to_list(Parts)],
    nkserver:put(SrvId, {topic_leaders, Topic}, maps:from_list(Leaders)),
    nkserver:put(SrvId, {topic_partitions, Topic}, map_size(Parts)),
    update_topics(SrvId, Rest).





%% ===================================================================
%% Internal
%% ===================================================================


%% @private
to_bin(R) when is_binary(R) -> R;
to_bin(R) -> nklib_util:to_binary(R).


