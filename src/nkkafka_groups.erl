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

-module(nkkafka_groups).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([get_group_coordinator/2, list_groups/1, describe_group/2, join_group/3]).
-export([heartbeat_group/4, leave_group/3, offset_fetch/4, offset_commit/6]).

-include("nkkafka.hrl").

-define(LLOG(Type, Txt, Args),lager:Type("NkKAFKA "++Txt, Args)).

%% ===================================================================
%% Types
%% ===================================================================

-type group() :: binary().

-type member() :: binary().

-type protocol() :: binary().

-type join_opts() ::
    #{
        heartbeat_timeout => integer()
    }.




%% ===================================================================
%% Public
%% ===================================================================



%% @doc MUST BE SEND TO ALL!
-spec list_groups(nkserver:id()) ->
    #{group() => protocol()} | {error, term()}.

list_groups(SrvId) ->
    case nkkafka_brokers:send_request(SrvId, #req_list_groups{}) of
        {ok, #{error:=Error}} ->
            {error, {kafka_error, Error}};
        {ok, #{groups:=Groups}} ->
            {ok, Groups};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec describe_group(nkserver:id(), group()) ->
    #{group() => protocol()} | {error, term()}.

describe_group(SrvId, Group) ->
    Group2 = to_bin(Group),
    Req = #req_describe_groups{groups = [Group2]},
    case nkkafka_brokers:send_request(SrvId, Req) of
        {ok, #{Group2:=#{error:=Error}}} ->
            {error, {kafka_error, Error}};
        {ok, #{Group2:=GroupData}} ->
            {ok, GroupData};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec get_group_coordinator(nkserver:id(), group()) ->
    {ok, #{id=>nkkafka:broker_id(), host=>binary(), port=>integer()}} | {error, term()}.

get_group_coordinator(SrvId, Group) ->
    Req = #req_group_coordinator{group=to_bin(Group)},
    case nkkafka_brokers:send_request(SrvId, Req) of
        {ok, #{error:=Error}} ->
            {error, {kafka_error, Error}};
        {ok, Data} ->
            {ok, Data};
        {error, Error} ->
            {error, Error}
    end.


-spec join_group(nkserver:id(), group(), join_opts()) ->
    any().

join_group(SrvId, Group, Opts) ->
    Req = #req_join_group{
        group = to_bin(Group),
        timeout = maps:get(heartbeat_timeout, Opts, 180000),
        member = <<>>,
        proto_type = <<"consumer">>,
        protocols  = [
            #req_join_group_protocol{
                name = to_bin(maps:get(protocol, Opts, <<"my_protocol">>)),
                metadata = to_bin(maps:get(protocol, Opts, <<"my_metadata">>))
            }
        ]
    },
    nkkafka_brokers:send_request(SrvId, Req).


-spec leave_group(nkserver:id(), group(), member()) -> ok.

leave_group(SrvId, Group, Member) ->
    Req = #leave_group_request{
        group = to_bin(Group),
        member = to_bin(Member)
    },
    case nkkafka_brokers:send_request(SrvId, Req) of
        {ok, #{error:=Error}} ->
            {error, {kafka_error, Error}};
        {ok, _} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.


heartbeat_group(SrvId, Group, Generation, Member) ->
    Req = #req_heartbeat{
        group = to_bin(Group),
        generation = Generation,
        member = to_bin(Member)
    },
    case nkkafka_broker:send_request(SrvId, Req) of
        {ok, #{error:=Error}} ->
            {error, {kafka_error, Error}};
        {ok, _} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

offset_fetch(SrvId, Group, Topic, Partition) ->
    Topic2 = to_bin(Topic),
    Req = #req_offset_fetch{
        group = to_bin(Group),
        topics = [
            #req_offset_fetch_topic{
                name = Topic2,
                partitions = [Partition]
            }
        ]
    },
    case nkkafka_brokers:send_request(SrvId, Req) of
        {ok, #{topics:=[#{partitions:=[#{metadata:=Meta, offset:=Offset}]}]}} ->
            {ok, Meta, Offset};
        Other ->
            {error, Other}
    end.


offset_commit(SrvId, Group, Topic, Partition, Offset, Meta) ->
    Req = #req_offset_commit{
        group = to_bin(Group),
        topics = [
            #req_offset_commit_topic{
                name = to_bin(Topic),
                partitions = [
                    #req_offset_commit_partition{
                        partition = Partition,
                        offset = Offset,
                        metadata = to_bin(Meta)
                    }
                ]
            }
        ]
    },
    case nkkafka_brokers:send_request(SrvId, Req) of
        {ok, #{topics:=[#{partitions:=[#{id:=Partition}]}]}} ->
            ok;
        Other ->
            {error, Other}
    end.





%% ===================================================================
%% Internal
%% ===================================================================


%% @private
to_bin(R) when is_binary(R) -> R;
to_bin(R) -> nklib_util:to_binary(R).
