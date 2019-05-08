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
-module(nkkafka_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkserver/include/nkserver_callback.hrl").
-include("nkkafka.hrl").

%% ===================================================================
%% Public
%% ===================================================================



t() ->
    Total = 100000,
    Now = nklib_date:epoch(usecs),
    t(Total),
    Time = nklib_date:epoch(usecs) - Now,
    Total / Time * 1000000.


t(N) when N > 0 ->
    Now = nklib_date:epoch(secs),
    nklib_date:to_3339(Now, secs),
    %nklib_date:now_3339(secs),
    t(N-1);
t(_) ->
    ok.




g() ->
    SrvId = test_kafka_srv,
    Ip = {192,168,0,111},
    Port = 9092,
    Group = group2,


    {ok, K1} = nkkafka_protocol:connect(SrvId, Ip, Port, undefined),
    {ok, #{
        generation := Gen1,
        id := Id1,
        leader := Leader1,
        members_metadata := #{},
            %Id1 := <<"my_metadata">>
        protocol := <<"my_protocol">>
    }} = nkkafka:join_group(K1, Group, #{}),
    lager:error("NKLOG 1 ~p", [{Gen1, Id1, Leader1}]),

    {ok, K2} = nkkafka_protocol:connect(SrvId, Ip, Port, undefined),
    {ok, #{
        generation := Gen2,
        id := Id2,
        leader := Leader2,
        members_metadata := #{},
        protocol := <<"my_protocol">>
    }} = nkkafka:join_group(K2, Group, #{}),
    lager:error("NKLOG 2 ~p", [{Gen2, Id2, Leader2}]),
    lager:error("NKLOG DESCRIBE ~p", [nkkafka:describe_group(SrvId, Group)]),
    nkkafka:heartbeat_group(SrvId, Group, Gen1, Id1).



send(N) when N > 0 ->
    nkkafka_producer:produce(?MODULE, <<>>, test_sdr, msg1),
    send(N-1);

send(_) ->
    ok.


metadata() ->
    Req = #req_metadata{},
    [Pid] = nkkafka_protocol:get_local_started(nkkafka_sample),
    nkkafka_broker:send_request(Pid, Req).


s() ->
    nkkafka_protocol:connect(?MODULE, "tcp://localhost:9092", #{}).


s2() ->
    {ok,#{topics:=#{<<"test_sdr">>:=#{0:=#{messages:=Msgs}}}}} = nkkafka:fetch(nkkafka_sample, test_sdr, 0, 0),
    [#{offset:=First}|_] = Msgs,
    [#{offset:=Last}|_] = lists:reverse(Msgs),
    {length(Msgs), First,Last}.

%% @doc Starts the service
start() ->
    Spec = #{
        brokers => [#{host => <<"192.168.0.7">>}],
        debug => processor,
        producers => [
            #{topic => test_sdr}
        ]
    },
    nkserver:start_link(<<"Kafka">>, ?MODULE, Spec).


get_all(Offset, Count) ->
    lager:error("NKLOG ASK ~p ~p", [Offset, Count]),
    {ok, Data} = nkkafka:fetch(nkkafka_sample, 0, <<"test_outbound">>, 0, Offset),
    #{topics:=#{<<"test_outbound">>:=#{0:=#{messages:=Msgs}}}} = Data,
    case Msgs of
        [] ->
            {ok, Count};
        _ ->
            Num = length(Msgs),
            lager:error("FOUND ~p", [Num]),
            [#{offset:=Last}|_] = lists:reverse(Msgs),
            get_all(Last+1, Count+Num)
    end.





%%%% @doc Starts the service
%%start() ->
%%    Spec = #{
%%        nodes => [#{host => <<"localhost">>}],
%%%%        consumerGroupId => my_group,
%%%%        consumerGroupTopics => "topic6",
%%        client_config => #{
%%            restart_delay_seconds => 11
%%        },
%%        consumerConfig => #{
%%            prefetch_count => 0
%%        },
%%        consumer_group_config => #{
%%            session_timeout_seconds => 12
%%        },
%%        process_topics => topic1,
%%        processed_offsets_topic => "__nkkafa_offsets",
%%        debug => processor
%%    },
%%    nkserver:start_link(<<"Kafka">>, ?MODULE, Spec).


%% @doc Stops the service
stop() ->
    nkserver:stop(?MODULE).



get_partitions_count(Topic) ->
    nkkafka:get_partitions_count(?MODULE, Topic).


produce(Topic, Key, Val) ->
    nkkafka:produce_sync(?MODULE, Topic, 0, to_bin(Key), to_bin(Val)).

produce(Topic, Part, Key, Val) ->
    nkkafka:produce_sync(?MODULE, Topic, Part, to_bin(Key), to_bin(Val)).

produce2(Topic, Key, Val) ->
    nkkafka:produce_sync(?MODULE, Topic, 0, <<>>, [{nklib_util:m_timestamp(), to_bin(Key), to_bin(Val)}]).


subscribe(Topic) ->
    nkkafka:subscribe(?MODULE, self(), Topic, 0).



get_metadata() ->
    nkkafka_util:update_metadata(?MODULE).


resolve_offset(Topic, Time) ->
    nkkafka_util:resolve_offset(?MODULE, Topic, 0, Time).


fetch(Topic, Offset) ->
    nkkafka_util:fetch(?MODULE, Topic, 0, Offset).





%%produce(Key, Val) ->
%%    nkservice_luerl_instance:call({?MODULE, s1, main}, [produce], [topic6, Key, Val]).
%%
%%
%%s1() -> <<"
%%    messageCB = function(msg, info)
%%        log.info('LUERL Incoming Message: ' .. json.encodePretty(msg) .. ' info: ' .. json.encodePretty(info))
%%    end
%%
%%    kafkaConfig = {
%%        nodes = { {host = 'localhost'} },
%%        consumerGroupId = 'my_group_2',
%%        consumerGroupTopics = 'topic6',
%%        clientConfig = { restart_delay_seconds = 13 },
%%        consumerConfig = { prefetch_count = 5 },
%%        consumerGroupConfig = { session_timeout_seconds = 15 },
%%        debug = 'processor',
%%
%%        consumerGroupCallback = messageCB
%%    }
%%
%%    kafka = startPackage('Kafka', kafkaConfig)
%%
%%    function produce(topic, key, val)
%%        return kafka.produce(topic, key, val)
%%    end
%%
%%">>.



%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).
