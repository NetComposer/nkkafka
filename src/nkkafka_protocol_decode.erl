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


-module(nkkafka_protocol_decode).
-export([response/2]).

-include("nkkafka.hrl").

%% ===================================================================
%% Decode
%% ===================================================================

%% @private
response(API, Data) ->
    decode(API, Data).


%% @private
decode(?PRODUCE_REQUEST, Data) ->
    {TopicsCount, R1} = array(Data),
    {Topics, <<>>} = produce_topics(TopicsCount, R1, #{}),
    Topics;

decode(?FETCH_REQUEST, Data) ->
    {TopicsCount, R1} = array(Data),
    {TopicsMap, <<>>} = fetch_topics(TopicsCount, R1, #{}),
    TopicsMap;

decode(?OFFSET_REQUEST, Data) ->
    {TopicsCount, R1} = array(Data),
    {Topics, <<>>} = offset_topics(TopicsCount, R1, #{}),
    Topics;

decode(?METADATA_REQUEST, Data) ->
    {BrokerCount, R1} = array(Data),
    {Brokers, Rest2} = metadata_broker(BrokerCount, R1, #{}),
    {TopicCount, Rest3} = array(Rest2),
    {Topics, <<>>} = metadata_topic(TopicCount, Rest3, #{}),
    {Brokers, Topics};

decode(?OFFSET_COMMIT_REQUEST, Data) ->
    {TopicsCount, R1} = array(Data),
    {Topics, <<>>} = offset_commit_topic(TopicsCount, R1, []),
    #{topics => Topics};

decode(?OFFSET_FETCH_REQUEST, Data) ->
    {TopicsCount, T1} = array(Data),
    {Topics, <<>>} = offset_fetch_topic(TopicsCount, T1, []),
    #{topics=>Topics};

decode(?GROUP_COORDINATOR_REQUEST, <<Error:16/signed-integer, ID:32/signed-integer, Tail/binary>>) ->
    {Host, <<Port:32/signed-integer>>} = string(Tail),
    add_error(Error, #{id=>ID, host=>Host, port=>Port});

decode(?JOIN_GROUP_REQUEST, <<Error:16/signed-integer, Gen:32/signed-integer, Tail/binary>>) ->
    {Protocol, T1} = string(Tail),
    {Leader, T2} = string(T1),
    {MemID, T3} = string(T2),
    {MemCount, T4} = array(T3),
    {Members, <<>>} = join_group_member(MemCount, T4, #{}),
    Rec = #{
        generation => Gen,
        protocol => Protocol,
        leader => Leader,
        id => MemID,
        members_metadata => Members
    },
    add_error(Error, Rec);

decode(?HEARTBEAT_REQUEST, <<Error:16/signed-integer>>) ->
    add_error(Error, #{});

decode(?LEAVE_GROUP_REQUEST, <<Error:16/signed-integer>>) ->
    add_error(Error, #{});

decode(?SYNC_GROUP_REQUEST, <<Error:16/signed-integer, Tail/binary>>) ->
    {Assignment, <<>>} = group_member_assignment(Tail),
    add_error(Error, #{assignment => Assignment});

decode(?DESCRIBE_GROUPS_REQUEST, Data) ->
    {GroupsCount, T1} = array(Data),
    {Groups, <<>>} = describe_groups_group(GroupsCount, T1, #{}),
    Groups;

decode(?LIST_GROUPS_REQUEST, <<Error:16/signed-integer, R1/binary>>) ->
    {GroupsCount, R2} = array(R1),
    {Groups, <<>>} = list_groups_group(GroupsCount, R2, #{}),
    add_error(Error, #{groups=>Groups});

decode(_ApiKey, _Data) ->
    <<>>.




%% ===================================================================
%% Low level
%% ===================================================================


%% @private
metadata_broker(Pos, <<ID:32/signed-integer, R1/binary>>, Acc) when Pos > 0 ->
    {Host, <<Port:32/signed-integer, Rest2/binary>>} = string(R1),
    metadata_broker(Pos-1, Rest2, Acc#{ID=>#{host=>Host, port=>Port}});

metadata_broker(_Pos, Data, Acc) ->
    {Acc, Data}.


%% @private
metadata_topic(Pos, <<Error:16/signed-integer, R1/binary>>, Acc) when Pos > 0->
    {Name, Rest2} = string(R1),
    {PartCount, Rest3} = array(Rest2),
    {Partitions, Rest4} = metadata_partition(PartCount, Rest3, #{}),
    Acc2 = Acc#{Name => add_error(Error, #{partitions => Partitions})},
    metadata_topic(Pos-1, Rest4, Acc2);

metadata_topic(_Pos, Data, Acc) ->
    {Acc, Data}.



%% @private
metadata_partition(Pos, Data, Acc) when Pos > 0 ->
    <<
        Error:16/signed-integer,
        ID:32/signed-integer,
        Leader:32/signed-integer,
        R1/binary
    >> = Data,
    {ReplicaCount, Rest2} = array(R1),
    {Replicas, Rest3} = metadata_replicas(ReplicaCount, Rest2, []),
    {IsrCount, Rest4} = array(Rest3),
    {Isr, Rest5} = metadata_isr(IsrCount, Rest4, []),
    Acc2 = Acc#{ID => add_error(Error, #{leader=>Leader, replicas=>Replicas, isr=>Isr})},
    metadata_partition(Pos-1, Rest5, Acc2);

metadata_partition(_Pos, Data, Acc) ->
    {Acc, Data}.


%% @private
metadata_replicas(Pos, <<ReplicaID:32/signed-integer, R1/binary>>, Acc) when Pos > 0 ->
    metadata_replicas(Pos-1, R1, [ReplicaID|Acc]);

metadata_replicas(_Pos, Data, Acc) ->
    {lists:reverse(Acc), Data}.


%% @private
metadata_isr(Pos, <<IsrID:32/signed-integer, Rest/binary>>, Acc) when Pos > 0 ->
    metadata_isr(Pos-1, Rest, [IsrID|Acc]);

metadata_isr(_Pos, Data, Acc) ->
    {lists:reverse(Acc), Data}.


%% @private
produce_topics(Pos, Data, Acc) when Pos > 0 ->
    {Name, R1} = string(Data),
    {PartCount, R2} = array(R1),
    {Partitions, R3} = produce_res_partition(PartCount, R2, #{}),
    Acc2 = Acc#{Name => Partitions},
    produce_topics(Pos-1, R3, Acc2);

produce_topics(_Pos, Data, Acc) ->
    {Acc, Data}.


%% @private
produce_res_partition(Pos, Data, Acc) when Pos > 0 ->
    <<
        ID:32/signed-integer,
        Error:16/signed-integer,
        Offset:64/signed-integer,
        R/binary
    >> = Data,
    Acc2 = Acc#{ID => add_error(Error, #{offset=>Offset})},
    produce_res_partition(Pos-1, R, Acc2);

produce_res_partition(_Pos, Data, Acc) ->
    {Acc, Data}.



%% @private
fetch_topics(Pos, Data, Acc) when Pos > 0 ->
    {Name, R1} = string(Data),
    {PartCount, Rest2} = array(R1),
    {Partitions, Rest3} = fetch_partition(PartCount, Rest2, #{}),
    fetch_topics(Pos-1, Rest3, Acc#{Name=>Partitions});

fetch_topics(_Pos, Data, Acc) ->
    {Acc, Data}.

%% @private
fetch_partition(Pos, Data, Acc) when Pos > 0 ->
    <<
        ID:32/signed-integer,
        Error:16/signed-integer,
        HwOffset:64/signed-integer,
        Size:32/signed-integer,
        Messages:Size/binary,
        Rest/binary
    >> = Data,
    {Total, Messages2} = fetch_messages(Messages, 0, []),
    Rec = #{
        hw_offset => HwOffset,
        total => Total,
        messages => Messages2
    },
    Value = add_error(Error, Rec),
    fetch_partition(Pos-1, Rest, Acc#{ID=>Value});

fetch_partition(_Pos, Data, Acc) ->
    {Acc, Data}.


fetch_messages(<<>>, Total, Acc) ->
    {Total, lists:reverse(Acc)};

fetch_messages(Bin, Total, Acc) ->
    case Bin of
        <<Offset:64/signed-integer, Size:32/signed-integer, Body:Size/binary, R1/binary>> ->
            <<CRC:32/signed-integer, Rest2/binary>> = Body,
            CRC32 = CRC band 16#FFFFFFFF,
            case erlang:crc32(Rest2) of
                CRC32 ->
                    <<_Magic:8/signed-integer, _Attr:8/signed-integer, Rest3/binary>> = Rest2,
                    lager:error("NKLOG _MAGIC ~p", [_Magic]),
                    lager:error("NKLOG _Attr ~p", [_Attr]),


                    {Key, Rest4} = bytes(Rest3),
                    {Value, <<>>} = bytes(Rest4),
                    Rec = {Offset, Key, Value},
                    fetch_messages(R1, Total+1, [Rec|Acc]);
                V ->
                    lager:error("NkKAFKA invalid message crc32, ~p:~p~n", [V, CRC32]),
                    fetch_messages(R1, Total, Acc)
            end;
        <<_Offset:64/signed-integer, _Size:32/signed-integer, _Rest/binary>> ->
            %lager:warning("discard chunk at ~p, size is ~p but has ~p", [Offset, Size, byte_size(Rest)]),
            fetch_messages(<<>>, Total, Acc);
        _Other ->
            %lager:warning("discard ~p bytes", [byte_size(_Other)]),
            fetch_messages(<<>>, Total, Acc)
    end.


%% @private
offset_topics(Pos, Data, Acc) when Pos > 0 ->
    {Name, R1} = string(Data),
    {PartCount, R2} = array(R1),
    {Partitions, R3} = offset_partitions(PartCount, R2, #{}),
    offset_topics(Pos-1, R3, Acc#{Name => Partitions});

offset_topics(_Pos, Data, Acc) ->
    {Acc, Data}.


%% @private
offset_partitions(Pos, Data, Acc) when Pos > 0 ->
    <<ID:32/signed-integer, Error:16/signed-integer, R1/binary>> = Data,
    {OffsetsCount, R2} = array(R1),
    {Offsets, R3} = offset_partition_offsets(OffsetsCount, R2, []),
    Acc2 = Acc#{ID => add_error(Error, #{offsets=>Offsets})},
    offset_partitions(Pos-1, R3, Acc2);

offset_partitions(_Pos, Data, Acc) ->
    {Acc, Data}.


%% @private
offset_partition_offsets(Pos, <<Offset:64/signed-integer, R/binary>>, Acc) when Pos > 0 ->
    offset_partition_offsets(Pos-1, R, [Offset|Acc]);

offset_partition_offsets(_Pos, Data, Acc) ->
    {lists:reverse(Acc), Data}.


%% @private
offset_commit_topic(Pos, Data, Acc) when Pos > 0 ->
    {Name, R1} = string(Data),
    {PartCount, R2} = array(R1),
    {Partitions, R3} = offset_commit_partition(PartCount, R2, []),
    offset_commit_topic(Pos-1, R3, [#{name=>Name, partitions=>Partitions}|Acc]);

offset_commit_topic(_Pos, Data, Acc) ->
    {Acc, Data}.


%% @private
offset_commit_partition(Pos, Data, Acc) when Pos > 0 ->
    <<ID:32/signed-integer, Error:16/signed-integer, R/binary>> = Data,
    offset_commit_partition(Pos-1, R, [add_error(Error, #{id=>ID})|Acc]);

offset_commit_partition(_Pos, Data, Acc) ->
    {Acc, Data}.


%% @private
offset_fetch_topic(Pos, Data, Acc) when Pos > 0 ->
    {Name, T1} = string(Data),
    {PartCount, T2} = array(T1),
    {Partitions, T3} = offset_fetch_partition(PartCount, T2, []),
    offset_fetch_topic(Pos-1, T3, [#{name=>Name, partitions=>Partitions}|Acc]);

offset_fetch_topic(_Pos, Data, Acc) ->
    {Acc, Data}.



%% @private
offset_fetch_partition(Pos, Data, Acc) when Pos > 0 ->
    <<ID:32/signed-integer, Offset:64/signed-integer, R1/binary>> = Data,
    {Metadata, <<Error:16/signed-integer, R2/binary>>} = string(R1),
    Rec = add_error(Error, #{id=>ID, offset=>Offset, metadata=>Metadata}),
    offset_fetch_partition(Pos-1, R2, [Rec|Acc]);

offset_fetch_partition(_Pos, Data, Acc) ->
    {Acc, Data}.



%% @private
join_group_member(Pos, Data, Acc) when Pos > 0 ->
    {MemID, T1} = string(Data),
    {Metadata, T2} = bytes(T1),
    join_group_member(Pos-1, T2, Acc#{MemID => Metadata});

join_group_member(_Pos, Data, Acc) ->
    {Acc, Data}.


%% @private
group_member_assignment(<<Ver:16/signed-integer, Tail/binary>>) ->
    {PartCount, T1} = array(Tail),
    {Partitions, T2} = topic_partitions(PartCount, T1, []),
    {UserData, T3} = bytes(T2),
    Rec = #{
        version => Ver,
        partitions => Partitions,
        user_data => UserData
    },
    {Rec, T3}.


%% @private
list_groups_group(Pos, Data, Acc) when Pos > 0 ->
    {ID, R1} = string(Data),
    {ProtoType, R2} = string(R1),
    list_groups_group(Pos-1, R2, Acc#{ID=>ProtoType});

list_groups_group(_Pos, Data, Acc) ->
    {Acc, Data}.



%% @private
describe_groups_group(Pos, <<Error:16/signed-integer, Tail/binary>>, Acc) when Pos > 0 ->
    {ID, T1} = string(Tail),
    {State, T2} = string(T1),
    {ProtoType, T3} = string(T2),
    {Protocol, T4} = string(T3),
    {MemCount, T5} = array(T4),
    {Members, T6} = describe_groups_group_member(MemCount, T5, #{}),
    Rec = #{
        state => State,
        proto_type => ProtoType,
        protocol => Protocol,
        members => Members
    },
    describe_groups_group(Pos-1, T6, Acc#{ID=>add_error(Error, Rec)});

describe_groups_group(_Pos, Data, Acc) ->
    {Acc, Data}.



%% @private
describe_groups_group_member(Pos, Data, Acc) when Pos > 0 ->
    {ID, T1} = string(Data),
    {Client, T2} = string(T1),
    {Host, T3} = string(T2),
    {Metadata, T4} = bytes(T3),
    {Assignment, T5} = bytes(T4),
    Rec = #{
        client => Client,
        host => Host,
        metadata => Metadata,
        assignment => Assignment
    },
    describe_groups_group_member(Pos-1, T5, Acc#{ID=>Rec});

describe_groups_group_member(_Pos, Data, Acc) ->
    {Acc, Data}.



%% @private
array(<<0,0>>) ->
    {0, []};
array(<<Len:32/signed-integer, Data/binary>>) ->
    {Len, Data}.


%% @private
string(<<Len:16/signed-integer, Data/binary>>) ->
    case Len of
        -1 ->
            {undefined, Data};
        _ ->
            <<Str:Len/binary, Tail/binary>> = Data,
            {Str, Tail}
    end.


%% @private
bytes(<<Len:32/signed-integer, Data/binary>>) ->
    case Len of
        -1 ->
            {undefined, Data};
        _ ->
            <<Bytes:Len/binary, Tail/binary>> = Data,
            {Bytes, Tail}
    end.


%% @private
topic_partitions(Pos, <<ID:32/signed-integer, R1/binary>>, Acc) when Pos > 0 ->
    topic_partitions(Pos-1, R1, [ID|Acc]);

topic_partitions(_Pos, Data, Acc) ->
    {Acc, Data}.



%% @private
add_error(0, Data) -> Data;
add_error(Error, Data) -> Data#{error=>kafka_error(Error)}.


%% @private
kafka_error(0) -> no_error;
kafka_error(-1) -> unknown;
kafka_error(1) -> offset_out_of_range;
kafka_error(2) -> invalid_message;
kafka_error(3) -> unknown_topic_or_partition;
kafka_error(4) -> invalid_message_size;
kafka_error(5) -> leader_not_available;
kafka_error(6) -> not_leader_for_partition;
kafka_error(7) -> request_timed_out;
kafka_error(8) -> broker_not_available;
kafka_error(9) -> replica_not_available;
kafka_error(10) -> message_size_too_large;
kafka_error(11) -> stale_controller_epoch;
kafka_error(12) -> offset_metadata_too_large;
kafka_error(13) -> network_exception;
kafka_error(14) -> group_load_in_progress;
kafka_error(15) -> group_coordinator_not_available;
kafka_error(16) -> not_coordinator_for_group;
kafka_error(17) -> invalid_topic;
kafka_error(18) -> record_list_too_large;
kafka_error(19) -> not_enough_replicas;
kafka_error(20) -> not_enough_replicas_after_append;
kafka_error(21) -> invalid_required_acks;
kafka_error(22) -> illegal_generation;
kafka_error(23) -> inconsistent_group_protocol;
kafka_error(24) -> invalid_group_id;
kafka_error(25) -> unknown_member_id;
kafka_error(26) -> invalid_session_timeout;
kafka_error(27) -> rebalance_in_progress;
kafka_error(28) -> invalid_commit_offset_size;
kafka_error(29) -> topic_authorization_failed;
kafka_error(30) -> group_authorization_failed;
kafka_error(31) -> cluster_authorization_failed;
kafka_error(32) -> invalid_timestamp;
kafka_error(33) -> unsupported_sasl_mechanism;
kafka_error(34) -> illegal_sasl_state;
kafka_error(35) -> unsupported_version;
kafka_error(36) -> topic_already_exists;
kafka_error(37) -> invalid_partitions;
kafka_error(38) -> invalid_replication_factor;
kafka_error(39) -> invalid_replica_assignment;
kafka_error(40) -> invalid_config;
kafka_error(41) -> not_controller;
kafka_error(42) -> invalid_request;
kafka_error(43) -> unsupported_for_message_format;
kafka_error(44) -> policy_violation;
kafka_error(45) -> out_of_order_sequence_number;
kafka_error(46) -> duplicate_sequence_number;
kafka_error(47) -> invalid_producer_epoch;
kafka_error(48) -> invalid_txn_state;
kafka_error(49) -> invalid_producer_id_mapping;
kafka_error(50) -> invalid_transaction_timeout;
kafka_error(51) -> concurrent_transactions;
kafka_error(52) -> transaction_coordinator_fenced;
kafka_error(53) -> transactional_id_authorization_failed;
kafka_error(54) -> security_disabled;
kafka_error(55) -> operation_not_attempted;
kafka_error(56) -> kafka_storage_error;
kafka_error(57) -> log_dir_not_found;
kafka_error(58) -> sasl_authentication_failed;
kafka_error(59) -> unknown_producer_id;
kafka_error(60) -> reassignment_in_progress;
kafka_error(61) -> delegation_token_auth_disabled;
kafka_error(62) -> delegation_token_not_found;
kafka_error(63) -> delegation_token_owner_mismatch;
kafka_error(64) -> delegation_token_request_not_allowed;
kafka_error(65) -> delegation_token_authorization_failed;
kafka_error(66) -> delegation_token_expired;
kafka_error(67) -> invalid_principal_type;
kafka_error(68) -> non_empty_group;
kafka_error(69) -> group_id_not_found;
kafka_error(70) -> fetch_session_id_not_found;
kafka_error(71) -> invalid_fetch_session_epoch;
kafka_error(72) -> listener_not_found;
kafka_error(73) -> topic_deletion_disabled;
kafka_error(74) -> fenced_leader_epoch;
kafka_error(75) -> unknown_leader_epoch;
kafka_error(76) -> unsupported_compression_type;
kafka_error(_) -> unknown.
