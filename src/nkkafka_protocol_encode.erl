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

-module(nkkafka_protocol_encode).
-export([request/3]).

-include_lib("nkkafka.hrl").


%% ===================================================================
%% Encode
%% ===================================================================


%% @private
request(TraceID, Client, Request) ->
    {API, Ver, ReqBin} = encode(Request),
    ClientBin = string(Client),
    Bin = <<
        API:16/signed-integer,
        Ver:16/signed-integer,
        TraceID:32/signed-integer,
        ClientBin/binary,
        ReqBin/binary
    >>,
    {API, Bin}.



%% Note: If "auto.create.topics.enable" is set in the broker configuration,
%%  a topic metadata request will create the topic with the default replication factor and number of partitions.
%% @private
encode(#req_metadata{topics=Topics}) ->
    BinTopics = [string(Topic) || Topic <- Topics],
    {?METADATA_REQUEST, 0, array(BinTopics)};

encode(#req_produce{ack =Acks, timeout=Timeout, topics=Topics}) ->
    BinTopics = [produce_topic(Topic) || Topic <- Topics],
    Array = array(BinTopics),
    Acks2 = case Acks of
        none -> 0;
        leader -> 1;
        all -> -1
    end,
    {?PRODUCE_REQUEST, 0, <<Acks2:16/signed-integer, Timeout:32/signed-integer, Array/binary>>};

encode(#req_fetch{max_wait=MaxWait, min_bytes=MinBytes, topics=Topics}) ->
    Replica = -1,
    TopicsBinL = [fetch_topic(Topic) || Topic <- Topics],
    TopicsBin = array(TopicsBinL),
    Req = <<
        Replica:32/signed-integer,
        MaxWait:32/signed-integer,
        MinBytes:32/signed-integer,
        TopicsBin/binary
    >>,
    {?FETCH_REQUEST, 0, Req};

encode(#req_offset{topics=Topics}) ->
    TopicsBinL = [offset_topic(Topic) || Topic <- Topics],
    Replica = -1,
    TopicsBin = array(TopicsBinL),
    {?OFFSET_REQUEST, 0, <<Replica:32/signed-integer, TopicsBin/binary>>};

encode(#req_group_coordinator{group=GroupID}) ->
    {?GROUP_COORDINATOR_REQUEST, 0, string(GroupID)};

encode(#req_offset_commit{group =GroupID, topics=Topics}) ->
    GroupBin = string(GroupID),
    TopicsBinL = [offset_commit_topic(Topic) || Topic <- Topics],
    TopicsBin = array(TopicsBinL),
    {?OFFSET_COMMIT_REQUEST, 0, <<GroupBin/binary, TopicsBin/binary>>};

encode(#req_offset_fetch{group =GroupID, topics=Topics}) ->
    GroupBin = string(GroupID),
    TopicsBinL = [offset_fetch_topic(Topic) || Topic <- Topics],
    TopicsBin = array(TopicsBinL),
    {?OFFSET_FETCH_REQUEST, 0, <<GroupBin/binary, TopicsBin/binary>>};

encode(#req_join_group{group=GroupId, timeout=Timeout, member=Member, proto_type=Type, protocols=Protocols}) ->
    GroupBin = string(GroupId),
    MemberBin = string(Member),
    TypeBin = string(Type),
    ProtocolsList = [join_group_protocol(Protocol) || Protocol <- Protocols],
    ProtocolsBin = array(ProtocolsList),
    Bin = <<
        GroupBin/binary,
        Timeout:32/signed-integer,
        MemberBin/binary,
        TypeBin/binary,
        ProtocolsBin/binary
    >>,
    {?JOIN_GROUP_REQUEST, 0, Bin};

encode(#req_sync_group{group =ID, generation=Gen, member =Member, assignment=Assigns}) ->
    IDBin = string(ID),
    MemberBin = string(Member),
    AssignsBinL = [sync_group_assignment(Assignment) || Assignment <- Assigns],
    AssignsBin = array(AssignsBinL),
    {?SYNC_GROUP_REQUEST, 0, <<IDBin/binary, Gen:32/signed-integer, MemberBin/binary, AssignsBin/binary>>};

encode(#req_heartbeat{group=ID, generation=Gen, member=Member}) ->
    IDBin = string(ID),
    MemBin = string(Member),
    {?HEARTBEAT_REQUEST, 0, <<IDBin/binary, Gen:32/signed-integer, MemBin/binary>>};

encode(#leave_group_request{group =ID, member =Member}) ->
    IDBin = string(ID),
    MemBin = string(Member),
    {?LEAVE_GROUP_REQUEST, 0, <<IDBin/binary, MemBin/binary>>};

encode(#req_list_groups{}) ->
    {?LIST_GROUPS_REQUEST, 0, <<>>};

encode(#req_describe_groups{groups=Groups}) ->
    GroupsBinL =[string(Group) || Group <- Groups],
    {?DESCRIBE_GROUPS_REQUEST, 0, array(GroupsBinL)}.


%% ===================================================================
%% Low level
%% ===================================================================


%% @private
string(null) ->
    <<-1:16/signed-integer>>;
string(Bin) ->
    <<(byte_size(Bin)):16/signed-integer, Bin/binary>>.


%% @private
bytes(null) ->
    <<-1:32/signed-integer>>;
bytes(Bin) ->
    <<(byte_size(Bin)):32/signed-integer, Bin/binary>>.


%% @private
%% @private
array(null) ->
    <<-1:32/signed-integer>>;
array([]) ->
    <<0:32/signed-integer>>;
array(List) ->
    {Length, Data} = encode_array(List, 0, <<>>),
    <<Length:32/signed-integer, Data/binary>>.


%% @private
encode_array([Entry|Rest], Length, Acc) ->
    encode_array(Rest, Length+1, <<Acc/binary, Entry/binary>>);
encode_array([], Length, Acc) ->
    {Length, Acc}.


%% @private
message(Msg) ->
    #req_message{
        offset = Offset,
        magic = Magic,
        attributes = Attr,
        key = Key,
        value = Value
    } = Msg,
    KeyBin = bytes(Key),
    ValueBin = bytes(Value),
    BodyBin = <<Magic:8/signed-integer, Attr:8/signed-integer, KeyBin/binary, ValueBin/binary>>,
    CRC = erlang:crc32(BodyBin),
    Size = erlang:size(BodyBin) + 4,
    <<Offset:64/signed-integer, Size:32/signed-integer, CRC:32/signed-integer, BodyBin/binary>>.


%% @private
messages([Msg|Rest], Acc) ->
    messages(Rest, <<Acc/binary, (message(Msg))/binary>>);
messages([], Acc) ->
    Acc.


%% @private
topic(#req_topic{name=Name, partitions=Partitions}) ->
    NameBin = string(Name),
    PartitionList = [<<Partition:32/signed-integer>> || Partition <- Partitions],
    PartitionsBin = array(PartitionList),
    <<NameBin/binary, PartitionsBin/binary>>.


%% @private
produce_topic(#req_topic{name=Name, partitions=Partitions}) ->
    Name2 = string(Name),
    Partitions2 = [produce_partition(Partition) || Partition <- Partitions],
    Partitions3 = array(Partitions2),
    <<Name2/binary, Partitions3/binary>>.


%% @private
produce_partition(#req_produce_partition{partition =ID, messages=Messages}) ->
    MessageBin = messages(Messages, <<>>),
    Size = erlang:size(MessageBin),
    <<ID:32/signed-integer, Size:32/signed-integer, MessageBin/binary>>.


%% @private
fetch_topic(#req_fetch_topic{name=Name, partitions=Partitions}) ->
    NameBin = string(Name),
    PartitionsList = [fetch_partition(Partition) || Partition <- Partitions],
    PartitionsBin = array(PartitionsList),
    <<NameBin/binary, PartitionsBin/binary>>.


%% @private
fetch_partition(#req_fetch_partition{partition =ID, offset=Offset, max_bytes=MaxBytes}) ->
    <<ID:32/signed-integer, Offset:64/signed-integer, MaxBytes:32/signed-integer>>.


%% @private
offset_topic(#req_offset_topic{name=Name, partitions=Partitions}) ->
    NameBin = string(Name),
    PartitionsList = [offset_partition(Partition) || Partition <- Partitions],
    PartitionsBin = array(PartitionsList),
    <<NameBin/binary, PartitionsBin/binary>>.


%% @private
offset_partition(#req_offset_partition{partition =ID, time=Time, max_num=MaxNum}) ->
    Time2 = case Time of
        first -> -2;
        last -> -1
    end,
    <<ID:32/signed-integer, Time2:64/signed-integer, MaxNum:32/signed-integer>>.


%% @private
offset_commit_topic(#req_offset_commit_topic{name=Name, partitions=Partitions}) ->
    NameBin = string(Name),
    PartitionsList = [offset_commit_partition(Partition) || Partition <- Partitions],
    PartitionsBin = array(PartitionsList),
    <<NameBin/binary, PartitionsBin/binary>>.


%% @private
offset_commit_partition(#req_offset_commit_partition{partition =ID, offset=Offset, metadata=Metadata}) ->
    MetadataBin = string(Metadata),
    <<ID:32/signed-integer, Offset:64/signed-integer, MetadataBin/binary>>.


%% @private
offset_fetch_topic(#req_offset_fetch_topic{name=Name, partitions=Partitions}) ->
    NameBin = string(Name),
    PartitionsList = [<<Partition:32/signed-integer>> || Partition <- Partitions],
    PartitionsBin = array(PartitionsList),
    <<NameBin/binary, PartitionsBin/binary>>.


%% @private
join_group_protocol(#req_join_group_protocol{name=Name, metadata=Metadata}) ->
    NameBin = string(Name),
    MetadataBytes = bytes(Metadata),
    <<NameBin/binary, MetadataBytes/binary>>.


%% @private
sync_group_assignment(#req_sync_group_assignment{member=ID, assignment=Assignment}) ->
    IDBin = string(ID),
    AssignBin = group_member_assignment(Assignment),
    <<IDBin/binary, AssignBin/binary>>.


%% @private
group_member_assignment(#req_group_member_assignment{version=Ver, partitions=Partitions, user_data=UData}) ->
    PartitionsList = [topic(Partition) || Partition <- Partitions],
    PartitionsBin = array(PartitionsList),
    UDataBin = bytes(UData),
    <<Ver:16/signed-integer, PartitionsBin/binary, UDataBin/binary>>.
