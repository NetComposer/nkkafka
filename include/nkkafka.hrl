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

-ifndef(NKKAFKA_HRL_).
-define(NKKAFKA_HRL, 1).

%% ===================================================================
%% Defines
%% ===================================================================

-define(PACKAGE_CLASS_KAFKA, <<"Kafka">>).

%% ===================================================================
%% Records
%% ===================================================================



%% Kafka Protocol API Key
-define(PRODUCE_REQUEST,            0).
-define(FETCH_REQUEST,              1).
-define(OFFSET_REQUEST,             2).
-define(METADATA_REQUEST,           3).
-define(OFFSET_COMMIT_REQUEST,      8).
-define(OFFSET_FETCH_REQUEST,       9).
-define(GROUP_COORDINATOR_REQUEST, 10).
-define(JOIN_GROUP_REQUEST,        11).
-define(HEARTBEAT_REQUEST,         12).
-define(LEAVE_GROUP_REQUEST,       13).
-define(SYNC_GROUP_REQUEST,        14).
-define(DESCRIBE_GROUPS_REQUEST,   15).
-define(LIST_GROUPS_REQUEST,       16).



%% ===================================================================
%% Requests
%% ===================================================================

%% Values for #message_body.attributes
-define(NO_COMPRESSION,     0).
-define(GZIP_COMPRESSION,   1).
-define(SNAPPY_COMPRESSION, 2).

-record(req_message, {
    magic = 0 :: integer(),         %% default 0
    attributes = 0 :: integer(),    %% default ?NO_COMPRESSION
    key :: binary(),
    value :: binary(),
    offset = 0 :: integer()
}).



%% Metadata
-record(req_metadata, {
    topics = [] :: [nkkafka:topic()]
}).


%% Produce
-record(req_produce_partition, {
    partition :: nkkafka:partition(),
    messages :: [#req_message{}]
}).

-record(req_topic, {
    name :: nkkafka:topic(),
    partitions :: [#req_produce_partition{}]
}).

-record(req_produce, {
    ack = all :: none | leader | all,
    timeout = 10000 :: integer(),
    topics :: [#req_topic{}]
}).


%% Fetch
-record(req_fetch_partition, {
    partition :: nkkafka:partition(),
    offset :: integer(),
    max_bytes :: integer()
}).

-record(req_fetch_topic, {
    name  :: nkkafka:topic(),
    partitions :: [#req_fetch_partition{}]
}).

-record(req_fetch, {
    max_wait = 100   :: integer(), %% default 100ms
    min_bytes = 32768 :: integer(), %% default 32k
    topics :: [#req_fetch_topic{}]
}).


%% Offset
-record(req_offset_partition, {
    partition :: nkkafka:partition(),
    time :: first | last,
    max_num = 1 :: integer()
}).

-record(req_offset_topic, {
    name :: nkkafka:topic(),
    partitions :: [#req_offset_partition{}]
}).

-record(req_offset, {
    topics :: [#req_offset_topic{}]
}).


%% Coordinator request
-record(req_group_coordinator, {
    group :: nkkafka_groups:group()
}).


%% Offset Commit
-record(req_offset_commit_partition, {
    partition :: nkkafka:partition(),
    offset :: integer(),
    metadata:: binary()
}).

-record(req_offset_commit_topic, {
    name :: nkkafka:topic(),
    partitions :: [#req_offset_commit_partition{}]
}).

-record(req_offset_commit, {
    group :: nkkafka_groups:group(),
    topics :: [#req_offset_commit_topic{}]
}).


%% Offset Fetch
-record(req_offset_fetch_topic, {
    name :: nkkafka:topic(),
    partitions :: [nkkafka:partition()]
}).

-record(req_offset_fetch, {
    group :: nkkafka_groups:group(),
    topics :: [#req_offset_fetch_topic{}]
}).


%% ListGroups Request
-record(req_list_groups, {}).


%% Describe Groups Request
-record(req_describe_groups, {
    groups :: [nkkafka_groups:group()]
}).


%% Join group
-record(req_join_group_protocol, {
    name = <<>> :: binary(),
    metadata = <<>> :: binary()
}).

-record(req_join_group, {
    group :: nkkafka_groups:group(),
    timeout :: integer(),
    member = <<>> :: binary(),
    proto_type :: binary(),
    protocols = [] :: [#req_join_group_protocol{}]
}).


%% Sync Group
-record(req_group_member_assignment, {
    version :: integer(),
    partitions :: [#req_topic{}],
    user_data :: binary()}).

-record(req_sync_group_assignment, {
    member :: nkkafka_groups:member(),
    assignment:: #req_group_member_assignment{}
}).

-record(req_sync_group, {
    group :: nkkafka_groups:group(),
    generation :: integer(),
    member :: nkkafka_groups:member(),
    assignment :: [#req_sync_group_assignment{}]
}).


%% Heartbeat Request
-record(req_heartbeat, {
    group :: nkkafka_groups:group(),
    generation :: integer(),
    member :: nkkafka_groups:member()
}).


%% Leave Group Request
-record(leave_group_request, {
    group :: nkkafka_groups:group(),
    member :: nkkafka_groups:member()
}).













-endif.

