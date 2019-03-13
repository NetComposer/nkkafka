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

%% @doc NkKAFKA API
%% After starting the plugin, you can start producing and subscribing to topics
%% Default config will we taken from plugin's config
%% Plugin also allows to create consumer groups (equivalent to start_link_group_subscriber)

-module(nkkafka).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([start_producer/3, start_producer/2, get_producer/3]).
-export([produce/5, produce_sync/5]).
-export([start_consumer/2, start_consumer/3, get_consumer/3]).
-export([subscribe/4, subscribe/5, unsubscribe/4]).
-export([consume_ack/4]).
-export([get_partitions_count/2, find_client/1]).
-export([start_link_topic_subscriber/8, topic_subscriber_stop/1, topic_subscriber_ack/3]).
-export([start_link_group_subscriber/8, group_subscriber_stop/1, group_subscriber_ack/4]).
-export_type([client_config/0, msg/0]).

-include_lib("brod/include/brod.hrl").

-define(LLOG(Type, Txt, Args),lager:Type("NkKAFKA "++Txt, Args)).

%% ===================================================================
%% Types
%% ===================================================================

-type srv_id() :: srv_id().
-type topic() :: undefined | string() | binary().
-type partition() :: pos_integer().
-type key() :: iodata().
-type value() :: iodata() | [{key(), kv_list()}].
-type kv_list() :: [{key(), value()} | {msg_ts(), key(), value()}].
-type msg_ts() :: integer().
-type call_ref() :: term().
-type partition_fun() ::
    fun((topic(), partition(), key(), value()) -> {ok, partition()}).

-type offset() :: integer().
-type offset_reset_policy() :: reset_by_subscriber | reset_to_earliest | reset_to_latest.

-type group_id() :: binary().

-type client_config() ::       %% @see brod.erl
    #{
        restart_delay_seconds => integer(),         % default=10
        max_metadata_sock_retry => integer(),       % default=1
        get_metadata_timeout_seconds => integer(),  % default=5
        reconnect_cool_down_seconds => integer(),   % default=1
        allow_topic_auto_creation => boolean(),     % default=true
        auto_start_producers => boolean(),          % default=false
        ssl => boolean() | #{certfile=>binary(), keyfile=>binary(), cacertfile=>binary()},
        sasl => undefined | {plain, User::string(), Pass::string()},
        connect_timeout => integer(),               % default=5000
        request_timeout => integer()                % default=240000
    }.


-type producer_config() ::  %% @see brod_producer.erl
    #{
        required_acks => integer(),   % 0:none, 1:leader wait, -1:all replicas (default),
        ack_timeout => integer(),                           % default = 10000 ms
        partition_buffer_limit => integer(),                % default = 256
        partition_onwire_limit => integer(),                % default = 1
        max_batch_size => integer(),                        % bytes, default = 1M
        max_retries => integer(),                           % default = 3
        retry_backoff_ms => integer(),                      % default = 500 ms
        compression => no_compression | gzip | snappy,      % default = no_compression
        min_compression_batch_size => integer(),            % bytes, default = 1K
        max_linger_ms => integer(),                         % default = 0
        max_linger_count => integer()                       % default = 0
    }.

-type consumer_config() ::              %% @see brod_consumer.erl
    #{
        min_bytes => pos_integer(),             % default = 0
        max_bytes => pos_integer(),             % default = 1MB
        max_wait_time => pos_integer(),         % default = 10000 ms
        sleep_timeout => pos_integer(),         % default = 1000 ms
        prefetch_count => pos_integer(),        % default = 1
        begin_offset => pos_integer() | latest, % default = latest
        offset_reset_policy => offset_reset_policy(),       % default = reset_by_subscriber
        size_stat_window => pos_integer()       % default = 5
    }.

-type group_config() ::                 %% @see brod_group_coordinator.erl
    #{
        partition_assignment_strategy => roundrobin | callback_implemented,     % roundrobin
        session_timeout_seconds => integer(),                                   % 10
        heartbeat_rate_seconds => integer(),                                    % 2
        max_rejoin_attempts => integer(),                                       % 5
        rejoin_delay_seconds => integer(),                                      % 1
        offset_commit_policy => commit_to_kafka_v2 | consumer_managed,          % commit_to_kafka_v2
        offset_commit_interval_seconds => integer(),                            % 5
        offset_retention_seconds => -1 | pos_integer(),                         % -1
        protocol_name => atom()                                                 % roundrobin
    }.

-type msg() ::
    #{
        srv_id => srv_id(),
        group_id => group_id(),
        topic => topic(),
        partition => partition(),
        pid => pid(),                   % Subscriber
        offset => offset(),
        key => binary(),
        value => binary(),
        ts => integer()
    }.


%% ===================================================================
%% API - Producers
%% After having a client, you can start any number of producers
%% Each client will start a single TCP connection
%% ===================================================================


%% @doc Starts the per-topic producer supervisor
%% Then, gets the list of partitions for the topic and starts a supervisor
%% for each partition. Get the pids with get_producer/3
%% It is called automatically by produce/6
-spec start_producer(srv_id(), topic()) ->
    ok | {error, term()}.

start_producer(SrvId, Topic) ->
    start_producer(SrvId, Topic, #{}).


%% @doc
-spec start_producer(srv_id(), topic(), producer_config()) ->
    ok | {error, term()}.

start_producer(SrvId, Topic, Config) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            Config2 = producer_config(SrvId, Config),
            brod:start_producer(BrodClient, to_bin(Topic), Config2);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec get_partitions_count(srv_id(), topic()) ->
    {ok, pos_integer()} | {error, term()}.

get_partitions_count(SrvId, Topic) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            brod:get_partitions_count(BrodClient, to_bin(Topic));
        {error, Error} ->
            {error, Error}
    end.


%% @doc
%% Really fast operation
-spec get_producer(srv_id(), topic(), partition()) ->
    {ok, pid()} | {error, {producer_not_found, binary(), integer()}|term()}.

get_producer(SrvId, Topic, Partition) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            brod:get_producer(BrodClient, to_bin(Topic), Partition);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is used only for partitioning (or discarded if Partition is used
%% instead of PartFun).
%% This function first lookup the producer pid,
%% then call produce/3 to do the real work.
%% @end
-spec produce(srv_id(), topic(), partition() | partition_fun() | random, key(), value()) ->
    {ok, call_ref()} | {error, any()}.


produce(SrvId, Topic, random, Key, Value) ->
    produce(SrvId, Topic, fun random_partition/4, Key, Value);

produce(SrvId, Topic, PartFun, Key, Value) when is_function(PartFun) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            case brod_client:get_partitions_count(BrodClient, Topic) of
                {ok, PartitionsCnt} ->
                    {ok, Partition} = PartFun(Topic, PartitionsCnt, Key, Value),
                    produce(SrvId, Topic, Partition, Key, Value);
                {error, Reason} ->
                    {error1, Reason}
            end;
        {error, Error} ->
            {error, Error}
    end;

produce(SrvId, Topic, Partition, Key, Value) when is_integer(Partition) ->
    case get_producer(SrvId, Topic, Partition) of
        {ok, Pid} ->
            nkkafka_util:produce(Pid, Key, Value);
        {error, {producer_not_found, _}} ->
            ?LLOG(notice, "auto starting producer ~s:~s", [SrvId, Topic]),
            case start_producer(SrvId, Topic) of
                ok ->
                    case get_producer(SrvId, Topic, Partition) of
                        {ok, Pid} ->
                            nkkafka_util:produce(Pid, Key, Value);
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc Sync version of produce/5
%% This function will not return until a response is received from kafka,
%% however if producer is started with required_acks set to 0, this function
%% will return once the messages are buffered in the producer process.
%% @end
-spec produce_sync(srv_id(), topic(), partition() | partition_fun(), key(), value()) ->
    ok | {error, any()}.

produce_sync(SrvId, Topic, Partition, Key, Value) ->
    case produce(SrvId, Topic, Partition, Key, Value) of
        {ok, CallRef} ->
            brod:sync_produce_request(CallRef);
        {error, Reason} ->
            {error, Reason}
    end.



%% ===================================================================
%% API - Consumers
%% After having a client, you can start any number of consumers
%% ===================================================================


%% @doc Starts the per-topic consumer supervisor
%% Then, gets the list of partitions for the topic and starts a supervisor
%% for each partition
-spec start_consumer(srv_id(), topic()) ->
    ok | {error, term()}.

start_consumer(SrvId, Topic) ->
    start_consumer(SrvId, Topic, #{}).


%% @doc
-spec start_consumer(srv_id(), topic(), consumer_config()) ->
    ok | {error, term()}.

start_consumer(SrvId, Topic, Config) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            Config2 = consumer_config(SrvId, Config),
            brod:start_consumer(BrodClient, to_bin(Topic), Config2);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec get_consumer(srv_id(), topic(), partition()) ->
    {ok, pid()} | {error, term()}.

get_consumer(SrvId, Topic, Partition) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            brod:get_consumer(BrodClient, to_bin(Topic), Partition);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Tell consumer process to fetch more (if pre-fetch count allows).
-spec consume_ack(srv_id(), topic(), partition(), offset()) ->
    ok | {error, term()}.

consume_ack(SrvId, Topic, Partition, Offset) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            brod:consume_ack(BrodClient, to_bin(Topic), Partition, Offset);
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% API - Subscriptions
%% Subscriptions are the lower-level way to access messages from Kafka
%% You must start a consumer first
%% ===================================================================


%% @doc Simple, low-level subscription for a topic and partition
%% It adds a subscription to a consumer process
%% Starts it if it is not yet started
%% If the consumer dies, subscriptions will be lost
%% The defined Pid will receive messages like
%% {ConsumerPid, #kafka_message_set{}} and
%% {ConsumerPid, #kafka_fetch_error{}} (MUST RESUBSCRIBE)
%%
%% @see brod_demo_cg_collector (reads data from __consumer_offsets) and
%% brod_demo_topic_subscriber

-spec subscribe(srv_id(), pid(), topic(), partition()) ->
    {ok, ConsumerPid::pid()} | {error, term()}.

subscribe(SrvId, Pid, Topic, Partition) ->
    subscribe(SrvId, Pid, Topic, Partition, #{}).

%% @doc
%% Performs an update of the consumer config
-spec subscribe(srv_id(), pid(), topic(), partition(), consumer_config()) ->
    {ok, ConsumerPid::pid()} | {error, term()}.

subscribe(SrvId, Pid, Topic, Partition, Config) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            % Config2 is used to update the consumer's config
            Config2 = consumer_config(SrvId, Config),
            case brod:subscribe(BrodClient, Pid, to_bin(Topic), Partition, Config2) of
                {ok, ConsumerPid} ->
                    {ok, ConsumerPid};
                {error, {consumer_not_found, _}} ->
                    ?LLOG(notice, "auto starting consumer ~s:~s", [SrvId, Topic]),
                    case start_consumer(SrvId, Topic) of
                        ok ->
                            brod:subscribe(BrodClient, Pid, to_bin(Topic), Partition, Config2);
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec unsubscribe(srv_id(), topic(), partition(), pid()) ->
    ok | {error, ignored|term()}.

unsubscribe(SrvId, Topic, Partition, Pid) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            brod:unsubscribe(BrodClient, to_bin(Topic), Partition, Pid);
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% API - TOPIC Subscriptions
%% High level way to access data
%% It takes care of partitions, and calls a function for each incoming
%% You must take care for already processed offsets
%% ===================================================================

%% @doc Starts a high-level topic subscriber, for all or some partitions
%% Must indicate 'last_seen' offsets. If [], consumer will use 'latest' (or other in config)
%%
%% It will start a consumer and call the function for each message or message set
%% If the function replies {ok, State} instead of {ok, ack, State} it should call topic_subscriber_ack/3
%% Client must restart the process if failed

-spec start_link_topic_subscriber(srv_id(), topic(), all | [partition()],
                                  consumer_config(), [{partition(), offset()}],
                                  message | message_set, brod_topic_subscriber:cb_fun(), term()) ->
     {ok, pid()} | {error, any()}.

start_link_topic_subscriber(SrvId, Topic, Partitions, ConsumerConfig, Offsets, MessageType, Fun, State) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            brod_topic_subscriber:start_link(BrodClient, to_bin(Topic), Partitions,
                                             maps:to_list(ConsumerConfig),
                                             MessageType, Offsets, Fun, State);
        {error, Error} ->
            {error, Error}
    end.


%% @doc See above
%% Will call consume_ack/2
-spec topic_subscriber_ack(pid(), partition(), offset()) ->
    ok.

topic_subscriber_ack(Pid, Partition, Offset) ->
    brod_topic_subscriber:ack(Pid, Partition, Offset).



%% @doc Stops the subscriber and waits for it coming down
-spec topic_subscriber_stop(pid()) ->
    ok.

topic_subscriber_stop(Pid) ->
    brod_topic_subscriber:stop(Pid).


%% ==================================================================================
%% API - GROUPS Subscriptions
%% Higher level way to access data
%% It takes care of several topics and partitions, and uses a callback module
%% It can store the acknowledged commits to Kafka
%% Multiple nodes can start a group with the same name (even with different topics)
%% and partitions will be assigned based on selected algorithm
%% (roundrobin is the only one supported)
%%
%% They can be started directly from the plugin config
%%
%% ===============================================================================

%% @doc Starts a high level subscription to a number of topics, all partitions
%% Module needs to have callbacks (@see brod_group_subscriber)
%% - init/2
%% - handle_message/4
%% - get_committed_offsets/3 (only if not committing offsets to Kafka)
%% - assign_partitions/3 (only if partition_management_strategy' is 'callback_implemented')
%%
%% @see brod_demo_group_subscriber_koc and brod_demo_group_subscriber_loc
%%
%% You could also create the coordinator yourself and use brod_group_member behavior

-spec start_link_group_subscriber(srv_id(), group_id(), [topic()], group_config(),
                                  consumer_config(), message | message_type, module(), term()) ->
    {ok, pid()} | {error, any()}.

start_link_group_subscriber(SrvId, GroupId, Topics, GroupConfig, ConsumerConfig,
                            MessageType, Mod, Args) ->
    case find_client(SrvId) of
        {ok, BrodClient} ->
            Topics2 = [to_bin(Topic) || Topic <- Topics],
            brod_group_subscriber:start_link(BrodClient, to_bin(GroupId), Topics2,
                                             maps:to_list(GroupConfig),
                                             maps:to_list(ConsumerConfig),
                                             MessageType, Mod, Args);
        {error, Error} ->
            {error, Error}
    end.


%% @doc See above
%% Will call consume_ack/2 and the calls the group coordinator
-spec group_subscriber_ack(pid(), topic(), partition(), offset()) ->
    ok.

group_subscriber_ack(Pid, Topic, Partition, Offset) ->
    brod_group_subscriber:ack(Pid, to_bin(Topic), Partition, Offset).


%% @doc Stops the subscriber and waits for it coming down
-spec group_subscriber_stop(pid()) ->
    ok.

group_subscriber_stop(Pid) ->
    brod_group_subscriber:stop(Pid).



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
%% Returns the registered name of the process
find_client(SrvId) ->
    case nkserver:get_cached_config(SrvId, nkkafka, brod_client_id) of
        undefined ->
            {error, client_not_found};
        BrodClientId ->
            {ok, BrodClientId}
    end.


%% @private
producer_config(SrvId, Config) ->
    Base = nkserver:get_cached_config(SrvId, nkkafka, producer_config),
    maps:to_list(maps:merge(Base, Config)).


%% @private
consumer_config(SrvId, Config) ->
    Base = nkserver:get_cached_config(SrvId, nkkafka, consumer_config),
    maps:to_list(maps:merge(Base, Config)).


%% @private
random_partition(_Topic, Partitions, _Key, _Value) ->
    {ok, nklib_util:rand(0, Partitions-1)}.


%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).

