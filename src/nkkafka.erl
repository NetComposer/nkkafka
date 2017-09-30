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

%% @doc NkKAFKA application

-module(nkkafka).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([start_producer/3, start_producer/4, get_producer/4]).
-export([produce/2, produce/3, produce/6, produce_sync/2, produce_sync/3, produce_sync/6]).
-export([start_consumer/3, start_consumer/4, get_consumer/4]).
-export([subscribe/5, subscribe/6, unsubscribe/2, unsubscribe/5, consume_ack/2, consume_ack/5]).
-export([get_partitions_count/3, find_client/2]).
-export_type([client_config/0]).


%% ===================================================================
%% Types
%% ===================================================================

-type client_id() :: binary().
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

-type client_config() ::       %% See brod.erl
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


-type producer_config() ::  %% See brod_producer.erl
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

-type consumer_config() ::              %% See brod_consumer.erl
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


%% ===================================================================
%% API
%% ===================================================================




%% @doc Starts the per-topic producer supervisor
%% Then, gets the list of partitions for the topic and starts a supervisor
%% for each partition
-spec start_producer(nkservice:id(), client_id(), topic()) ->
    ok | {error, term()}.

start_producer(SrvId, ClientId, Topic) ->
    start_producer(SrvId, ClientId, Topic, #{}).


%% @doc
-spec start_producer(nkservice:id(), client_id(), topic(), producer_config()) ->
    ok | {error, term()}.

start_producer(SrvId, ClientId, Topic, Config) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            brod:start_producer(Pid, to_bin(Topic), maps:to_list(Config));
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec get_partitions_count(nkservice:id(), client_id(), topic()) ->
    {ok, pos_integer()} | {error, term()}.

get_partitions_count(SrvId, ClientId, Topic) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            brod:get_partitions_count(Pid, to_bin(Topic));
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec get_producer(nkservice:id(), client_id(), topic(), partition()) ->
    {ok, pid()} | {error, term()}.

get_producer(SrvId, ClientId, Topic, Partition) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            brod:get_producer(Pid, to_bin(Topic), Partition);
        {error, Error} ->
            {error, Error}
    end.


%% @equiv produce(Pid, 0, <<>>, Value)
-spec produce(pid(), value()) ->
    {ok, call_ref()} | {error, any()}.

produce(Pid, Value) ->
    produce(Pid, _Key = <<>>, Value).

%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is discarded (only the keys in kv-list are sent to kafka).
%% The pid should be a partition producer pid, NOT client pid.
-spec produce(pid(), key(), value()) ->
    {ok, call_ref()} | {error, any()}.

produce(ProducerPid, Key, Value) ->
    brod_producer:produce(ProducerPid, Key, Value).


%% @doc Produce one message if Value is binary or iolist,
%% or a message set if Value is a (nested) kv-list, in this case Key
%% is used only for partitioning (or discarded if Partition is used
%% instead of PartFun).
%% This function first lookup the producer pid,
%% then call produce/3 to do the real work.
%% @end
-spec produce(nkservice:id(), client_id(), topic(), partition() | partition_fun(), key(), value()) ->
    {ok, call_ref()} | {error, any()}.

produce(SrvId, ClientId, Topic, PartFun, Key, Value) when is_function(PartFun) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            case brod_client:get_partitions_count(Pid, Topic) of
                {ok, PartitionsCnt} ->
                    {ok, Partition} = PartFun(Topic, PartitionsCnt, Key, Value),
                    produce(SrvId, ClientId, Topic, Partition, Key, Value);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Error} ->
            {error, Error}
    end;

produce(SrvId, ClientId, Topic, Partition, Key, Value) when is_integer(Partition) ->
    case get_producer(SrvId, ClientId, Topic, Partition) of
        {ok, Pid} ->
            produce(Pid, Key, Value);
        {error, Reason} ->
            {error, Reason}
    end.


%% @equiv produce_sync(Pid, 0, <<>>, Value)
-spec produce_sync(pid(), value()) ->
    ok.

produce_sync(Pid, Value) ->
    produce_sync(Pid, _Key = <<>>, Value).


%% @doc Sync version of produce/3
%% This function will not return until a response is received from kafka,
%% however if producer is started with required_acks set to 0, this function
%% will return once the messages is buffered in the producer process.
%% @end
-spec produce_sync(pid(), key(), value()) ->
    ok | {error, any()}.

produce_sync(Pid, Key, Value) ->
    case produce(Pid, Key, Value) of
        {ok, CallRef} ->
            %% Wait until the request is acked by kafka
            brod:sync_produce_request(CallRef);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Sync version of produce/5
%% This function will not return until a response is received from kafka,
%% however if producer is started with required_acks set to 0, this function
%% will return once the messages are buffered in the producer process.
%% @end
-spec produce_sync(nkservice:id(), client_id(), topic(), partition() | partition_fun(), key(), value()) ->
    ok | {error, any()}.

produce_sync(SrvId, ClientId, Topic, Partition, Key, Value) ->
    case produce(SrvId, ClientId, Topic, Partition, Key, Value) of
        {ok, CallRef} ->
            brod:sync_produce_request(CallRef);
        {error, Reason} ->
            {error, Reason}
    end.


%% @doc Starts the per-topic producer supervisor
%% Then, gets the list of partitions for the topic and starts a supervisor
%% for each partition
-spec start_consumer(nkservice:id(), client_id(), topic()) ->
    ok | {error, term()}.

start_consumer(SrvId, ClientId, Topic) ->
    start_consumer(SrvId, ClientId, Topic, #{}).


%% @doc
-spec start_consumer(nkservice:id(), client_id(), topic(), consumer_config()) ->
    ok | {error, term()}.

start_consumer(SrvId, ClientId, Topic, Config) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            brod:start_consumer(Pid, to_bin(Topic), maps:to_list(Config));
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec get_consumer(nkservice:id(), client_id(), topic(), partition()) ->
    {ok, pid()} | {error, term()}.

get_consumer(SrvId, ClientId, Topic, Partition) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            brod:get_producer(Pid, to_bin(Topic), Partition);
        {error, Error} ->
            {error, Error}
    end.



%% @doc
-spec subscribe(nkservice:id(), client_id(), pid(), topic(), partition(), consumer_config()) ->
    {ok, pid()} | {error, term()}.

subscribe(SrvId, ClientId, SubscriberPid, Topic, Partition, Opts) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            brod:subscribe(Pid, SubscriberPid, to_bin(Topic), Partition, maps:to_list(Opts));
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec subscribe(pid(), pid(), topic(), partition(), consumer_config()) ->
    {ok, pid()} | {error, term()}.

subscribe(ConsumerPid, SubscriberPid, Topic, Partition, Opts) ->
    brod:subscribe(ConsumerPid, SubscriberPid, to_bin(Topic), Partition, maps:to_list(Opts)).


%% @doc
-spec unsubscribe(nkservice:id(), client_id(), topic(), partition(), pid()) ->
    {ok, pid()} | {error, term()}.

unsubscribe(SrvId, ClientId, Topic, Partition, SubscriberPid) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            brod:unsubscribe(Pid, to_bin(Topic), Partition, SubscriberPid);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec unsubscribe(pid(), pid()) ->
    ok | {error, term()}.

unsubscribe(ConsumerPid, SubscriberPid) ->
    brod:unsubscribe(ConsumerPid, SubscriberPid).


%% @doc
-spec consume_ack(nkservice:id(), client_id(), topic(), partition(), offset()) ->
    ok | {error, term()}.

consume_ack(SrvId, ClientId, Topic, Partition, Offset) ->
    case find_client(SrvId, ClientId) of
        {ok, Pid} ->
            brod:consume_ack(Pid, to_bin(Topic), Partition, Offset);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec consume_ack(pid(), offset()) ->
    ok | {error, term()}.

consume_ack(ConsumerPid, Offset) ->
    brod:consume_ack(ConsumerPid, Offset).





%% ===================================================================
%% Internal
%% ===================================================================


%% @private
find_client(SrvId, ClientId) ->
    Clients = SrvId:config_nkkafka(),
    case maps:find(to_bin(ClientId), Clients) of
        {ok, Name} ->
            {ok, Name};
        error ->
            {error, client_not_found}
    end.


%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).

