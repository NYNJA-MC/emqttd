%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqttd_server).

-behaviour(gen_server2).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-include_lib("kernel/include/logger.hrl").

-export([start_link/3]).

%% PubSub API.
-export([subscribe/1, subscribe/2, subscribe/3, publish/1,
         unsubscribe/1, unsubscribe/2]).

%% Async PubSub API.
-export([async_subscribe/1, async_subscribe/2, async_subscribe/3,
         async_unsubscribe/1, async_unsubscribe/2]).

%% Management API.
-export([setqos/3]).

%% Debug API
-export([dump/0]).

%% gen_server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, env, submon :: emqttd_pmon:pmon()}).

%% @doc Start server
-spec(start_link(atom(), pos_integer(), list()) -> {ok, pid()} | ignore | {error, any()}).
start_link(Pool, Id, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id, Env], []).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% @doc Subscribe a Topic
-spec(subscribe(binary()) -> ok | emqttd:pubsub_error()).
subscribe(Topic) when is_binary(Topic) ->
    subscribe(Topic, self()).

-spec(subscribe(binary(), emqttd:subscriber()) -> ok | emqttd:pubsub_error()).
subscribe(Topic, Subscriber) when is_binary(Topic) ->
    subscribe(Topic, Subscriber, []).

-spec(subscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) ->
      ok | emqttd:pubsub_error()).
subscribe(Topic, Subscriber, Options) when is_binary(Topic) ->
    call(pick(Subscriber), {subscribe, Topic, Subscriber, Options}).

%% @doc Subscribe a Topic Asynchronously
-spec(async_subscribe(binary()) -> ok).
async_subscribe(Topic) when is_binary(Topic) ->
    async_subscribe(Topic, self()).

-spec(async_subscribe(binary(), emqttd:subscriber()) -> ok).
async_subscribe(Topic, Subscriber) when is_binary(Topic) ->
    async_subscribe(Topic, Subscriber, []).

-spec(async_subscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
async_subscribe(Topic, Subscriber, Options) when is_binary(Topic) ->
    cast(pick(Subscriber), {subscribe, Topic, Subscriber, Options}).

%% @doc Publish message to Topic.
-spec(publish(mqtt_message()) -> {ok, mqtt_delivery()} | ignore).
publish(Msg = #mqtt_message{from = From}) ->
    trace(publish, From, Msg),
    case emqttd_hooks:run('message.publish', [], Msg) of
        {ok, Msg1 = #mqtt_message{topic = Topic}} ->
            emqttd_pubsub:publish(Topic, Msg1);
        {stop, Msg1} ->
            ?LOG_WARNING("Stop publishing: ~s", [emqttd_message:format(Msg1)]),
            ignore
    end.

%% @private
trace(_, _, _) -> ok.

%% @doc Unsubscribe
-spec(unsubscribe(binary()) -> ok | emqttd:pubsub_error()).
unsubscribe(Topic) when is_binary(Topic) ->
    unsubscribe(Topic, self()).

%% @doc Unsubscribe
-spec(unsubscribe(binary(), emqttd:subscriber()) -> ok | emqttd:pubsub_error()).
unsubscribe(Topic, Subscriber) when is_binary(Topic) ->
    call(pick(Subscriber), {unsubscribe, Topic, Subscriber}).

%% @doc Async Unsubscribe
-spec(async_unsubscribe(binary()) -> ok).
async_unsubscribe(Topic) when is_binary(Topic) ->
    async_unsubscribe(Topic, self()).

-spec(async_unsubscribe(binary(), emqttd:subscriber()) -> ok).
async_unsubscribe(Topic, Subscriber) when is_binary(Topic) ->
    cast(pick(Subscriber), {unsubscribe, Topic, Subscriber}).

setqos(Topic, Subscriber, Qos) when is_binary(Topic) ->
    call(pick(Subscriber), {setqos, Topic, Subscriber, Qos}).

-spec(subscriptions(emqttd:subscriber()) ->
             [{binary(), emqttd:subscriber(), list(emqttd:suboption())}]).
subscriptions(Subscriber) ->
    lists:map(fun({mqtt_subscription, _, {_Share, Topic}}) ->
                subscription(Topic, Subscriber);
                 ({mqtt_subscription,_, Topic}) ->
                subscription(Topic, Subscriber)
        end, ets:lookup(mqtt_subscription, Subscriber)).

subscription(Topic, Subscriber) ->
    {Topic, Subscriber, ets:lookup_element(mqtt_subproperty, {Topic, Subscriber}, 3)}.

subscribers(Topic) ->
    emqttd_pubsub:subscribers(Topic).

-spec(is_subscribed(binary(), emqttd:subscriber()) -> boolean()).
is_subscribed(Topic, Subscriber) when is_binary(Topic) ->
    ets:member(mqtt_subproperty, {Topic, Subscriber}).


call(Server, Req) ->
    gen_server2:call(Server, Req, infinity).

cast(Server, Msg) when is_pid(Server) ->
    gen_server2:cast(Server, Msg).

pick(Subscriber) ->
    gproc_pool:pick_worker(server, Subscriber).

dump() ->
    [{Tab, ets:tab2list(Tab)} || Tab <- [mqtt_subproperty, mqtt_subscription, mqtt_subscriber]].

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Env]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, env = Env, submon = emqttd_pmon:new()}}.

handle_call({subscribe, Topic, Subscriber, Options}, _From, State) ->
    case do_subscribe_(Topic, Subscriber, Options, State) of
        {ok, NewState} -> {reply, ok, setstats(NewState)};
        {error, Error} -> {reply, {error, Error}, State}
    end;

handle_call({unsubscribe, Topic, Subscriber}, _From, State) ->
    case do_unsubscribe_(Topic, Subscriber, State) of
        {ok, NewState} -> {reply, ok, setstats(NewState), hibernate};
        {error, Error} -> {reply, {error, Error}, State}
    end;

handle_call({setqos, Topic, Subscriber, Qos}, _From, State) ->
    Key = {Topic, Subscriber},
    case ets:lookup(mqtt_subproperty, Key) of
        [{mqtt_subproperty, _, Opts}] ->
            Opts1 = lists:ukeymerge(1, [{qos, Qos}], Opts),
            ets:insert(mqtt_subproperty, #mqtt_subproperty{key = Key, value = Opts1}),
            {reply, ok, State};
        [] ->
            {reply, {error, {subscription_not_found, Topic}}, State}
    end;

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({subscribe, Topic, Subscriber, Options}, State) ->
    case do_subscribe_(Topic, Subscriber, Options, State) of
        {ok, NewState}  -> {noreply, setstats(NewState)};
        {error, _Error} -> {noreply, State}
    end;

handle_cast({unsubscribe, Topic, Subscriber}, State) ->
    case do_unsubscribe_(Topic, Subscriber, State) of
        {ok, NewState}  -> {noreply, setstats(NewState), hibernate};
        {error, _Error} -> {noreply, State}
    end;

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({'DOWN', _MRef, process, DownPid, _Reason}, State = #state{submon = PM}) ->
    {noreply, setstats(State#state{submon = PM:erase(DownPid)}), hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

do_subscribe_(Topic, Subscriber, Options, State) ->
    case ets:lookup(mqtt_subproperty, {Topic, Subscriber}) of
        [] ->
            emqttd_pubsub:subscribe(Topic, Subscriber, Options),
            {ok, monitor_subpid(Subscriber, State)};
        [_] ->
            {error, {already_subscribed, Topic}}
    end.

monitor_subpid(SubPid, State = #state{submon = PMon}) when is_pid(SubPid) ->
    State#state{submon = PMon:monitor(SubPid)};
monitor_subpid(_SubPid, State) ->
    State.

do_unsubscribe_(Topic, Subscriber, State) ->
    case ets:lookup(mqtt_subproperty, {Topic, Subscriber}) of
        [{mqtt_subproperty, _, Options}] ->
            emqttd_pubsub:unsubscribe(Topic, Subscriber, Options),
            {ok, case ets:member(mqtt_subscription, Subscriber) of
                true  -> State;
                false -> demonitor_subpid(Subscriber, State)
            end};
        [] ->
            {error, {subscription_not_found, Topic}}
    end.

demonitor_subpid(SubPid, State = #state{submon = PMon}) when is_pid(SubPid) ->
    State#state{submon = PMon:demonitor(SubPid)};
demonitor_subpid(_SubPid, State) ->
    State.

setstats(State) ->
    emqttd_stats:setstats('subscriptions/count', 'subscriptions/max',
                          ets:info(mqtt_subscription, size)), State.

