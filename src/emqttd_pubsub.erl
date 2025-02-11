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

-module(emqttd_pubsub).
-behaviour(gen_server2).
-author("Feng Lee <feng@emqtt.io>").
-include("emqttd.hrl").
-include("emqttd_internal.hrl").

-compile(export_all).
-export([start_link/3]).

%% PubSub API.
-export([subscribe/3, async_subscribe/3, publish/1, publish/2, setqos/3,
         unsubscribe/2, unsubscribe/3, unsubscribe_client/1,
         async_unsubscribe/3, subscribers/1, subscriptions/1]).

-export([dispatch/2]).

%% gen_server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, env}).

-define(PUBSUB, ?MODULE).

-define(is_local(Options), lists:member(local, Options)).

%%--------------------------------------------------------------------
%% Start PubSub
%%--------------------------------------------------------------------

-spec(start_link(atom(), pos_integer(), list()) -> {ok, pid()} | ignore | {error, any()}).
start_link(Pool, Id, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id, Env], []).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

setqos(Topic, Subscriber, Qos) when is_binary(Topic) ->
    call(pick(Subscriber), {setqos, Topic, Subscriber, Qos}).

%% @doc Subscribe a Topic
-spec(subscribe(binary() | list(binary()), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
subscribe(Topics, Subscriber, Options) ->
    call(pick(Subscriber), {subscribe, Topics, Subscriber, Options}).

-spec(async_subscribe(binary() | list(binary()), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
async_subscribe(Topics, Subscriber, Options) ->
    cast(pick(Subscriber), {subscribe, Topics, Subscriber, Options}).

-spec(publish(mqtt_message()) -> {ok, mqtt_delivery()} | ignore).
publish(Msg = #mqtt_message{}) ->
    case emqttd_hooks:run('message.publish', [], Msg) of
        {ok, Msg1 = #mqtt_message{topic = Topic}} ->
            publish(Topic, Msg1);
        {stop, Msg1} ->
            ?LOG_WARNING("Stop publishing: ~s", [emqttd_message:format(Msg1)]),
            ignore
    end.

%% @doc Publish MQTT Message to Topic
-spec(publish(binary(), any()) -> {ok, mqtt_delivery()} | ignore).
publish(Topic, Msg) ->
    route(lists:append(emqttd_router:match(Topic),
                       emqttd_router:match_local(Topic)), delivery(Msg)).

route([], #mqtt_delivery{message = #mqtt_message{topic = Topic}}) ->
    dropped(Topic), ignore;

%% Dispatch on the local node
route([#mqtt_route{topic = To, node = Node}],
      Delivery = #mqtt_delivery{flows = Flows}) when Node =:= node() ->
    dispatch(To, Delivery#mqtt_delivery{flows = [{route, Node, To} | Flows]});

%% Forward to other nodes
route([#mqtt_route{topic = To, node = Node}], Delivery = #mqtt_delivery{flows = Flows}) ->
    forward(Node, To, Delivery#mqtt_delivery{flows = [{route, Node, To}|Flows]});

route(Routes, Delivery) ->
    {ok, lists:foldl(fun(Route, DelAcc) ->
                    {ok, DelAcc1} = route([Route], DelAcc), DelAcc1
            end, Delivery, Routes)}.

delivery(Msg) -> #mqtt_delivery{sender = self(), message = Msg, flows = []}.

%% @doc Forward message to another node...
forward(Node, To, Delivery) ->
    rpc:cast(Node, ?PUBSUB, dispatch, [To, Delivery]), {ok, Delivery}.

%% @doc Dispatch Message to Subscribers
-spec(dispatch(binary(), mqtt_delivery()) -> mqtt_delivery()).
dispatch(Topic, Delivery = #mqtt_delivery{message = Msg, flows = Flows}) ->
    case subscribers(Topic) of
        [] ->
            dropped(Topic), {ok, Delivery};
        [Sub] -> %% optimize?
            dispatch(Sub, Topic, Msg),
            {ok, Delivery#mqtt_delivery{flows = [{dispatch, Topic, 1}|Flows]}};
        Subscribers ->
            Flows1 = [{dispatch, Topic, length(Subscribers)} | Flows],
            lists:foreach(fun(Sub) -> dispatch(Sub, Topic, Msg) end, Subscribers),
            {ok, Delivery#mqtt_delivery{flows = Flows1}}
    end.

dispatch(Pid, Topic, Msg) when is_pid(Pid) ->
    Pid ! {dispatch, Topic, Msg};
dispatch(SubId, Topic, Msg) when is_binary(SubId) ->
    emqttd_sm:dispatch(SubId, Topic, Msg);
dispatch({_Share, [Sub]}, Topic, Msg) ->
    dispatch(Sub, Topic, Msg);
dispatch({_Share, []}, _Topic, _Msg) ->
    ok;
%%TODO: round-robbin
dispatch({_Share, Subs}, Topic, Msg) ->
    dispatch(lists:nth(rand:uniform(length(Subs)), Subs), Topic, Msg).

subscribers(Topic) ->
    Subscribers = ets:match(mqtt_subscriber, {{Topic, '$1'}}),
    group_by_share([S || [S] <- Subscribers]).

group_by_share([]) -> [];

group_by_share(Subscribers) ->
    {Subs1, Shares1} =
    lists:foldl(fun({Share, Sub}, {Subs, Shares}) ->
                    {Subs, dict:append(Share, Sub, Shares)};
                   (Sub, {Subs, Shares}) ->
                    {[Sub|Subs], Shares}
                end, {[], dict:new()}, Subscribers),
    lists:append(Subs1, dict:to_list(Shares1)).

-spec(subscriptions(emqttd:subscriber()) ->
             [{binary(), emqttd:subscriber(), list(emqttd:suboption())}]).
subscriptions(Subscriber) ->
    Topics = ets:match(mqtt_subscription, {{Subscriber, '$1'}}),
    [ subscription(Topic, Subscriber) || [Topic] <- Topics ].

subscription(Topic, Subscriber) ->
    {Topic, Subscriber, ets:lookup_element(mqtt_subproperty, {Topic, Subscriber}, 3)}.

%% @private
%% @doc Ingore $SYS Messages.
dropped(<<"$SYS/", _/binary>>) ->
    ok;
dropped(_Topic) ->
    emqttd_metrics:inc('messages/dropped').

%% @doc Unsubscribe
-spec(unsubscribe(binary(), emqttd:subscriber()) -> ok).
unsubscribe(Topic, Subscriber) ->
    case ets:lookup(mqtt_subproperty, {Topic, Subscriber}) of
        [{mqtt_subproperty, _, Options}] ->
            unsubscribe(Topic, Subscriber, Options);
        [] ->
            {error, {subscription_not_found, Topic}}
    end.

-spec(unsubscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
unsubscribe(Topic, Subscriber, Options) ->
    call(pick(Subscriber), {unsubscribe, Topic, Subscriber, Options}).

-spec(async_unsubscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
async_unsubscribe(Topic, Subscriber, Options) ->
    cast(pick(Subscriber), {unsubscribe, Topic, Subscriber, Options}).

-spec(unsubscribe_client(emqttd:subscriber()) -> ok).
unsubscribe_client(Subscriber) ->
    call(pick(Subscriber), {unsubscribe_client, Subscriber}).

call(PubSub, Req) when is_pid(PubSub) ->
    gen_server2:call(PubSub, Req, infinity).

cast(PubSub, Msg) when is_pid(PubSub) ->
    gen_server2:cast(PubSub, Msg).

pick(Subscriber) ->
    gproc_pool:pick_worker(pubsub, Subscriber).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Env]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, env = Env},
     hibernate, {backoff, 2000, 2000, 20000}}.

handle_call({subscribe, Topics, Subscriber, Options}, _From, State) ->
    add_subscriber(Topics, Subscriber, Options),
    {reply, ok, setstats(State), hibernate};

handle_call({unsubscribe, Topic, Subscriber, Options}, _From, State) ->
    del_subscriber(Topic, Subscriber, Options),
    {reply, ok, setstats(State), hibernate};

handle_call({unsubscribe_client, Subscriber}, _From, State) ->
    del_subscriber(Subscriber),
    {reply, ok, setstats(State)};

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

handle_cast({subscribe, Topics, Subscriber, Options}, State) ->
    add_subscriber(Topics, Subscriber, Options),
    {noreply, setstats(State), hibernate};

handle_cast({unsubscribe, Topic, Subscriber, Options}, State) ->
    del_subscriber(Topic, Subscriber, Options),
    {noreply, setstats(State), hibernate};

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

add_subscriber(Topic, Subscriber, Options) when is_binary(Topic) ->
    add_subscriber([Topic], Subscriber, Options);
add_subscriber(Topics, Subscriber, Options) ->
    Share = proplists:get_value(share, Options),
    case ?is_local(Options) of
        false -> add_subscriber_(Share, Topics, Subscriber, Options);
        true  -> add_local_subscriber_(Share, Topics, Subscriber, Options)
    end.

add_subscriber_(Share, Topics, Subscriber, Options) ->
    lists:foreach(fun(Topic) -> not has_subscriber(Topic) andalso emqttd_router:add_route(Topic) end, Topics),
    ets:insert(mqtt_subproperty, [ #mqtt_subproperty{key = {Topic, Subscriber}, value = Options} || Topic <- Topics ]),
    ets:insert(mqtt_subscriber, [ {{Topic, shared(Share, Subscriber)}} || Topic <- Topics ]),
    ets:insert(mqtt_subscription, [ {{Subscriber, Topic}} || Topic <- Topics ]).

add_local_subscriber_(Share, Topics, Subscriber, Options) ->
    lists:foreach(fun(Topic) -> not has_subscriber({local, Topic}) andalso emqttd_router:add_local_route(Topic) end, Topics),
    ets:insert(mqtt_subproperty, [ #mqtt_subproperty{key = {Topic, Subscriber}, value = Options} || Topic <- Topics ]),
    ets:insert(mqtt_subscriber, [ {{{local, Topic}, shared(Share, Subscriber)}} || Topic <- Topics ]),
    ets:insert(mqtt_subscription, [ {{Subscriber, Topic}} || Topic <- Topics ]).

del_subscriber(Subscriber) ->
    Topics = [ Topic || [Topic] <- ets:match(mqtt_subscription, {{Subscriber, '$1'}}) ],
    ets:match_delete(mqtt_subscription, {{Subscriber, '_'}}),
    [ begin
          ets:delete(mqtt_subscriber, {Topic, Subscriber}),
          ets:delete(mqtt_subproperty, {Topic, Subscriber}),
          not has_subscriber(Topic) andalso emqttd_router:del_route(Topic)
      end || Topic <- Topics ],
    ok.

del_subscriber(Topic, Subscriber, Options) ->
    Share = proplists:get_value(share, Options),
    case ?is_local(Options) of
        false -> del_subscriber_(Share, Topic, Subscriber);
        true  -> del_local_subscriber_(Share, Topic, Subscriber)
    end.

del_subscriber_(Share, Topic, Subscriber) ->
      ets:delete(mqtt_subscriber, {Topic, shared(Share, Subscriber)}),
      ets:delete(mqtt_subscription, {Subscriber, Topic}),
      ets:delete(mqtt_subproperty, {Topic, Subscriber}),
      not has_subscriber(Topic) andalso emqttd_router:del_route(Topic).

del_local_subscriber_(Share, Topic, Subscriber) ->
    ets:delete(mqtt_subscriber, {{local, Topic}, shared(Share, Subscriber)}),
    ets:delete(mqtt_subscription, {Subscriber, Topic}),
    ets:delete(mqtt_subproperty, {Topic, Subscriber}),
    not has_subscriber({local, Topic}) andalso emqttd_router:del_local_route(Topic).

shared(undefined, Subscriber) ->
    Subscriber;
shared(Share, Subscriber) ->
    {Share, Subscriber}.

setstats(State) ->
    emqttd_stats:setstats('subscribers/count', 'subscribers/max', ets:info(mqtt_subscriber, size)),
    State.

has_subscriber(Topic) ->
    ets:match(mqtt_subscriber, {{Topic, '_'}}, 1) /= '$end_of_table'.
