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
-export([publish/1]).

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

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
