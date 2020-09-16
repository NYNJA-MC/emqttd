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

%% @doc PubSub Supervisor.
-module(emqttd_pubsub_sup).

-author("Feng Lee <feng@emqtt.io>").

-behaviour(supervisor).

%% API
-export([start_link/0, pubsub_pool/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CONCURRENCY_OPTS, [{read_concurrency, true}, {write_concurrency, true}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

pubsub_pool() ->
    hd([Pid || {pubsub_pool, Pid, _, _} <- supervisor:which_children(?MODULE)]).

%%--------------------------------------------------------------------
%% Supervisor Callbacks
%%--------------------------------------------------------------------

init([]) ->
    EtsOpts = [{keypos, 2}, named_table, public],
    ets:new(mqtt_subscriber,   [bag|EtsOpts]),
    ets:new(mqtt_subscription, [ordered_set|EtsOpts]),
    ets:new(mqtt_subproperty,  [set|EtsOpts]),
    {ok, Env} = emqttd:env(pubsub),
    {ok, { {one_for_all, 10, 3600}, [pool_sup(pubsub, Env)]} }.

%%--------------------------------------------------------------------
%% Pool
%%--------------------------------------------------------------------

pool_size(Env) ->
    Schedulers = erlang:system_info(schedulers),
    proplists:get_value(pool_size, Env, Schedulers).

pool_sup(Name, Env) ->
    Pool = list_to_atom(atom_to_list(Name) ++ "_pool"),
    Mod = list_to_atom("emqttd_" ++ atom_to_list(Name)),
    MFA = {Mod, start_link, [Env]},
    emqttd_pool_sup:spec(Pool, [Name, hash, pool_size(Env), MFA]).
