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

%% @doc HTTP publish API and websocket client.

-module(emqttd_http).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include_lib("kernel/include/logger.hrl").

-import(proplists, [get_value/2, get_value/3]).

-export([handle_request/1]).

handle_request(Req) ->
    {R,_} = Req,
    handle_request(R:get(method,Req), R:get(path,Req), Req).

handle_request(Method, "/status", {R,_} = Req) when Method =:= 'HEAD'; Method =:= 'GET' ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    AppStatus =
    case lists:keysearch(emqttd, 1, application:which_applications()) of
        false         -> not_running;
        {value, _Val} -> running
    end,
    Status = io_lib:format("Node ~s is ~s~nemqttd is ~s",
                            [node(), InternalStatus, AppStatus]),
    R:ok({"text/plain", iolist_to_binary(Status)}, Req);

%%--------------------------------------------------------------------
%% HTTP Publish API
%%--------------------------------------------------------------------

handle_request('POST', "/mqtt/publish", {R,_} = Req) ->
    case authorized(Req) of
        true  -> http_publish(Req);
        false -> R:respond({401, [], <<"Unauthorized">>}, Req)
    end;

%%--------------------------------------------------------------------
%% MQTT Over WebSocket
%%--------------------------------------------------------------------

handle_request('GET', "/mqtt", {R,_} = Req) ->
    ?LOG_INFO("WebSocket Connection from: ~s", [R:get(peer, Req)]),
    Upgrade = R:get_header_value("Upgrade", Req),
    Proto   = R:get_header_value("Sec-WebSocket-Protocol", Req),
    case {is_websocket(Upgrade), Proto} of
        {true, "mqtt" ++ _Vsn} ->
            emqttd_ws:handle_request(Req);
        {false, _} ->
            ?LOG_ERROR("Not WebSocket: Upgrade = ~s", [Upgrade]),
            R:respond({400, [], <<"Bad Request">>}, Req);
        {_, Proto} ->
            ?LOG_ERROR("WebSocket with error Protocol: ~s", [Proto]),
            R:respond({400, [], <<"Bad WebSocket Protocol">>}, Req)
    end;

%%--------------------------------------------------------------------
%% Get static files
%%--------------------------------------------------------------------

handle_request('GET', "/" ++ File, Req) ->
    ?LOG_INFO("HTTP GET File: ~s", [File]),
    mochiweb_request:serve_file(File, docroot(), Req);

handle_request(Method, Path, {R,_} = Req) ->
    ?LOG_ERROR("Unexpected HTTP Request: ~s ~s", [Method, Path]),
    R:not_found(Req).

%%--------------------------------------------------------------------
%% HTTP Publish
%%--------------------------------------------------------------------

http_publish({R,_} = Req) ->
    Params = mochiweb_request:parse_post(Req),
    ?LOG_INFO("HTTP Publish: ~p", [Params]),
    Topics   = topics(Params),
    ClientId = get_value("client", Params, http),
    Qos      = int(get_value("qos", Params, "0")),
    Retain   = bool(get_value("retain", Params, "0")),
    Payload  = list_to_binary(get_value("message", Params)),
    case {validate(qos, Qos), validate(topics, Topics)} of
        {true, true} ->
            lists:foreach(fun(Topic) ->
                Msg = emqttd_message:make(ClientId, Qos, Topic, Payload),
                emqttd:publish(Msg#mqtt_message{retain  = Retain})
            end, Topics),
            R:ok({"text/plain", <<"OK">>}, Req);
       {false, _} ->
            R:respond({400, [], <<"Bad QoS">>}, Req);
        {_, false} ->
            R:respond({400, [], <<"Bad Topics">>}, Req)
    end.

topics(Params) ->
    Tokens = [get_value("topic", Params) | string:tokens(get_value("topics", Params, ""), ",")],
    [list_to_binary(Token) || Token <- Tokens, Token =/= undefined].

validate(qos, Qos) ->
    (Qos >= ?QOS_0) and (Qos =< ?QOS_2);

validate(topics, [Topic|Left]) ->
    case validate(topic, Topic) of
        true  -> validate(topics, Left);
        false -> false
    end;
validate(topics, []) ->
    true;

validate(topic, Topic) ->
    emqttd_topic:validate({name, Topic}).

%%--------------------------------------------------------------------
%% basic authorization
%%--------------------------------------------------------------------

authorized({R,_} = Req) ->
    case R:get_header_value("Authorization", Req) of
    undefined ->
        false;
    "Basic " ++ BasicAuth ->
        {Username, Password} = user_passwd(BasicAuth),
        {ok, Peer} = R:get(peername, Req),
        case emqttd_access_control:auth(#mqtt_client{username = Username, peername = Peer}, Password) of
            ok ->
                true;
            {ok, _IsSuper} -> 
                true;
            {error, Reason} ->
                ?LOG_ERROR("HTTP Auth failure: username=~s, reason=~p", [Username, Reason]),
                false
        end
    end.

user_passwd(BasicAuth) ->
    list_to_tuple(binary:split(base64:decode(BasicAuth), <<":">>)). 

int(S) -> list_to_integer(S).

bool("0") -> false;
bool("1") -> true.

is_websocket(Upgrade) ->
    Upgrade =/= undefined andalso string:to_lower(Upgrade) =:= "websocket".

docroot() ->
    {file, Here} = code:is_loaded(?MODULE),
    Dir = filename:dirname(filename:dirname(Here)),
    filename:join([Dir, "priv", "www"]).

