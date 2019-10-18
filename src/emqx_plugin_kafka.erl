%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugin_kafka).

-include_lib("emqx/include/emqx.hrl").

-export([ load/1
        , unload/0
        ]).

%% Hooks functions
-export([on_message_publish/2]).

%% Called when the plugin application start
load(Env) ->   
    ekaf_init([Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
%%    io:format("Publish ~s~n", [emqx_message:format(Message)]),   
    io:format("Publish ~s~n", [emqx_message:payload(Message)]), 

    Id = emqx_message:id(Message),
    Qos = emqx_message:qos(Message),
    From = emqx_message:from(Message),
    Topic = emqx_message:topic(Message),
    Payload = emqx_message:payload(Message),
    Timestamp = emqx_message:timestamp(Message),

     Json = [
        {type, <<"published">>},
        {id, Id},
        {from, From},
        {topic, Topic},
        {payload, Payload},
        {qos, Qos},
        {timestamp, Timestamp}
       ],
    ekaf:produce_async(<<"emq_broker_message">>, jsx:encode(Json)),


 %%      aa =     jsx:encode([{<<"library">>,<<"jsx">>},{<<"awesome">>,true}]),



 %%    ekaf:produce_async(<<"emq_broker_message">>, Payload),

 %%     io:format("Test Json ~s~n", aa), 
                 
 %%   io:format("Publish Json ~s~n", jsx:encode(Json)), 

    {ok, Message}.


ekaf_init(_Env) ->
    {ok, Values} = application:get_env(emqx_plugin_kafka, values),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values),
    PartitionStrategy= proplists:get_value(partition_strategy, Values),
    application:load(ekaf),
    application:set_env(ekaf, ekaf_partition_strategy, PartitionStrategy),
    application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),
    {ok, _} = application:ensure_all_started(ekaf),
    io:format("Initialized ekaf with ~p~n", [BootstrapBroker]).

%% Called when the plugin application stop
unload() ->   
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).










