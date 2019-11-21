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
    %% ekaf_init([Env]),
    brod_init([Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
%%    io:format("Publish ~s~n", [emqx_message:format(Message)]),   
    io:format("Publish ~s~n", [emqx_message:payload(Message)]), 
    

%%    io:format("String Id ~s~n",[lists:flatten(io_lib:format("~p", [Id]))]), 
%%   io:format("Base62 Id ~s~n", [emqx_guid:to_base62(Id)]), 
%%   io:format("Hexstr Id ~s~n", [emqx_guid:to_hexstr(Id)]), 


   %% 构建json
    Id = emqx_message:id(Message),
    KafkaJson = [
        {type, <<"publish">>},
        {id, emqx_guid:to_hexstr(Id)},
        {from, emqx_message:from(Message)},
        {topic, emqx_message:topic(Message)},
        {payload, emqx_message:payload(Message)},
        {qos, emqx_message:qos(Message)},
        {timestamp, emqx_message:timestamp(Message)}
       ],
    %% 从配置文件中读取发送到的kafka主题
    {ok, Values} = application:get_env(emqx_plugin_kafka, values),
    KafkaTopic = proplists:get_value(kafka_producer_topic, Values),
   
    %% 使用ekaf发送到kafka
    %%ekaf:produce_async(KafkaTopic, jsx:encode(KafkaJson)), 

    %% 使用brod发送到kafka
    PartitionFun = fun(_Topic, PartitionsCount, _Key, _Value) ->
                   {ok, crypto:rand_uniform(0, PartitionsCount)}
                   end,
    ok = brod:produce_sync(brod_client_1, KafkaTopic, PartitionFun, <<"key2">>, jsx:encode(KafkaJson)),

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



%% 初始化brod https://github.com/klarna/brod
brod_init(_Env) ->
    {ok, _} = application:ensure_all_started(brod), 
    {ok, Values} = application:get_env(emqx_plugin_kafka, values),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values),  
    KafkaTopic = proplists:get_value(kafka_producer_topic, Values),
    ClientConfig = [],%% socket error recovery
    ok = brod:start_client(BootstrapBroker, brod_client_1, ClientConfig),
    ok = brod:start_producer(brod_client_1, KafkaTopic, _ProducerConfig = []),   
    io:format("Init brod with ~p~n", [BootstrapBroker]).

%% 关闭brod
brod_close() ->
    {ok, Values} = application:get_env(emqx_plugin_kafka, values),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values), 
    io:format("Close brod with ~p~n", [BootstrapBroker]),
    brod:stop_client(brod_client_1).


%% Called when the plugin application stop
unload() ->   
    brod_close(),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).
