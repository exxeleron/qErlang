%% Copyright (c) 2011-2014 Exxeleron GmbH
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% @author Slawomir Kolodynski
%% @doc an example gen_server subscribing to a tick instance and printing the updates on the console

-module(tick_sub).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

start_link(Port) ->
    gen_server:start_link(?MODULE, Port, []).

init(Port) ->
    io:fwrite("initializing qListener, port ~p~n",[Port]),
    case qErlang:open("localhost",Port,"","",1000) of
      {error,Reason} -> {error,Reason};
      {Socket,_Byte} -> 
        io:fwrite("connected to server at port ~p~n",[Port]),
        %% subscribe for the trade table, all symbols
        case qErlang:sync(Socket,{symbol_list,[<<".u.sub">>,<<"trade">>,<<>>]}) of 
          {ok,{mixed_list,[{symbol,<<"trade">>},Model]}} ->
            io:fwrite("subscribed for table 'trade' with model ~p~n",[Model]),
            gen_server:cast(self(),session),
            {ok, Socket};
          Error -> {stop,Error}
        end
    end.

%% callbacks
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(session,Socket) ->
  listen(Socket), %% this will exit only on error
  {stop,"tick down", Socket};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% other functions
listen(Socket) ->
  case qErlang:ipc_recv(Socket) of
    {ok,Message} -> 
      io:fwrite("received update ~p~n",[Message]),
      listen(Socket);
    Error -> Error
  end.
    