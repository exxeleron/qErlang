%% Copyright (c) 2011-2015 Exxeleron GmbH
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

-module(tick_route_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
	% open connections to ticks
	case open(5001,5002) of
		{error,Reason} -> {error,Reason};
		{ok,{OutSock1,OutSock2}} ->
			{ok, ListenSocket} = gen_tcp:listen(8000, [{active,false}, {packet,raw}, {mode, binary}]),
    		{ok, { {one_for_one, 20, 30}, [{router,{tick_route,start_link,[OutSock1,OutSock2,ListenSocket]},permanent,1000,worker,[tick_route]}]}}
	end.

%----------------- private functions -------------------------
 
%% opens connection to q servers listening on given ports
open(Port1,Port2) -> 
	io:format("connecting to server on port ~p~n",[Port1]),
	case qErlang:open("localhost",Port1,"","",1000) of
		{error,Reason} -> {error,Reason};
		{Socket1,_Byte1} ->
			io:format("connected to server at port ~p~n",[Port1]),
			io:format("connecting to server on port ~p~n",[Port2]),
			case qErlang:open("localhost",Port2,"","",1000) of
				{error,Reason} -> {error,Reason};
				{Socket2,_Byte2} ->
					io:format("connected to server at port ~p~n",[Port2]),
					{ok,{Socket1,Socket2}}
			end
		end.


