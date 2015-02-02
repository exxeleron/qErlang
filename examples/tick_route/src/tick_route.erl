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

-module(tick_route).
-behaviour(gen_server).

% interface calls
-export([stop/0, start_link/3]).

% gen_server callbacks
-export([init/1,
         handle_call/3, 
         handle_cast/2,
         handle_info/2, 
         terminate/2, 
         code_change/3]).

-record(state, {outSock1,outSock2,inSocket}). % the current socket

start_link(OutSock1,OutSock2,InSocket) ->
	gen_server:start_link(?MODULE, {OutSock1,OutSock2,InSocket}, []).

stop() ->
    io:format("Stopping~n"),
    gen_server:cast(?MODULE, shutdown).

init({OutSock1,OutSock2,InSocket}) ->
    % io:format("Initializing listener with socket ~p~n",[Socket]),
	gen_server:cast(self(), accept),
    process_flag(trap_exit, true),
    {ok, #state{outSock1=OutSock1,outSock2=OutSock2,inSocket=InSocket}}.

handle_call(_Message, _From, State) -> 
    {reply, ok, State}.

handle_cast(shutdown, State) ->
	gen_tcp:close(State#state.outSock1),
    gen_tcp:close(State#state.outSock2),
	gen_tcp:close(State#state.inSocket),
    {stop, normal, State};

handle_cast(session, State) ->
	ClientSocket = State#state.inSocket,
	TradeSocket = State#state.outSock1,
	QuoteSocket = State#state.outSock2,
	% io:format("waiting for message~n"),
	{ok,Data} = qErlang:ipc_recv_raw(ClientSocket,0),
	case qErlang:peek(Data,2,1) of
		{ok,[{symbol,<<"upd">>},{symbol,<<"trade">>}]} ->
			ok = qErlang:async_raw(TradeSocket,Data);
		{ok,[{symbol,<<"upd">>},{symbol,<<"quote">>}]} ->
			ok = qErlang:async_raw(QuoteSocket,Data);
		_ -> io:format("unsupported message~n")
		end,
	gen_server:cast(self(), session),
	{noreply, State};

handle_cast(accept, S = #state{inSocket=ListenSocket}) ->
	io:format("listening... ~n"),
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    inet:setopts(AcceptSocket, [{packet, raw}]),
	io:format("accepted connection~n"),
    case qErlang:handshake(AcceptSocket,sets:from_list([<<"u:p">>])) of
        ok -> 
            io:format("handshake successful~n"),
            gen_server:cast(self(), session);
        _  ->
            io:format("handshake failed~n"),
            gen_server:cast(self(), shutdown)
        end,
	
    {noreply, S#state{inSocket=AcceptSocket}};

%% generic async handler
handle_cast(Message, State) ->
    io:format("Generic cast handler: '~p' ",[Message]),
    {noreply, State}.

handle_info(_Message, _Server) -> 
    io:format("Generic info handler: '~p' '~p'~n",[_Message, _Server]),
    {noreply, _Server}.
		
terminate(_Reason, _Server) -> ok.

%% Code change
code_change(_OldVersion, _Server, _Extra) -> {ok, _Server}. 

