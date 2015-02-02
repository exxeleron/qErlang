tick_route
==========

The purpose of this example is to show how the `qErlang:peek` function can be used for routing KDB+ IPC messages based on their contents without full decoding of the whole message.
In the example the `tick_route` application opens connections to two KDB+ servers listening on ports 5001 and 5002, resp., then starts to listen on port 8000. Every incoming message is partially decoded. Messages that encode a mixed list with the first two elements equal to `upd` and `trade` symbols are forwarded to a KDB+ server listening on port 5001 while messages with the first two elements equal to `upd` and `quote` symbols are forwarded to a KDB+ server listening on port 5002. This way a stream of updates coming to the `tick_route` application is split into two streams routed to two tick instances. 

The main functionality is implemented in the `handle_cast(session,State)` function in the `tick_route` module. The code is quoted below for easy reference:

```erlang
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
```

