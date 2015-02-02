%%
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


%% @author S. Kolodynski
%% @doc KDB+ interface for Erlang
%% @version 1.0

-module(qErlang).

-export [open/4,open/5,close/1,start_listener/1,tick_sub_listen/2,listen/2,handshake/2].
-export [peek/3,ipc_message/3].
-export [async/2,async_raw/2,respond/2,sync/2,tick_send/3,sync_raw/3].
-export [ipc_recv/1,ipc_recv/3,ipc_recv_raw/2].
-export [time_to_int/2,date_to_int/1,int_to_date/1,datetime_to_float/2,timestamp_to_int/3,float_to_datetime/1,int_to_time/1,int_to_timestamp/1].
-export [get_null/1].

-type qFloat() :: float() | null.

-type qObj() :: 
		  {boolean,boolean()} |
		  {boolean_list,[boolean()]} |
		  {guid,<<_:128>>} |
		  {guid,[<<_:128>>]} |
		  {byte,byte()} |
		  {byte_list,[byte()]} |
		  {short,integer()} |
		  {short_list,[integer()]} |
		  {int,integer()} |
		  {int_list,[integer()]} |
		  {long,integer()} |
		  {long_list,[integer()]} |
		  {real,qFloat()} |
		  {real_list,[qFloat()]} |
		  {float,qFloat()} |
		  {float_list,[qFloat()]} |
		  {char,char()} |
		  {char_list,[char()]} |
		  {symbol,binary()} |
		  {symbol_list,[binary()]} |
		  {timestamp,integer()} |
		  {timestamp_list,[integer()]} |
		  {month,integer()} |
		  {month_list,[integer()]} |
		  {date,integer()} |
		  {date_list,[integer()]} |
		  {datetime,qFloat()} |
		  {datetime_list,[qFloat()]} |
		  {timespan,integer()} |
		  {timespan_list,[integer()]} |
		  {minute,integer()} |
		  {minute_list,[integer()]} |
		  {second,integer()} |
		  {second_list,[integer()]} |
		  {time,integer()} |
		  {time_list,[integer()]} |
		  {mixed_list,[qObj()]} | 
		  {table,[ColNames :: binary()],[Columns :: qObj()]} |
		  {dictionary,Keys::[qObj()],Values :: [qObj()]} |
		  {function,Context :: binary(),Body :: string()} |
		  {projection,Context :: binary(),Body :: string(),Parameters :: [qObj()]} |
		  {error,string()} |
		  {null,false} |
		  {gap,false}.
		  
-type msgType() :: synchronous() | asynchronous() | response().
-type asynchronous() :: 0.
-type synchronous() :: 1.
-type response() :: 2.

-define(KB,1).
-define(UU,2).
-define(KG,4).
-define(KH,5).
-define(KI,6).
-define(KJ,7).
-define(KE,8).
-define(KF,9).
-define(KC,10).
-define(KS,11).
-define(KP,12).
-define(KM,13).
-define(KD,14).
-define(KZ,15).
-define(KN,16).
-define(KU,17).
-define(KV,18).
-define(KT,19).
-define(XT,98).
-define(XD,99).
-define(XL,100). %% lambda
-define(XN,101). %% generic null,
-define(XS,104). %% projection 
-define(QERR,128).

-define(_KB,255).
-define(_UU,254).
-define(_KG,252).
-define(_KH,251).
-define(_KI,250).
-define(_KJ,249).
-define(_KE,248).
-define(_KF,247).
-define(_KC,246).
-define(_KS,245).
-define(_KP,244).
-define(_KM,243).
-define(_KD,242).
-define(_KZ,241).
-define(_KN,240).
-define(_KU,239).
-define(_KV,238).
-define(_KT,237).

%% gregorian days up to 2000.01.01
-define(QEPOCHDAYS,730485).

%% gregorian seconds up 2000.01.01, same as calendar:datetime_to_gregorian_seconds({{2000,01,01},{0,0,0}})
-define(QEPOCHSECS,63113904000).

%% null values
-define(NF_LITTLE,<<0,0,0,0,0,0,248,255>>).
-define(NF_BIG,<<255,248,0,0,0,0,0,0>>).

%% number of bytes in 32MB. Receiving 32 MB in a raw socket is fine, 64MB gives enomem. The exact cutoff is unknown
-define(MB32,33554432).

%% integer nulls
-define(INT4_NULL,-2147483648).
-define(INT8_NULL,-9223372036854775808).

%% ====================================================================
%% API functions
%% ====================================================================


%% @doc opens a connection to the provided host
-spec open(string(),integer(),string(),string()) -> {gen_tcp:socket(),byte()} | {error,Reason} when
		  Reason :: inet:posix().

open(Host,Port,User,Password) -> open(Host,Port,User,Password,infinity). 
	

%% @doc opens a connection to the provided host, with timeout 
-spec open(string(),integer(),string(),string(),integer()) -> {gen_tcp:socket(),byte()} | {error,Reason} when
		  Reason :: inet:posix().

open(Host,Port,User,Password,Timeout) ->
	case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}],Timeout ) of
		{ok, Sock} -> 
			ok = gen_tcp:send(Sock, [list_to_binary(User), <<":">>, list_to_binary(Password),<<3,0>>]),
			case gen_tcp:recv(Sock, 1, 1000) of
				{ok,Cap} -> {Sock,Cap}; 
				{error,Reason} -> {error,{access_denied,Reason}}
			end;
		{error,Reason} -> {error,{failed_to_connect,Reason}}
	end.

%% @doc closes a connection to the provided host.
%%      This just runs gen_tcp:close() and exists only to document that this is the intended way to 
%%      close a connection to a KDB+ server.
close(Socket) -> gen_tcp:close(Socket).

%% @doc performs handshake. This supports acting as KDB+ protocol server.
-spec handshake(Socket,Credentials) -> ok | error_handshake | error_access | {error, Reason} when
			Socket :: gen_tcp:socket(), 
		  	Credentials :: sets:set(binary()), % allowed user:password combinations
			Reason :: closed | inet:posix().
	
handshake(Socket,Credentials) ->
	case gen_tcp:recv(Socket, 0, 1000) of
		{ok,Data} ->
            if 
				byte_size(Data) > 1 ->
						Tail = binary:part(Data,size(Data),-2),
						UserPass = binary:part(Data, 0, byte_size(Data)-2),
						LegalUserPass = sets:is_element(UserPass,Credentials),
						if 
							Tail == <<3,0>> andalso LegalUserPass ->
								ok = gen_tcp:send(Socket, [<<3>>]);
								ok;
							true -> error_access
						end;
				true -> error_handshake
			end;
		{error,_Reason} -> error_handshake
	end.


%% @doc spawns a listener process
-spec(start_listener(gen_tcp:socket()) -> pid()).

start_listener(Socket) -> spawn(qErlang,listen,[Socket,self()]).	

%% @doc contructs IPC message from data - should give the same as -8!Data in q. MessType: 0 - asyn, 1- sync, 2 - response
-spec ipc_message(qObj(),msgType()) -> [binary()].

ipc_message(QData,MessType) ->
	Obj = encode(QData),
	MesLen = 8 + byte_size(Obj),
	[<<1,MessType,0,0>>,<<MesLen:4/little-unsigned-integer-unit:8>>,Obj].


%% @doc contructs IPC message from data using the provided encoder - should give the same as -8!Data in q. MessType: 0 - asyn, 1- sync, 2 - response
-spec ipc_message(qObj(),msgType(),fun((qObj()) -> binary)) -> [binary()].

ipc_message(QData,MessType,Encoder) ->
	Obj = Encoder(QData),
	MesLen = 8 + byte_size(Obj),
	[<<1,MessType,0,0>>,<<MesLen:4/little-unsigned-integer-unit:8>>,Obj].


%% @doc receives and parses a message from a given socket
-spec ipc_recv(gen_tcp:socket()) -> 
		  {ok,qObj()} | {decoding_error,binary()} | {receive_error,Reason} when Reason :: closed | inet:posix().

ipc_recv(Socket) -> ipc_recv(Socket,1000).


%% @doc receive with timeout
-spec ipc_recv(gen_tcp:socket(),integer()) -> 
		  {ok,qObj()} | {decoding_error,binary()} | {receive_error,Reason} when Reason :: closed | inet:posix().

ipc_recv(Socket,Timeout) ->	
	case gen_tcp:recv(Socket, 8) of
		{ok,<<Endian,_MessType,_:2/binary,RawLength:4/binary>>} -> 
			MessLen = decode_unsigned(RawLength, Endian),
			case ipc_read(<<>>,Socket, MessLen-8, Timeout) of 
			{ok,Data} -> 
				{N,Obj} = decode(Data,Endian),
				Res = if N>0 -> {ok,Obj};
						 true -> {decoding_error,Data}
					  end,
				Res;
			{error,Reason} -> {receive_error,Reason}
			end;
		{error,Reason} -> {header_receive_error,Reason}
	end.


%% @doc receive with timeout and decoder specified. The code repeats ipc_recv/2 - maybe generalize?
-spec ipc_recv(gen_tcp:socket(),integer(),fun((binary(),0..1) -> {integer(),qObj()})) -> 
		  {ok,qObj()} | {decoding_error,binary()} | {receive_error,Reason} when Reason :: closed | inet:posix().

ipc_recv(Socket,Timeout,Decoder) ->	
	case gen_tcp:recv(Socket, 8) of
		{ok,<<Endian,_MessType,_:2/binary,RawLength:4/binary>>} -> 
			MessLen = decode_unsigned(RawLength, Endian),
			case ipc_read(<<>>,Socket, MessLen-8, Timeout) of 
			{ok,Data} -> 
				{N,Obj} = Decoder(Data,Endian),
				Res = if N>0 -> {ok,Obj};
						 true -> {decoding_error,Data}
					  end,
				Res;
			{error,Reason} -> {receive_error,Reason}
			end;
		{error,Reason} -> {header_receive_error,Reason}
	end.


%% @doc receive an ipc message without decoding
-spec ipc_recv_raw(gen_tcp:socket(),integer()) -> 
		  {ok,binary()} | {header_receive_error,Reason} | {receive_error,Reason} when Reason :: closed | inet:posix().

ipc_recv_raw(Socket,Timeout) ->
	case gen_tcp:recv(Socket, 8) of
		{ok,<<Endian,_MessType,_:2/binary,RawLength:4/binary>>} -> 
			MessLen = decode_unsigned(RawLength, Endian),
			ipc_read(<<>>,Socket, MessLen-8, Timeout);
		{error,Reason} -> {header_receive_error,Reason}
	end.


%% @doc sends an asynchronous message
-spec async(gen_tcp:socket(),qObj()) -> ok | {error, Reason} when Reason :: closed | inet:posix().

async(Socket,QData) -> gen_tcp:send(Socket, ipc_message(QData,0)).

%% @doc converts a binary encoding to the IPC message and sends it to the socket as an asynchronous message
-spec async_raw(gen_tcp:socket(),binary()) -> ok | {error, Reason} when Reason :: closed | inet:posix().
		  
async_raw(Socket,Data) ->
	Header = <<1,0,0,0,(8+size(Data)):4/little-unsigned-integer-unit:8>>, % byte-1 = 0 => asynchronous
	gen_tcp:send(Socket, [Header,Data]).	

%% @doc sends a response containing given binary encoding a q object
-spec respond(gen_tcp:socket(),binary()) -> ok | {error, Reason} when Reason :: closed | inet:posix().

respond(Socket,Data) ->
	MessLen = 8 + size(Data),
	gen_tcp:send(Socket,[<<1,2,0,0>>,<<MessLen:4/little-unsigned-integer-unit:8>>,Data]).

%% @doc sends a synchronous message
-spec sync(gen_tcp:socket(),qObj()) -> 
		  {ok,qObj()} 
		| {decoding_error,binary()} 
		| {receive_error,Reason} 
		| {send_error,SendError} when
		  Reason :: closed | inet:posix(), SendError :: {error,Reason}.
		  
sync(Socket,QData) -> 
	case gen_tcp:send(Socket, ipc_message(QData,1)) of
		ok -> ipc_recv(Socket,1000);
		Error -> {send_error,Error}
	end.

%% @doc converts a binary encoding to the IPC message and sends it synchronously to the socket. Returns the returned binary (stripped of the header) 
-spec sync_raw(gen_tcp:socket(),binary(),integer()) -> 
		  {ok,binary()} 
		| {receive_error,Reason} 
		| {send_error,SendError}
		| {header_receive_error,Reason} 
		| {receive_error,Reason}
	when 
		Reason :: closed | inet:posix(), SendError :: {error,Reason}.

sync_raw(Socket,Data,Timeout) ->
	Header = <<1,1,0,0,(8+size(Data)):4/little-unsigned-integer-unit:8>>,
	case gen_tcp:send(Socket, [Header,Data]) of
		ok -> ipc_recv_raw(Socket,Timeout);
		Error -> {send_error,Error}
	end.
		 
%% @doc sends data to tick
-spec tick_send(gen_tcp:socket(),binary(),qObj()) -> ok | {error, Reason} when 
		  Reason :: closed | inet:posix().

tick_send(Socket,Tab,Data) ->	gen_tcp:send(Socket, ipc_message({mixed_list,[{symbol,<<".u.upd">>},{symbol,Tab},{mixed_list,Data}]},1)).


%% @doc subscribes for all symbols for given table and starts listener, returns the model and PID of the listener
-spec tick_sub_listen(gen_tcp:socket(),binary()) -> 
		  {ok,pid(),{table,qObj()}}
		| {ok,{error,{error,string()}}}
		| {decoding_error,binary()}
		| {send_error,SendError}
		| {receive_error,Reason} 
		when Reason :: closed | inet:posix(), SendError :: {error,Reason}.

tick_sub_listen(Socket,Table) ->
	case sync(Socket,{symbol_list,[<<".u.sub">>,Table,<<>>]}) of
		{ok,{mixed_list,Data}} -> 
			Pid = start_listener(Socket),
			{ok,Pid,{table,Data}};
		{ok,{error,Qerror}} -> {error,{error,Qerror}};
		{Error,Reason} -> {Error,Reason}
	end.

%% @doc obtains null of the given type
-spec get_null(atom()) -> binary().
get_null(guid) -> <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>;
get_null(long) -> <<X:8/big-signed-integer-unit:8>> = <<128,0, 0, 0, 0, 0, 0, 0>>, X;
get_null(int) -> <<X:4/big-signed-integer-unit:8>> = <<128,0,0,0>>, X;
get_null(boolean) -> false; 
get_null(byte) -> <<0>>;
get_null(short) -> -32768;
get_null(real) -> null;
get_null(float) -> null;
get_null(char) -> 32;
get_null(symbol) -> <<>>;
get_null(timestamp) -> ?INT8_NULL;
get_null(month) -> ?INT4_NULL;
get_null(date) -> ?INT4_NULL;
get_null(datetime) -> null;
get_null(timespan) -> ?INT8_NULL;
get_null(minute) -> ?INT4_NULL;
get_null(second) -> ?INT4_NULL;
get_null(time) -> ?INT4_NULL.


%% date time utilities

%% @doc takes year,month, day and converts to integer that can be used to build a q date
-spec date_to_int(calendar:date()) -> integer().

date_to_int(Date) -> calendar:date_to_gregorian_days(Date) - ?QEPOCHDAYS.


%% @doc reverse to date_to_int - takes integer from q date and converts to Erlang date
-spec int_to_date(integer()) -> calendar:date().

int_to_date(Days) -> calendar:gregorian_days_to_date(Days+?QEPOCHDAYS).


%% @doc takes a an Erlang date and time	and converts to a float representing q datetime
-spec datetime_to_float(calendar:datetime(),integer()) -> float().

datetime_to_float({Date,{Hour,Minute,Second}},Millisecond) ->
	date_to_int(Date) + (Hour + (Minute + (Second+Millisecond/1000)/60)/60)/24.


%% @doc converts float in q representation to Erlang's datetime plus milliseconds
-spec float_to_datetime(float()) -> {calendar:datetime(),integer()}.

float_to_datetime(Qdt) ->
	Days = trunc(Qdt),
	Date = int_to_date(Days),
	Time = Qdt - Days,
	TimeHours = 24*Time,
	Hour = trunc(TimeHours),
	TimeMinutes = 60*(TimeHours - Hour),
	Minute = trunc(TimeMinutes),
	TimeSeconds = 60*(TimeMinutes - Minute),
	Second = trunc(TimeSeconds),
	TimeMilliseconds = 1000*(TimeSeconds - Second),
	{{Date,{Hour,Minute,Second}},round(TimeMilliseconds)}.


%% @doc takes Erlang time and converts to an integer representing q time (milliseconds since midnight)
-spec time_to_int(calendar:time(),integer()) -> integer().

time_to_int({Hour,Minute,Second},Millisecond) -> ((Hour*60 +Minute)*60+Second)*1000 + Millisecond.


%% @doc takes an integer representing q time and converts to calendar:time()
-spec int_to_time(integer()) -> calendar:time().

int_to_time(Qt) -> 
	Hour = Qt div 3600000,
	RemHour = Qt rem 3600000,
	Minute = RemHour div 60000,
	RemMinute = RemHour rem 60000,
	Second = RemMinute div 1000,
	Millisecond = RemMinute rem 1000,
	{{Hour,Minute,Second},Millisecond}.
	

%% @doc converts a timestamp to integer representing q timestamp
-spec timestamp_to_int(DateTime,Millisecond,Nanosecond) -> integer() when
		  	DateTime :: calendar:datetime(),
			Millisecond :: 0..999,
			Nanosecond :: 0..999999999.

timestamp_to_int({Date,Time},Millisecond,Nanosecond) ->
	(calendar:datetime_to_gregorian_seconds({Date,Time}) - ?QEPOCHSECS)*1000000000 + Millisecond*1000000 + Nanosecond.


%% @doc takes an integer representing timestamp and converts to calendar:time plus milliseconds and nanoseconds
-spec int_to_timestamp(integer()) -> {calendar:datetime(),integer(),integer()}.

int_to_timestamp(Timestamp) ->
	DateTime = calendar:gregorian_seconds_to_datetime((Timestamp div 1000000000) + ?QEPOCHSECS),
	RemSeconds = Timestamp rem 1000000000, %% time remaining after subtracting full seconds
	Millisecond = RemSeconds div 1000000,
	Nanosecond = RemSeconds rem 1000000,
	{DateTime,Millisecond,Nanosecond}.
	

%% ====================================================================
%% Internal functions
%% ====================================================================
%% @doc receives data, works around the restriction on the packet size when option is {packet,raw}
ipc_read(Bin,Socket,Size,Timeout) when Size < ?MB32 -> 
	case gen_tcp:recv(Socket,Size,Timeout) of
		{ok,Data} -> {ok,<<Bin/binary,Data/binary>>};
		Error -> Error
	end;

ipc_read(Bin,Socket,Size,Timeout) ->
	case gen_tcp:recv(Socket,?MB32,Timeout) of
		{ok,Data} -> ipc_read(<<Bin/binary,Data/binary>>,Socket,Size-?MB32,Timeout);
		Error -> Error
	end.

%% @doc listens on specified socket and sends decoded messages to given Pid
-spec listen(gen_tcp:socket(),pid()) -> {}.
listen(Socket,Pid) ->
	case ipc_recv(Socket) of
		{ok,Message} -> 
			Pid ! {self(),{ok,Message}},
			listen(Socket,Pid);
		 Error ->  
			 Pid ! {self(),Error}
	end.
							  
%% atom and simple lists encoders
encode({boolean,Data}) -> << ?_KB, (bool_byte(Data))/binary>>;
encode({boolean_list,Data}) -> << ?KB, 0, (length(Data)):4/little-signed-integer-unit:8, << <<(bool_byte(X))/binary>> || X <- Data >>/binary >>;

encode({guid,Data}) -> << ?_UU,Data/binary>>;
encode({guid_list,Data}) -> << ?UU, 0, (length(Data)):4/little-signed-integer-unit:8,<< <<X/binary>> || X <- Data >>/binary >>;

%% byte and byte list expect binaries
encode({byte,Data}) -> <<?_KG,Data:1/binary>>;
encode({byte_list,Data}) -> << ?KG, 0, (byte_size(Data)):4/little-signed-integer-unit:8, Data/binary >>;

encode({short,Data}) -> <<?_KH,Data:2/little-signed-integer-unit:8>>;
encode({short_list,Data}) -> << ?KH, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:2/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({int,Data}) -> <<?_KI,Data:4/little-signed-integer-unit:8>>;
encode({int_list,Data}) -> << ?KI, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:4/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({long,Data}) -> <<?_KJ,Data:8/little-signed-integer-unit:8>>;
encode({long_list,Data}) -> << ?KJ, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:8/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({real,null}) -> <<?_KE,0,0,192,255>>;
encode({real,Data}) -> <<?_KE,Data:4/little-signed-float-unit:8>>;
encode({real_list,Data}) -> << ?KE, 0, (length(Data)):4/little-signed-integer-unit:8, << <<(real_to_bin(X))/binary>> || X <- Data >>/binary >>;

encode({float,null}) -> <<?_KF,0,0,0,0,0,0,248,255>>;
encode({float,Data}) -> <<?_KF,Data:8/little-signed-float-unit:8>>;
encode({float_list,Data}) -> << ?KF, 0, (length(Data)):4/little-signed-integer-unit:8, << <<(float_to_bin(X))/binary>> || X <- Data >>/binary >>;

encode({char,Data}) -> << ?_KC, Data >>;
encode({char_list,Data}) -> << ?KC, 0, (length(Data)):4/little-signed-integer-unit:8, (list_to_binary(Data))/binary >>;

encode({symbol,Data}) -> << ?_KS, (Data)/binary,0 >>;
encode({symbol_list,Data}) -> << ?KS, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X/binary,0>> || X <- Data >>/binary >>;

encode({timestamp,Data}) -> <<?_KP,Data:8/little-signed-integer-unit:8>>;
encode({timestamp_list,Data}) -> << ?KP, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:8/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({month,Data}) -> <<?_KM,Data:4/little-signed-integer-unit:8>>;
encode({month_list,Data}) -> << ?KM, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:4/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({date,Data}) -> <<?_KD,Data:4/little-signed-integer-unit:8>>;
encode({date_list,Data}) -> << ?KD, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:4/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({datetime,null}) -> <<?_KZ,0,0,0,0,0,0,248,255>>;
encode({datetime,Data}) -> <<?_KZ,Data:8/little-signed-float-unit:8>>;
encode({datetime_list,Data}) -> << ?KZ, 0, (length(Data)):4/little-signed-integer-unit:8, << <<(float_to_bin(X))/binary>> || X <- Data >>/binary >>;

encode({timespan,Data}) -> <<?_KN,Data:8/little-signed-integer-unit:8>>;
encode({timespan_list,Data}) -> << ?KN, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:8/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({minute,Data}) -> <<?_KU,Data:4/little-signed-integer-unit:8>>;
encode({minute_list,Data}) -> << ?KU, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:4/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({second,Data}) -> <<?_KV,Data:4/little-signed-integer-unit:8>>;
encode({second_list,Data}) -> << ?KV, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:4/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

encode({time,Data}) -> <<?_KT,Data:4/little-signed-integer-unit:8>>;
encode({time_list,Data}) -> << ?KT, 0, (length(Data)):4/little-signed-integer-unit:8, << <<X:4/little-signed-integer-unit:8>> || X <- Data >>/binary >>;

%% encode a mixed list
encode({mixed_list,Data}) -> <<0, 0, (length(Data)):4/little-signed-integer-unit:8, << <<(encode(X))/binary>> || X<-Data >>/binary >>;

%% encode a table, example: {table, [<<"a">>,<<"b">>], [{int_list,[1,2,3]},{long_list,[4,5,6]}] }
encode({table,ColNames,Columns}) ->
	<<?XT, 0, ?XD, (encode({symbol_list,ColNames}))/binary, (encode({mixed_list,Columns}))/binary >>;

%% encode a dictionary, example {dictionary,{symbol_list,[<<"a">>,<<"b">>,<<"c">>]},{long_list,[1,2,3]}}
encode({dictionary,Keys,Values}) ->
	<<?XD,(encode(Keys))/binary,(encode(Values))/binary>>;

%% encode a function (lambda). Note: Context is a binary, a bit inconsistent as usually we use strings (lists of integers) to represent q symbols
encode({function,Context,Body}) ->
	case check_body(Body) of 
		ok -> <<?XL,Context/binary,0,(encode({char_list,Body}))/binary>>;
		_  -> erlang:error(bad_function_body)
	end;

%% encode a projection (function with partially applied parameters. {gap, false} indicates a gap in parameters
encode({projection,Context,Body,ParameterList}) ->
	case check_body(Body) of
		ok -> ParListLen = 1+length(ParameterList),
			  Function = encode({function,Context,Body}),
			  Parameters = << << (encode(Par))/binary>> || Par <- ParameterList >>,
			  <<?XS,(ParListLen):4/little-signed-integer-unit:8,(Function)/binary,(Parameters)/binary >>;
		_ ->  erlang:error(bad_function_body)
	end;

%% encode q error
encode({error,Message}) -> <<?QERR,(binary:list_to_bin(Message))/binary,0>>; 

%% encode generic null 
encode({null,false}) -> <<?XN,0>>;

%% encode gap - used for projections
encode({gap,false}) -> <<?XN,255>>.

%% @doc decodes first N elements of a mixed list and returns a list of resulting q objects
- spec peek(binary(),Depth,0..1) -> {ok,[qObj()]} | {error,decoding_error} | {error,badmatch} when Depth :: integer().

peek(<<0,_,LL:4/little-signed-integer-unit:8,Data/binary>>,N,1) when N =< LL ->
	{Consumed,Objs} = decode(Data,N,1),
	if 
		0 < Consumed -> {ok,Objs};
		true -> {error,decoding_error}
	end;

peek(_Data,_N,1) ->  {error,badmatch}. % this happens when it's not a mixed list or when N > LL

%% parses N objects from a binary and returns pair {NumberOfBytesConsumed,Objects} TODO: try to rewrite using tail recursion
decode(_Data,0,_Endian) -> {0,[]};
decode(Data,N,Endian) ->
	{ConsumedH,H} = decode(Data,Endian),
	{ConsumedT,T} = decode(binary:part(Data,ConsumedH,byte_size(Data)-ConsumedH),N-1,Endian),
	{ConsumedH+ConsumedT,[H|T]}.

	
%% mixed list
decode(<<0,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) ->
	{Consumed,ObjL} = decode(Data,LL,1),
	{6+Consumed,{mixed_list,ObjL}};


%% decoders return a pair {bytes consumed,object}, where object is a pair {type, data}
decode(<<?_KB,0,_/binary>>,_) -> {2,{boolean,false}};

decode(<<?_KB,1,_/binary>>,_) -> {2,{boolean,true}};

decode(<<?KB,_,L:4/binary,Data/binary>>,Endian) ->
	LL = bin_int(L,Endian),
	{6+LL,{boolean_list,[ byte_bool(X) ||  <<X:1/binary>> <= binary:part(Data,0,LL)]}};


%% TODO: check if q transmits guids differently in little and big endian
decode(<<?_UU,Data:16/binary,_/binary>>,_) -> {17,{guid,Data}};
decode(<<?UU,_,L:4/binary,Data/binary>>,Endian) ->
	LL = bin_int(L,Endian),
	{6+LL*16,{guid_list,[ X ||  <<X:16/binary>> <= binary:part(Data,0,LL*16)]}};

decode(<<?_KG,Data,_/binary>>,_Endian) -> {2,{byte,<<Data>>}};
decode(<<?KG,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL,{byte_list,binary:part(Data,0,LL)}};

decode(<<?_KH,Data:2/binary,_/binary>>,Endian) -> {3,{short,decode_signed(Data,2,Endian)}};
decode(<<?KH,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) ->
	{6+LL*2,{short_list,[ X ||  <<X:2/little-signed-integer-unit:8>> <=  binary:part(Data,0,LL*2) ]}};


decode(<<?_KI,Data:4/binary,_/binary>>,Endian) -> {5,{int,decode_signed(Data,4,Endian)}};
decode(<<?KI,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) ->
	{6+LL*4,{int_list,[ X ||  <<X:4/little-signed-integer-unit:8>> <=  binary:part(Data,0,LL*4) ]}};

decode(<<?_KJ,Data:8/binary,_/binary>>,Endian) -> {9,{long,decode_signed(Data,8,Endian)}};
decode(<<?KJ,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*8,{long_list,[ X || <<X:8/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*8)]}};

decode(<<?_KE,0,0,192,255,_/binary>>,1) -> {5,{real,null}}; 
decode(<<?_KE,Data:4/binary,_/binary>>,1) -> <<R:4/little-signed-float-unit:8>> = Data, {5,{real,R}};
decode(<<?KE,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*4,{real_list,[ bin_to_real_little(X) || <<X:4/binary>> <= Data ]}};


decode(<<?_KF,0,0,0,0,0,0,248,255,_/binary>>,1) -> {9,{float,null}};
decode(<<?_KF,Data:8/binary,_/binary>>,1) -> <<R:8/little-signed-float-unit:8>> = Data, {9,{float,R}};
decode(<<?KF,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*8,{float_list,[ bin_to_float_little(X) || <<X:8/binary>> <= binary:part(Data,0,LL*8) ]}};

decode(<<?_KC,Data:1/binary,_/binary>>,_Endian) -> {2,{char,binary:decode_unsigned(Data)}};

decode(<<?KC,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL,{char_list,binary:bin_to_list(binary:part(Data,0,LL))}};

decode(<<?_KS,Data/binary>>,_Endian) ->
	[Chopped|_] = binary:split(Data,<<0>>), %% get until the first null in Data, null is not included 
	{byte_size(Chopped)+2,{symbol,Chopped}};

decode(<<?KS,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) ->
	Split = splitN(Data,LL),
	{6 + sum_len(Split) + LL,{symbol_list,Split}};

decode(<<?_KP,Data:8/binary,_/binary>>,Endian) -> {9,{timestamp,decode_signed(Data,8,Endian)}};

decode(<<?KP,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*8,{timestamp_list,[ X ||  <<X:8/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*8) ]}};

decode(<<?_KM,Data:4/binary,_/binary>>,Endian) -> {5,{month,decode_signed(Data,4,Endian)}};

decode(<<?KM,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*4,{month_list,[ X || <<X:4/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};

decode(<<?_KD,Data:4/binary,_/binary>>,Endian) -> {5,{date,decode_signed(Data,4,Endian)}};

decode(<<?KD,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*4,{date_list,[ X || <<X:4/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};

decode(<<?_KZ,0,0,0,0,0,0,248,255,_/binary>>,1) -> {9,{datetime,null}};
decode(<<?_KZ,Data:8/binary,_/binary>>,1) -> <<R:8/little-signed-float-unit:8>> = Data, {9,{datetime,R}};
decode(<<?KZ,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*8,{datetime_list,[ bin_to_float_little(X) || <<X:8/binary>> <= binary:part(Data,0,LL*8) ]}};


decode(<<?_KN,Data:8/binary,_/binary>>,Endian) -> {9,{timespan,decode_signed(Data,8,Endian)}};
decode(<<?KN,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*8,{timespan_list,[ X || <<X:8/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*8) ]}};

decode(<<?_KU,Data:4/binary,_/binary>>,Endian) -> {5,{minute,decode_signed(Data,4,Endian)}};

decode(<<?KU,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*4,{minute_list,[ X || <<X:4/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};


decode(<<?_KV,Data:4/binary,_/binary>>,Endian) -> {5,{second,decode_signed(Data,4,Endian)}};

decode(<<?KV,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*4,{second_list,[ X || <<X:4/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};


decode(<<?_KT,Data:4/binary,_/binary>>,Endian) -> {5,{time,decode_signed(Data,4,Endian)}};

decode(<<?KT,_,LL:4/little-signed-integer-unit:8,Data/binary>>,1) -> 
	{6+LL*4,{time_list,[ X ||  <<X:4/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};

%% table 
decode(<<?XT,_,?XD,Data/binary>>,1) ->
	{N,{symbol_list,ColNames}} = decode(Data,1), %% extract column names
	{K,{mixed_list,Columns}} = decode(binary:part(Data, N, byte_size(Data)-N),1),
	{3+N+K,{table,ColNames,Columns}};

decode(<<?XT,_,?XD,Data/binary>>,0) ->
	{N,{symbol_list,ColNames}} = decode(Data,1), %% extract column names
	{K,{mixed_list,Columns}} = decode(binary:part(Data, N, byte_size(Data)-N),0),
	{3+N+K,{table,ColNames,Columns}};


%% dictionary
decode(<<?XD,Data/binary>>,1) ->
	{N,Keys} = decode(Data,1), %% extract column names
	{K,Values} = decode(binary:part(Data, N, byte_size(Data)-N),1),
	{1+N+K,{dictionary,Keys,Values}};

%% function
decode(<<?XL,Data/binary>>,Endian) ->
	[Context|[Rest]] = binary:split(Data, <<0>>),
	ContextSize = 1+byte_size(Context), %% context is null terminated
	{BodySize,{char_list,Body}} = decode(Rest,Endian),
	{1+ContextSize+BodySize,{function,Context,Body}};

%% projection
decode(<<?XS,NumPars:4/little-signed-integer-unit:8,Data/binary>>,1) ->
	{Fbytes,{function,Context,Body}} = decode(Data,1),
	{ParListBytes,ParList} = decode(binary:part(Data,Fbytes,byte_size(Data)-Fbytes),NumPars-1,1), %% the actual number of parameters is one less
	{5+Fbytes+ParListBytes,{projection,Context,Body,ParList}};

%% generic null
decode(<<?XN,0,_Data/binary>>,1) -> {2,{null,false}};

%% gap - used to encode projections
decode(<<?XN,255,_Data/binary>>,1) -> {2,{gap,false}};

%% error
decode(<<?QERR,Data/binary>>,_Endian) ->
	[Err|_] = binary:split(Data, <<0>>), %% extract null-terminated error string
	{2+byte_size(Err),{error,binary:bin_to_list(Err)}};
	
%% catch all - failure, return binary
decode(Bin,1) -> {0,Bin};


%% big endian decoders

decode(<<0,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) ->
	{Consumed,ObjL} = decode(Data,LL,0),
	{6+Consumed,{mixed_list,ObjL}};

decode(<<?KG,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL,{byte_list,binary:part(Data,0,LL)}};

decode(<<?KH,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) ->
	{6+LL*2,{short_list,[ X ||  <<X:2/big-signed-integer-unit:8>> <=  binary:part(Data,0,LL*2) ]}};

decode(<<?KI,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) ->
	{6+LL*4,{int_list,[ X ||  <<X:4/big-signed-integer-unit:8>> <=  binary:part(Data,0,LL*4) ]}};
	
decode(<<?KJ,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*8,{long_list,[ X || <<X:8/big-signed-integer-unit:8>> <= binary:part(Data,0,LL*8)]}};

decode(<<?_KE,255,192,0,0,_/binary>>,0) -> {5,{real,null}};
decode(<<?_KE,Data:4/binary,_/binary>>,0) -> <<R:4/big-signed-float-unit:8>> = Data, {5,{real,R}};

decode(<<?KE,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*4,{real_list,[ X || <<X:4/big-signed-float-unit:8>> <= Data ]}};

decode(<<?_KF,Data:8/binary,_/binary>>,0) -> <<R:8/big-signed-float-unit:8>> = Data, {9,{float,R}};

decode(<<?KF,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*8,{float_list,[ X ||  <<X:8/little-signed-float-unit:8>> <= binary:part(Data,0,LL*8) ]}};
	
decode(<<?KC,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL,{char_list,binary:bin_to_list(binary:part(Data,0,LL))}};
	
decode(<<?KS,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) ->
	Split = splitN(Data,LL),
	{6 + sum_len(Split) + LL,{symbol_list,Split}};
	
decode(<<?KP,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*8,{timestamp_list,[ X ||  <<X:8/little-signed-integer-unit:8>> <= binary:part(Data,0,LL*8) ]}};
	
decode(<<?KM,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*4,{month_list,[ X || <<X:4/big-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};
	
decode(<<?KD,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*4,{date_list,[ X || <<X:4/big-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};
	
decode(<<?KZ,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*8,{datetime_list,[ X || <<X:8/big-signed-float-unit:8>> <= binary:part(Data,0,LL*8) ]}};
	
decode(<<?KN,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*8,{timespan_list,[ X || <<X:8/big-signed-integer-unit:8>> <= binary:part(Data,0,LL*8) ]}};
	
decode(<<?KU,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*4,{minute_list,[ X || <<X:4/big-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};

decode(<<?KV,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*4,{second_list,[ X || <<X:4/big-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};
	
decode(<<?KT,_,LL:4/big-signed-integer-unit:8,Data/binary>>,0) -> 
	{6+LL*4,{time_list,[ X ||  <<X:4/big-signed-integer-unit:8>> <= binary:part(Data,0,LL*4) ]}};
	
decode(<<?_KZ,Data:8/binary>>,0) -> <<R:8/big-signed-float-unit:8>> = Data, {9,{datetime,R}};

decode(Bin,0) -> {0,Bin}.
 

%% like binary:decode_unsigned, but endianess given by int
decode_unsigned(Subject,1) -> binary:decode_unsigned(Subject, little);
decode_unsigned(Subject,0) -> binary:decode_unsigned(Subject, big).


%% missing decode signed, limited to n bytes
decode_signed(Subject,N,1) -> <<Res:N/little-integer-signed-unit:8>> = Subject, Res;
decode_signed(Subject,N,0) -> <<Res:N/big-integer-signed-unit:8>> = Subject, Res.


%% checks if a function body is correct. We require that a function body enclosed in {}.
check_body(Body) when length(Body) < 2 -> error;
check_body(Body) ->
	Last = lists:last(Body),
	if (hd(Body) =:= ${) and (Last =:= $}) -> ok;
	   true -> error
	end.

%% converts true/false atom to corresponding byte
bool_byte(false) -> <<0>>;
bool_byte(true) -> <<1>>.

%% converts byte to corresponding true/false atom
byte_bool(<<0>>) -> false;
byte_bool(<<1>>) -> true.

%% converts 8 byte binary to either float or atom null
bin_to_float_little(<<0,0,0,0,0,0,248,255>>) -> null;
bin_to_float_little(<<X:8/little-signed-float-unit:8>>) -> X.

%% converts a floating point or null to corresponding 8 byte binary. Only little endian is supported as this is used in encoding only
float_to_bin(null) -> <<0,0,0,0,0,0,248,255>>;
float_to_bin(X) -> <<X:8/little-signed-float-unit:8>>.

%% converts 4 byte binary to either float or atom null
bin_to_real_little(<<0,0,192,255>>) -> null;
bin_to_real_little(<<X:4/little-signed-float-unit:8>>) -> X.

%% converts 4 byte floating point or null to corresponding binary
real_to_bin(null) -> <<0,0,192,255>>;
real_to_bin(X) -> <<X:4/little-signed-float-unit:8>>.

%% interprets a 4 byte binary as integer in given endian
bin_int(<<R:4/little-signed-integer-unit:8>>,1) -> R;
bin_int(<<R:4/big-signed-integer-unit:8>>,1) -> R.

splitN(Bin,N) -> 
	Sbin = splitN([],N,Bin),
	lists:reverse(Sbin).

%% returns the first N occurences separated by null bytes in a binary - tail recursive version
-spec splitN(Acc,N,Bin) -> [binary()] when
		Acc :: [binary()],
		N :: integer(),
		Bin :: binary().
	
splitN(Sbin,0,_Bin) -> Sbin;

splitN(Sbin,N,Bin) ->
	[H|[T]] = binary:split(Bin, <<0>>),
	splitN([H|Sbin],N-1,T).

%% returns sum of lengths of a list of binaries
sum_len(L) -> sum_len(L,0).

sum_len([H|T],Sum) ->sum_len(T,Sum + byte_size(H));
sum_len([],Sum) -> Sum.