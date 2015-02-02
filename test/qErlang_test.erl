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
%% @doc Tests qErlang functionality
%% run from console with 'eunit:test(qErlang_test)' or from command line with 'rebar eunit'.

-module(qErlang_test).

-include_lib("eunit/include/eunit.hrl").

-ifdef(TEST).

%% ====================================================================
%% API functions
%% ====================================================================
-export([hex_to_bin/1]).


%% ====================================================================
%% Internal functions
%% ====================================================================

open() -> 
	case qErlang:open("localhost", 5001, "u", "p") of 
		{error,Reason} -> {error,Reason};
		{Socket,_Cap} -> {ok,Socket}
	end.

%% converts a hex string to a binary. hex_to_bin(<<"0102FF">>) -> <<0,1,255>>
hex_to_bin(Bin) -> << <<(h2b(X))/binary>> || <<X:2/binary>> <= Bin >>.

%% two bytes binary denoting hex code to corresponding byte
h2b(<<B1,B0>>) -> 
	Res = (h2num(B0)+ 16*h2num(B1)),
	<<Res>>.

h2num($A) -> 10;
h2num($a) -> 10;
h2num($B) -> 11;
h2num($b) -> 11;
h2num($C) -> 12;
h2num($c) -> 12;
h2num($D) -> 13;
h2num($d) -> 13;
h2num($E) -> 14;
h2num($e) -> 14;
h2num($F) -> 15;
h2num($f) -> 15;
h2num(N) -> N-48.


%% tests a single query
test_expr(Socket,{Query,Expected}) ->
	{ok,Res} = qErlang:sync(Socket, {char_list,Query}),
	?assertEqual({Query,Expected},{Query,Res}),
	case Expected of
		{error,_} -> ?assertEqual(1,1);
		_ ->?assertEqual({Query,{ok,Expected}},{Query,qErlang:sync(Socket,{mixed_list,[{symbol,<<"echo">>},Expected]})})
	end.
	

check0(Socket) ->
	Tests = [{"1+`",{error,"type"}}
			,{"()",{mixed_list,[]}}
			,{"::",{null,false}}
			,{"1",{long,1}}
			,{"1i",{int,1}}
			,{"-234h",{short,-234}}
			,{"(234h;235h)",{short_list,[234,235]}}
			,{"0xFF",{byte,<<255>>}}
			,{"0xFFFE",{byte_list,<<255,254>>}}
			,{"1b",{boolean,true}}
			,{"0x2a",{byte,<<16#2a>>}}
			,{"89421099511627575j",{long,89421099511627575}}
			,{"3.234",{float,3.234}}
			,{"5.5e",{real,5.5}}
			,{"\"0\"",{char,$0}}
			,{"\"abc\"",{char_list,"abc"}}
			,{"\"\"",{char_list,[]}}
			,{"\"quick brown fox jumps over a lazy dog\"",{char_list,"quick brown fox jumps over a lazy dog"}}
			,{"`abc",{symbol,<<"abc">>}}
			,{"`quickbrownfoxjumpsoveralazydog",{symbol,<<"quickbrownfoxjumpsoveralazydog">>}}
			,{"2000.01.04D05:36:57.600",{timestamp,279417600000000}}
			,{"2001.01m",{month,12}}
			,{"2001.01.01",{date,366}}
			,{"2000.05.01",{date,121}}
			,{"2000.01.04T05:36:57.600",{datetime,3.234}}
			,{"0D05:36:57.600",{timespan,20217600000000}}
			,{"12:01",{minute,721}}
			,{"12:05:00",{second,43500}}
			,{"12:04:59.123",{time,43499123}}
			,{"0b",{boolean,qErlang:get_null(boolean)}}
			,{"0x00",{byte,qErlang:get_null(byte)}}
			,{"0Nh",{short,qErlang:get_null(short)}}
			,{"0N",{long,qErlang:get_null(long)}}
			,{"0n",{float,qErlang:get_null(float)}}
			,{"0Ne",{real,qErlang:get_null(real)}}
			,{"\" \"",{char,qErlang:get_null(char)}}
			,{"`",{symbol,qErlang:get_null(symbol)}}
			,{"0Np",{timestamp,qErlang:get_null(timestamp)}}
			,{"0Nm",{month,qErlang:get_null(month)}}
			,{"0Nd",{date,qErlang:get_null(date)}}
			,{"0Nz",{datetime,qErlang:get_null(datetime)}}
			,{"0Nn",{timespan,qErlang:get_null(timespan)}}
			,{"0Nu",{minute,qErlang:get_null(minute)}}
			,{"0Nv",{second,qErlang:get_null(second)}}
			,{"0Nt",{time,qErlang:get_null(time)}}
			,{"(0b;1b;0b)",{boolean_list,[false,true,false]}}
			,{"(0x01;0x02;0xff)",{byte_list,<<1,2,255>>}}
			,{"(1h;2h;3h)",{short_list,[1,2,3]}}
			,{"1 2 3",{long_list,[1,2,3]}}
			,{"(1i;2i;3i)",{int_list,[1,2,3]}}
			,{"(1j;2j;3j)",{long_list,[1,2,3]}}
			,{"(5.5e; 8.5e)",{real_list,[5.5,8.5]}}
			,{"3.23 6.46",{float_list,[3.23,6.46]}}
			,{"(1;`bcd;\"0bc\";5.5e)",{mixed_list,[{long,1},{symbol,<<"bcd">>},{char_list,"0bc"},{real,5.5}]}}
			,{"(42;::;`foo)",{mixed_list,[{long,42},{null,false},{symbol,<<"foo">>}]}}
			,{"(enlist 1h; 2; enlist 3j)",{mixed_list,[{short_list,[1]},{long,2},{long_list,[3]}]}}
			,{"`the`quick`brown`fox",{symbol_list,[<<"the">>,<<"quick">>,<<"brown">>,<<"fox">>]}}
			,{"``quick``fox",{symbol_list,[<<>>,<<"quick">>,<<>>,<<"fox">>]}}
			,{"``",{symbol_list,[<<>>,<<>>]}}
			,{"(\"quick\"; \"brown\"; \"fox\"; \"jumps\"; \"over\"; \"a lazy\"; \"dog\")",{mixed_list,[{char_list,"quick"},{char_list,"brown"},{char_list,"fox"},{char_list,"jumps"},{char_list,"over"},{char_list,"a lazy"},{char_list,"dog"}]}}
			,{"2000.01.04D05:36:57.600 0Np",{timestamp_list,[279417600000000,qErlang:get_null(timestamp)]}}
			,{"(2001.01m; 0Nm)",{month_list,[12,qErlang:get_null(month)]}}
			,{"2001.01.01 2000.05.01 0Nd",{date_list,[366,121,qErlang:get_null(date)]}}
			,{"2000.01.04T05:36:57.600 0Nz",{datetime_list,[3.234,qErlang:get_null(datetime)]}}
			,{"0D05:36:57.600 0Nn",{timespan_list,[20217600000000,qErlang:get_null(timestamp)]}}
			,{"12:01 0Nu",{minute_list,[721,qErlang:get_null(minute)]}}
			,{"12:05:00 0Nv",{second_list,[43500,qErlang:get_null(second)]}}
			,{"12:04:59.123 0Nt",{time_list,[43499123,qErlang:get_null(time)]}}
			,{"(enlist `a)!(enlist 1)",{dictionary,{symbol_list,[<<"a">>]},{long_list,[1]}}}
			,{"1 2!`abc`cdefgh",{dictionary,{long_list,[1,2]},{symbol_list,[<<"abc">>,<<"cdefgh">>]}}}
			,{"(`x`y!(`a;2))",{dictionary,{symbol_list,[<<"x">>,<<"y">>]},{mixed_list,[{symbol,<<"a">>},{long,2}]}}}
			,{"`abc`def`gh!([] one: 1 2 3; two: 4 5 6)",{dictionary,{symbol_list,[<<"abc">>,<<"def">>,<<"gh">>]},{table,[<<"one">>,<<"two">>],[{long_list,[1,2,3]},{long_list,[4,5,6]}]}}}
			,{"(1;2h;3.3;\"4\")!(`one;2 3;\"456\";(7;8 9))",{dictionary,{mixed_list,[{long,1},{short,2},{float,3.3},{char,$4}]},{mixed_list,[{symbol,<<"one">>},{long_list,[2,3]},{char_list,"456"},{mixed_list,[{long,7},{long_list,[8,9]}]}]}}}
			,{"(0 1; 2 3)!`first`second",{dictionary,{mixed_list,[{long_list,[0,1]},{long_list,[2,3]}]},{symbol_list,[<<"first">>,<<"second">>]}}}
			,{"`A`B`C!((1;2.2;3);(`x`y!(`a;2));5.5)",{dictionary,{symbol_list,[<<"A">>,<<"B">>,<<"C">>]},{mixed_list,[{mixed_list,[{long,1},{float,2.2},{long,3}]},{dictionary,{symbol_list,[<<"x">>,<<"y">>]},{mixed_list,[{symbol,<<"a">>},{long,2}]}},{float,5.5}]}}}
			,{"flip `abc`def!(1 2 3; 4 5 6)",{table,[<<"abc">>,<<"def">>],[{long_list,[1,2,3]},{long_list,[4,5,6]}]}}
			,{"flip `name`iq!(`Dent`Beeblebrox`Prefect;98 42 126)",{table,[<<"name">>,<<"iq">>],[{symbol_list,[<<"Dent">>,<<"Beeblebrox">>,<<"Prefect">>]},{long_list,[98,42,126]}]}}
			,{"flip `name`iq`grade!(`Dent`Beeblebrox`Prefect;98 42 126;\"a c\")",{table,[<<"name">>,<<"iq">>,<<"grade">>],[{symbol_list,[<<"Dent">>,<<"Beeblebrox">>,<<"Prefect">>]},{long_list,[98,42,126]},{char_list,"a c"}]}}
			,{"flip `name`iq`fullname!(`Dent`Beeblebrox`Prefect;98 42 126;(\"Arthur Dent\"; \"Zaphod Beeblebrox\"; \"Ford Prefect\"))",
			  {table,[<<"name">>,<<"iq">>,<<"fullname">>],[{symbol_list,[<<"Dent">>,<<"Beeblebrox">>,<<"Prefect">>]},{long_list,[98,42,126]},{mixed_list,[{char_list,"Arthur Dent"},{char_list,"Zaphod Beeblebrox"},{char_list,"Ford Prefect"}]}]}}
			,{"([] name:`symbol$(); iq:`int$())",{table,[<<"name">>,<<"iq">>],[{symbol_list,[]},{int_list,[]}]}}
			,{"([] pos:`d1`d2`d3;dates:(2001.01.01;2000.05.01;0Nd))",
			  {table,[<<"pos">>,<<"dates">>],[{symbol_list,[<<"d1">>,<<"d2">>,<<"d3">>]},{date_list,[qErlang:date_to_int({2001,1,1}),qErlang:date_to_int({2000,5,1}),qErlang:get_null(date)]}]}}
			,{"([eid:1001 1002 1003] pos:`d1`d2`d3;dates:(2001.01.01;2000.05.01;0Nd))",
			  {dictionary,
			   {table,[<<"eid">>],[{long_list,[1001,1002,1003]}]},
			   {table,[<<"pos">>,<<"dates">>],[{symbol_list,[<<"d1">>,<<"d2">>,<<"d3">>]},{date_list,[qErlang:date_to_int({2001,1,1}),qErlang:date_to_int({2000,5,1}),qErlang:get_null(date)]}]}}}
			,{"{x+y}",{function,<<>>,"{x+y}"}}
			,{"{x+y}[3]",{projection,<<>>,"{x+y}",[{long,3}]}}
			,{"{x+y+z}[3;;2]",{projection,<<>>,"{x+y+z}",[{long,3},{gap,false},{long,2}]}}
			,{"\"G\"$\"8c680a01-5a49-5aab-5a65-d4bfddb6a661\"",{guid,hex_to_bin(<<"8c680a015a495aab5a65d4bfddb6a661">>)}}
			,{"(\"G\"$\"8c680a01-5a49-5aab-5a65-d4bfddb6a661\"; 0Ng)",{guid_list,[hex_to_bin(<<"8c680a015a495aab5a65d4bfddb6a661">>),qErlang:get_null(guid)]}}
			,{"2014.03.15T10:15:23.250",{datetime,qErlang:datetime_to_float({{2014,03,15},{10,15,23}},250)}}
			,{"2014.03.15T10:15:23.250p",{timestamp,qErlang:timestamp_to_int({{2014,03,15},{10,15,23}},250,0)}}
			,{"10:15:23.250",{time,qErlang:time_to_int({10,15,23},250)}}
			],
	
	qErlang:sync(Socket, {char_list,"echo:{x}"}),
	[test_expr(Socket,T) || T <- Tests].

check1(Socket) ->
	Res = qErlang:sync_raw(Socket,hex_to_bin(<<"0A0003000000322B32">>),1000),
	?assertEqual({ok,hex_to_bin(<<"F90400000000000000">>)}, Res).

encdec_test() ->
  case open() of
    {ok,Socket} -> 
      check0(Socket),
      check1(Socket),
      qErlang:close(Socket);
    {error,Reason} -> 
      ?debugFmt("failed to connect to KDB+ server at port 5001: ~p~n",[Reason]),
      ?debugMsg("test requires a KDB+ server running on localhost:5001"),
      %% just to make sure the test does not pass in this case
      ?assertEqual("connected to q server running on localhost:5001","not connected")
    end.
      
-endif.