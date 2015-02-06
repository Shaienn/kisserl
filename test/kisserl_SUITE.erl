%% @author shaienn
%% @doc @todo Add description to list_to_path_SUITE.


-module(kisserl_SUITE).
-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(demo_packet, {sulsid=null, operation_code=null, balance=null, is_blocked=0}).
-record(kissdb_open_param, {filepath=null, mode=null, version=null, sub_version=null, hash_table_size = 128, key_size=null, value_size=null}).

all() ->
  [
    kisserl_test
  ].

suite() ->
    [{timetrap,{seconds,30}}].

init_per_suite(Config) ->
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, Config) ->
  Config.

end_per_suite(Config) ->
  Config.
	
kisserl_test(Config) ->
	Param = #kissdb_open_param{mode = [write, read, binary], filepath="./db2", version = 3, sub_version = 2, key_size = 4, value_size = 25},
  	io:format("Config: ~p~n", [Config]),
  	{ok, KISSDB} = kisserl:kissdb_open(Param),
	{ok, KISSDB2} = kisserl:kissdb_put(KISSDB, 12345, <<"test1">>),
	{ok, KISSDB3} = kisserl:kissdb_put(KISSDB2, 11111, "test2"),
	{ok, Key1_bin, Value1_bin} = kisserl:kissdb_get(KISSDB3, 12345),
	?assertEqual(<<"test1">>, Value1_bin),
	{ok, Key2_bin, Value2_bin} = kisserl:kissdb_get(KISSDB3, 11111),
	?assertEqual(<<"test2">>, Value2_bin),
	ok = kisserl:kissdb_close(KISSDB).
	
	



	
	


