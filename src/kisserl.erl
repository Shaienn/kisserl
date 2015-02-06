%% @author shaienn
%% @doc @todo Add description to kisserl.


-module(kisserl).

%% ====================================================================
%% API functions
%% ====================================================================
-export([kissdb_open/1,test/0, kissdb_close/1, kissdb_put/3, kissdb_get/2]).

-record(kissdb, {current_operation=null, hash_table_size=null, key_size=null, value_size=null, hash_table_size_bytes=null, num_hash_tables=null, version=null, hash_tables=null, hash_tables_offsets=null, file=null}).
-record(kissdb_open_param, {filepath=null, mode=null, version=null, hash_table_size = 128, key_size=null, value_size=null}).

-define(KISSDB_VERSION, 2).
-define(KISSDB_HEADER_SIZE, 18).
-define(UINT32_SIZE, 4).
%% ====================================================================
%% Internal functions
%% ====================================================================


%% General Functions  
%% --------------------------------------------------------------------
kissdb_open(Param) -> 
	{ok, KISSDB} = kissdb_open(open_file, Param).
		
kissdb_put(KISSDB, Key, Value) ->
	kissdb_put(looking_for_key, KISSDB#kissdb{current_operation = put}, Key, Value).

kissdb_get(KISSDB, Key) ->
	kissdb_get(looking_for_key, KISSDB#kissdb{current_operation = get}, Key).

kissdb_close(KISSDB) ->
	ok = file:close(KISSDB#kissdb.file).

%% Get 
%% --------------------------------------------------------------------
kissdb_get(looking_for_key, KISSDB, Key) when 
  												KISSDB#kissdb.file /= null, 
												KISSDB#kissdb.version /= null,
												KISSDB#kissdb.num_hash_tables /= null,
												KISSDB#kissdb.hash_table_size_bytes /= null,
												KISSDB#kissdb.value_size /= null,
												KISSDB#kissdb.key_size /= null, 
												KISSDB#kissdb.hash_table_size /= null ->
	HashPoint = kissdb_hash(Key, KISSDB#kissdb.key_size) rem KISSDB#kissdb.hash_table_size,
	case kissdb_check_hashtable_for_key(KISSDB, HashPoint, Key) of 
		{empty, NewHashPoint, KISSDB} -> not_found;
		{value, Offset} -> 
%% Read value from file 
				{ok, Offset} = file:position(KISSDB#kissdb.file, {bof, Offset}),
				{ok, Key_bin} = file:read(KISSDB#kissdb.file, KISSDB#kissdb.key_size),
				{ok, Value_bin} = file:read(KISSDB#kissdb.file, KISSDB#kissdb.value_size),
				{ok, Key_bin, Value_bin};
		not_found -> not_found	
	end.

%% Put 
%% --------------------------------------------------------------------
kissdb_put(looking_for_key, KISSDB, Key, Value) when 
  												KISSDB#kissdb.file /= null, 
												KISSDB#kissdb.version /= null,
												KISSDB#kissdb.num_hash_tables /= null,
												KISSDB#kissdb.hash_table_size_bytes /= null,
												KISSDB#kissdb.value_size /= null,
												KISSDB#kissdb.key_size /= null, 
												KISSDB#kissdb.hash_table_size /= null ->
	HashPoint = kissdb_hash(Key, KISSDB#kissdb.key_size) rem KISSDB#kissdb.hash_table_size,
	
	case kissdb_check_hashtable_for_key(KISSDB, HashPoint, Key) of 
		{empty, EmptyHashPoint, NewKISSDB} -> 
			io:format("EmptyHashPoint -> ~p~n", [EmptyHashPoint]),
			
			{ok, EndOffset} = file:position(NewKISSDB#kissdb.file, eof),
%% Write Key and Value into the file  			
			KeyBin = element_to_binary(Key, NewKISSDB#kissdb.key_size),
			io:format("KeyBin -> ~p~n", [KeyBin]),
			ok = file:write(NewKISSDB#kissdb.file, KeyBin),
			io:format("Value -> ~p, Size: ~p~n", [Value, NewKISSDB#kissdb.value_size]),
			ValueBin = element_to_binary(Value, NewKISSDB#kissdb.value_size),
			io:format("ValueBin -> ~p~n", [ValueBin]),
			ok = file:write(NewKISSDB#kissdb.file, ValueBin),	
			
			NewHashTables = array:set(EmptyHashPoint, EndOffset, NewKISSDB#kissdb.hash_tables),		
			HashTable = EmptyHashPoint div NewKISSDB#kissdb.hash_table_size,
			
			io:format("HashTable -> ~p~n", [HashTable]),
			HashTableOffset = array:get(HashTable, NewKISSDB#kissdb.hash_tables_offsets),
			io:format("HashTableOffset -> ~p~n", [HashTableOffset]),
			{ok, HashPointOffset} = file:position(NewKISSDB#kissdb.file, {bof, HashTableOffset + (HashPoint*?UINT32_SIZE)}),
			
			EndOffset_bin = element_to_binary(EndOffset, ?UINT32_SIZE),
			io:format("EndOffset_bin -> ~p~n", [EndOffset_bin]),
			ok = file:write(NewKISSDB#kissdb.file, EndOffset_bin),
			{ok, NewKISSDB#kissdb{hash_tables = NewHashTables}};
		{value, Offset} -> 
			{ok, ValueOffset} = file:position(KISSDB#kissdb.file, {bof, Offset + KISSDB#kissdb.key_size}),
			ValueBin = element_to_binary(Value, KISSDB#kissdb.value_size),
			ok = file:write(KISSDB#kissdb.file, ValueBin),
			{ok, KISSDB}
	end.

%% Open 
%% --------------------------------------------------------------------
kissdb_open(open_file, Param) ->
	{ok, FD} = file:open(Param#kissdb_open_param.filepath, Param#kissdb_open_param.mode),
	kissdb_open(check_header, Param, #kissdb{
											 file = FD, 
											 hash_tables = array:new([{fixed, false}, {default,0}]),
											 hash_tables_offsets = array:new([{fixed, false}, {default,0}])}).

kissdb_open(check_header, Param, KISSDB) ->
	case file:position(KISSDB#kissdb.file, eof) of
		{ok, Offset} when Offset >= ?KISSDB_HEADER_SIZE -> 
%% Probably it has header	
				kissdb_open(read_header, Param, KISSDB);
		{ok, Offset} ->
%% No header 
		 		io:format("Offset -> ~p~n", [Offset]),
				kissdb_open(create_header, Param, KISSDB);				
		{error, Reason} ->
%% No header 
		 		kissdb_open(create_header, Param, KISSDB)
	end;

kissdb_open(create_header, Param, KISSDB) -> 
%%	Set file pointer to begin of file  	
	case file:position(KISSDB#kissdb.file, bof) of
		{ok, Offset} ->
				Header_kdb = <<"KDB">>,
				Header_kissdb_ver = element_to_binary(?KISSDB_VERSION, 1),
				Version = element_to_binary(Param#kissdb_open_param.version, 2),
				HashTableSize = element_to_binary(Param#kissdb_open_param.hash_table_size, 4),
				KeySize = element_to_binary(Param#kissdb_open_param.key_size, 4),
				ValueSize = element_to_binary(Param#kissdb_open_param.value_size, 4),		
%%	So, now we create header. Write it in file. 				
				HeaderBin = <<Header_kdb/binary, 
							  Header_kissdb_ver/binary, 
							  Version/binary, 
							  HashTableSize/binary, 
							  KeySize/binary, 
							  ValueSize/binary>>,
				
				file:write(KISSDB#kissdb.file, HeaderBin),
				io:format("-> ~p~n", [HeaderBin]),
				kissdb_open(create_hash_table, Param, KISSDB#kissdb{
																		version=Param#kissdb_open_param.version, 
																		hash_table_size=Param#kissdb_open_param.hash_table_size, 
																		key_size=Param#kissdb_open_param.key_size,
																		value_size=Param#kissdb_open_param.value_size,
																		hash_table_size_bytes = 4 * (Param#kissdb_open_param.hash_table_size + 1),
																		num_hash_tables = 0  
																   });
		{error, Reason} -> {error, Reason}
	end;

kissdb_open(read_header, Param, KISSDB) ->
	case file:position(KISSDB#kissdb.file, bof) of
		{ok, Offset} ->
			case file:read(KISSDB#kissdb.file, ?KISSDB_HEADER_SIZE) of
				{ok, <<Header_kdb:3/binary,
					   Header_kissdb_ver:1/binary,
					   Version:2/little-unsigned-integer-unit:8,
					   HashTableSize:4/little-unsigned-integer-unit:8, 
					   KeySize:4/little-unsigned-integer-unit:8, 
				       ValueSize:4/little-unsigned-integer-unit:8>>
					   } ->
					kissdb_open(create_hash_table, Param, KISSDB#kissdb{
																		version=Version, 
																		hash_table_size=HashTableSize, 
																		key_size=KeySize,
																		value_size=ValueSize,
																		hash_table_size_bytes = (4 * (HashTableSize + 1)),
																		num_hash_tables = 0 
																		});
				{error, Reason} -> {error, Reason}
			end;
		{error, Reason} -> {error, Reason}
	end;

kissdb_open(create_hash_table, Param, KISSDB) -> 
	{ok, Offset} = file:position(KISSDB#kissdb.file, cur),
	NewOffsetsArray = array:set(0, Offset, KISSDB#kissdb.hash_tables_offsets),
	kissdb_open(read_and_parse_hash_table, KISSDB#kissdb{hash_tables_offsets = NewOffsetsArray}, 0);
	
kissdb_open(read_and_parse_hash_table, KISSDB, HashPoint) ->
		case file:read(KISSDB#kissdb.file, KISSDB#kissdb.hash_table_size_bytes) of 
			{ok, Data} -> 
				DataSize = erlang:byte_size(Data),
				NextTableOffset_bin = binary_part(Data, {DataSize, -?UINT32_SIZE}),
				io:format("NextTableOffset_bin: ~p~n", [NextTableOffset_bin]),
				NextTableOffset_int = binary:decode_unsigned(NextTableOffset_bin, little),		
				io:format("NextTableOffset_int: ~p~n", [NextTableOffset_int]),
				HashTable_clean = binary_part(Data, {0, DataSize - ?UINT32_SIZE}),
 				{ok, NewHashPoint, NewArray} = parse_binary_hashtable_to_array(HashTable_clean, 
																			   HashPoint, 
																			   KISSDB#kissdb.hash_tables),
				NewNumHashTables = KISSDB#kissdb.num_hash_tables + 1,
				NewOffsetsArray = array:set(NewNumHashTables, NextTableOffset_int, KISSDB#kissdb.hash_tables_offsets),
				
				if 
					NextTableOffset_int /= 0 ->
						file:position(KISSDB#kissdb.file, {bof, NextTableOffset_int}),
						kissdb_open(read_and_parse_hash_table, 
									KISSDB#kissdb{
												  hash_tables = NewArray, 
												  num_hash_tables = NewNumHashTables,
												  hash_tables_offsets = NewOffsetsArray	
												 }, 
									NewHashPoint);
					true -> 
						io:format("KISSDB tables: ~p~n", [NewNumHashTables]),
						{ok, KISSDB#kissdb{
										   hash_tables = NewArray, 
										   num_hash_tables = NewNumHashTables,
										   hash_tables_offsets = NewOffsetsArray	
										  }}
				end;
			eof -> 
%% There is no one hash table in file, create one
				{ok, Offset} = file:position(KISSDB#kissdb.file, eof), 
				 ok = kissdb_fill_file(KISSDB, KISSDB#kissdb.hash_table_size * ?UINT32_SIZE),			
				{ok, KISSDB}
	end.


%% Helpers 		
%% --------------------------------------------------------------------

kissdb_check_hashtable_for_key(KISSDB, HashPoint, Key) when 
  HashPoint =< (KISSDB#kissdb.hash_table_size * (KISSDB#kissdb.num_hash_tables+1)) ->
	io:format("HashPoint -> ~p~n", [HashPoint]),
	case array:get(HashPoint, KISSDB#kissdb.hash_tables) of 
		0 ->
%% Empty slot  		
			io:format("Empty~n"),		
			{empty, HashPoint, KISSDB};	
		Value ->
%% Busy slot, need check  		
			io:format("Value -> ~p~n", [Value]),	
			{ok, Offset} = file:position(KISSDB#kissdb.file, {bof, Value}),
			{ok, Key_from_file} = file:read(KISSDB#kissdb.file, KISSDB#kissdb.key_size),
			io:format("Key_from_file -> ~p~n", [Key_from_file]),
			Key_bin = element_to_binary(Key, KISSDB#kissdb.key_size),
			io:format("Key_bin -> ~p~n", [Key_bin]),
			if 
				Key_from_file == Key_bin -> 
%% Our key, offset to replace  					
					{value, Offset};
				true -> 
%% Switch to the next table  
					kissdb_check_hashtable_for_key(KISSDB, HashPoint + KISSDB#kissdb.hash_table_size, Key)	
			end		
	end;

kissdb_check_hashtable_for_key(KISSDB, HashPoint, Key) when KISSDB#kissdb.current_operation == get  ->
	not_found;

kissdb_check_hashtable_for_key(KISSDB, HashPoint, Key) when KISSDB#kissdb.current_operation == put  -> 
%% Create new hash table 
	io:format("New table -> ~p~n", [HashPoint]),
	{ok, EndOffset} = file:position(KISSDB#kissdb.file, eof),	
	NewNumHashTables = KISSDB#kissdb.num_hash_tables + 1,
	NewHashTableOffset = array:set(KISSDB#kissdb.num_hash_tables, EndOffset, KISSDB#kissdb.hash_tables_offsets),
	ok = kissdb_fill_file(KISSDB, KISSDB#kissdb.hash_table_size * ?UINT32_SIZE),
	if 
		KISSDB#kissdb.num_hash_tables > 0 ->
			PrevHashTableOffset = array:get(
									KISSDB#kissdb.num_hash_tables - 1, 
									KISSDB#kissdb.hash_tables_offsets
										   ),
			{ok, LinkPosOffset} = file:position(
									KISSDB#kissdb.file, 
									{bof, PrevHashTableOffset + (KISSDB#kissdb.hash_table_size * ?UINT32_SIZE)}),
			EndOffset_bin = element_to_binary(EndOffset, ?UINT32_SIZE),
			ok = file:write(KISSDB#kissdb.file, EndOffset_bin);
		true -> ok
	end,	
	{ok, EndOfLastHashTable} = file:position(KISSDB#kissdb.file, eof),	
	{empty, HashPoint, KISSDB#kissdb{
											  num_hash_tables = NewNumHashTables, 
											  hash_tables_offsets = NewHashTableOffset}}.

kissdb_fill_file(KISSDB, BytesCounter) when BytesCounter > 0 ->
	ok = file:write(KISSDB#kissdb.file, <<0>>),
	kissdb_fill_file(KISSDB, BytesCounter -1);
kissdb_fill_file(KISSDB, 0) -> ok.

parse_binary_hashtable_to_array(Data, HashPoint, Array) ->
	case Data of 
		<<Offset:?UINT32_SIZE/little-unsigned-integer-unit:8, Tail/binary>>-> 
					io:format("Off: ~p -> ~p~n", [HashPoint, Offset]),
					NewArray = array:set(HashPoint, Offset, Array),
					parse_binary_hashtable_to_array(Tail, HashPoint+1, NewArray);
		Something -> {ok, HashPoint, Array}
	end.

kissdb_hash(Value, Size) -> 
	ValueBin = element_to_binary(Value, ?UINT32_SIZE),
	kissdb_hash(binary_to_list(ValueBin), Size, 5381).

kissdb_hash([H|T], Size, Hash) when Size > 0 -> 
	NewHash = ((Hash bsl 5) + Hash) + H,
	kissdb_hash(T, Size - 1, NewHash);

kissdb_hash(T, 0, Hash) -> kissdb_check_hash(Hash);
kissdb_hash([], Size, Hash) -> kissdb_check_hash(Hash).

kissdb_check_hash(Hash) -> 
%% Cut hash value to UINT32_T size 	
	Hash_bin = element_to_binary(Hash, ?UINT32_SIZE),
	Hash_valid =  binary:decode_unsigned(Hash_bin).
		
			
element_to_binary(Element, MaxSize) when is_list(Element) ->	
	io:format("list to binary~n"),
	Element_bin = binary:list_to_bin(Element),
	Element_size = erlang:byte_size(Element_bin),
	if 
		Element_size > MaxSize ->
			binary_part(Element_bin, {Element_size, -MaxSize});
		Element_size < MaxSize ->
			io:format("elem_size: ~p, max_size: ~p~n", [Element_size, MaxSize]),
			expand_binary(Element_bin, MaxSize - Element_size);
		true -> Element_bin
	end;

element_to_binary(Element, MaxSize) when is_binary(Element) ->	
	io:format("binary to binary~n"),
	Element_size = erlang:byte_size(Element),
	if 
		Element_size > MaxSize ->
			binary_part(Element, {Element_size, -MaxSize});
		Element_size < MaxSize ->
			expand_binary(Element, MaxSize - Element_size);
		true -> Element
	end;

element_to_binary(Element, MaxSize) when is_integer(Element) ->
	Element_bin = binary:encode_unsigned(Element, big),
	Element_size = erlang:byte_size(Element_bin),
	if 
		Element_size > MaxSize ->
			binary_part(Element_bin, {Element_size, -MaxSize});
		Element_size < MaxSize ->
			<<Element:MaxSize/little-unsigned-integer-unit:8>>;
		true -> Element_bin
	end.
										
expand_binary(Binary, Step) when is_binary(Binary), Step > 0 ->
	expand_binary(<<Binary/binary, 0>>, Step - 1);

expand_binary(Binary, 0) when is_binary(Binary) -> 
	Binary.

test() -> 
	Param = #kissdb_open_param{mode = [write, read, binary], filepath="./db", version = 3, key_size = 4, value_size = 25},
	{ok, KISSDB} = kissdb_open(Param),
	io:format("KISSDB: ~p~n", [KISSDB]),
	{ok, NewKISSDB1} = kissdb_put(KISSDB, 12345, "test1"),
	{ok, NewKISSDB2} = kissdb_put(NewKISSDB1, 12345, "none2"),	
    {ok, NewKISSDB3} = kissdb_put(NewKISSDB2, 12300, "none3"),
	Res = kissdb_get(NewKISSDB3, 12301), 
	io:format("Res: ~p~n", [Res]),
	kissdb_close(NewKISSDB3).
	
	
