-module(mapreduce).
%%-compile().

-export([test/0, mapreduce_seq/3, spawnsender/3, testwithNodes/0]).

test() ->
  Mapper = fun(_Key, Text) ->
    [{Word, 1} || Word <- Text]
           end,
  Reducer = fun(Word, Counts) ->
    [{Word, lists:sum(Counts)}]
            end,
  mapreduce(Mapper, 2, Reducer, 10, [{a, ["hello", "world", "a", "hello", "text"]}, {b, ["world", "a", "a", "b", "text"]}]).

testwithNodes() ->
  HostName = inet:gethostname(),
  Nodes = [secondNode@Baltazar, thirdNode@Baltazar],
  io:format("Starting Nodes: ~p\n",[Nodes]),
  Mapper = fun(_Key, Text) ->
    [{Word, 1} || Word <- Text]
           end,
  Reducer = fun(Word, Counts) ->
    [{Word, lists:sum(Counts)}]
            end,
  mapreduce(Nodes, Mapper, 2, Reducer, 10, [{a, ["hello", "world", "a", "hello", "text"]}, {b, ["world", "a", "a", "b", "text"]}]).


mapreduce_seq(Mapper, Reducer, Input) ->
  Mapped = [{K2, V2} || {K, V} <- Input, {K2, V2} <- Mapper(K, V)],
  reduce_seq(Reducer, Mapped).

reduce_seq(Reduce, KVs) ->
  [KV || {K, Vs} <- groupkeys(lists:sort(KVs)), KV <- Reduce(K, Vs)].

%% INPUT:  [{K1, V1}, {K1, V2}, {K2, V3}]
%% OUTPUT: [{K1, [V1, V2]}, {K2, [V3]}]
groupkeys([]) ->
  [];
groupkeys([{K, V} | Rest]) ->
  groupkeys(K, [V], Rest).

groupkeys(K, Vs, [{K, V} | Rest]) ->
  groupkeys(K, [V | Vs], Rest);
groupkeys(K, Vs, Rest) ->
  [{K, lists:reverse(Vs)} | groupkeys(Rest)].

%% INPUT: [a,b,c,d], 2
%% OUTPUT: [[a,b], [c,d]]
%% INPUT: [a, b], 2
%% OUTPUT: [[],[], [a], [b]]
partition(N, L) ->
  partition(N, L, length(L)).

partition(1, L, _) ->
  [L];
partition(N, L, Len) ->
  {Prefix, Suffix} = lists:split(Len div N, L),
  [Prefix | partition(N - 1, Suffix, Len - (Len div N))].

%% Partition tagged chunks to the same list
%%
%% INPUT: [[{1, [{K1, V1}, {K2, V2}]}, {2, [{K3, V1}]}], [{2, [{K3, V10}]}]]
%% OUTPUT: [[{K1, V1}, {K2, V2}], [{K3,V1}, {K3, V10}]]
chunk(Data, Chunks) ->
  chunk(Data, Chunks + 1, 1).

chunk(_, Chunks, I) when Chunks =:= I ->
  [];
chunk(Data, Chunks, I) ->
  [[Value || Chunk <- Data,
    {MapKey, MapValue} <- Chunk,
    MapKey =:= I,
    Value <- MapValue] | chunk(Data, Chunks, I + 1)].

sendStartToReduce({I, Pid}) ->
  io:format("send start to Pid: ~p\n", [Pid]),
  Pid ! startreducing.

mapreduce(Mapper, Mappers, Reducer, Reducers, Input) ->
  Self = self(),
  io:format("I'm the master of the universe: ~p \n", [Self]),
  Ref = make_ref(),
  io:format("Master REf: ~p\n", [Ref]),
  Partitions = partition(Mappers, Input),
  io:format("Partisions: ~p slut egen \n", [Partitions]),
  ReducerPidsList = [{I, spawn_reducer(Self, Ref, Reducer)} || I <- lists:seq(1, Reducers)],
  ReducerPids = maps:from_list(ReducerPidsList),
  io:format("ReducerPid: ~p \n", [ReducerPids]),

  MapperPids = [spawn_mapper(Self, Ref, Mapper, Reducers, Part, ReducerPids) || Part <- Partitions],
  io:format("Waiting for Mappers"),
  [receive
     {ready, {Pid, Ref}} ->
       {ready, Pid}
   end || Pid <- MapperPids],

  io:format("Start to send start to ~p\n", [ReducerPidsList]),
  [sendStartToReduce(ReducerPid) || ReducerPid <- ReducerPidsList],

  %%io:format("MapperData: ~p\n", [MapperData]),
  %% Bring all data from the mappers togheter such that all keyvalue
  %% pairs with the same id is assigned to the same reducer
  %%Chunks = chunk(MapperData, Reducers),
  %%io:format("Chunks: ~p\n", [Chunks]),
  %%ReducerPids = [spawn_reducer(Self, Ref, Reducer, Chunk) || Chunk <- Chunks],
  io:format("Waiting for output \n"),
  Output = [receive
%%              X ->
%%                io:format("Recived output ~p\n",[X]), notok;
              {reduce, Pid, Ref, Data} ->
                io:format("Recived output ~p\n", [Data]),
                Data
            end || {I, Pid} <- ReducerPidsList],

  %% Flatten the output from the reducers
  %% sort becouse it looks nice :-)
  lists:sort(lists:flatten(Output)).

mapreduce(Nodes, Mapper, Mappers, Reducer, Reducers, Input) ->
  Self = self(),
  io:format("I'm the master of the universe: ~p \n", [Self]),
  Ref = make_ref(),
  io:format("Master REf: ~p\n", [Ref]),
  Partitions = partition(Mappers, Input),
  io:format("Partisions: ~p slut egen \n", [Partitions]),
  ReducerPidsList = spawn_reducers(Nodes, Self, Ref, Reducer, Reducers, 1),
  %%[{I, spawn_reducerNodes(Nodes, Self, Ref, Reducer)} || I <- lists:seq(1, Reducers)],
  ReducerPids = maps:from_list(ReducerPidsList),
  io:format("ReducerPids: ~p \n", [ReducerPids]),

  MapperPids = [spawn_mapper(Self, Ref, Mapper, Reducers, Part, ReducerPids) || Part <- Partitions],
  io:format("Waiting for Mappers"),
  [receive
     {ready, {Pid, Ref}} ->
       {ready, Pid}
   end || Pid <- MapperPids],

  io:format("Start to send start to ~p\n", [ReducerPidsList]),
  [sendStartToReduce(ReducerPid) || ReducerPid <- ReducerPidsList],

  %%io:format("MapperData: ~p\n", [MapperData]),
  %% Bring all data from the mappers togheter such that all keyvalue
  %% pairs with the same id is assigned to the same reducer
  %%Chunks = chunk(MapperData, Reducers),
  %%io:format("Chunks: ~p\n", [Chunks]),
  %%ReducerPids = [spawn_reducer(Self, Ref, Reducer, Chunk) || Chunk <- Chunks],
  io:format("Waiting for output \n"),
  Output = [receive
%%              X ->
%%                io:format("Recived output ~p\n",[X]), notok;
              {reduce, Pid, Ref, Data} ->
                io:format("Recived output ~p\n", [Data]),
                Data
            end || {I, Pid} <- ReducerPidsList],

  %% Flatten the output from the reducers
  %% sort becouse it looks nice :-)
  lists:sort(lists:flatten(Output)).

spawnsender([], _ReducerPids, _Index) ->
  ok;

spawnsender(MapperChunks, ReducerPids, Index) ->
  [H | T] = MapperChunks,
  io:format("Mapperchunk Head: ~p : ~p : ~p \n", [H, maps:get(Index, ReducerPids), Index]),
  maps:get(Index, ReducerPids) ! H,
  spawnsender(T, ReducerPids, Index + 1).

%% INPUT: [{DataKey, [DataValue1, ..., DataValueN]}]
spawn_mapper(Master, Ref, Mapper, Reducers, Data, ReducerPids) ->
  spawn_link(fun() ->
    %% phash: hash erlang term to `Reducers` bins.
    %% In this case we tag each value with the same
    %% value so that they will be processed by the
    %% same reducer.
    Map = [{erlang:phash(MapKey, Reducers), {MapKey, MapValue}} ||
      %% For each element in Data
      {DataKey, DataValue} <- Data,
      %% Apply the Mapper to the key and value
      %% and iterate over those
      {MapKey, MapValue} <- Mapper(DataKey, DataValue)],
    io:format("MAP: ~p \n", [Map]),
    MappperChunks = chunk([groupkeys(lists:sort(Map))], Reducers),
    io:format("MapperChunks ~p \n", [MappperChunks]),
    spawn_link(?MODULE, spawnsender, [MappperChunks, ReducerPids, 1]),
    %% Use `groupkeys` to group each tagged tupe together

    Master ! {ready, {self(), Ref}}
             end).
reducing(Master, Ref, Reducer, Chunks) ->
  receive
    startreducing ->
      Reduce = [KV || {K, Vs} <- groupkeys(lists:sort(Chunks)),
        KV <- Reducer(K, Vs)],
      io:format("Send to Master from: ~p Reduce: ~p\n", [self(), Reduce]),
      Master ! {reduce, self(), Ref, Reduce};
    Chunk ->
      reducing(Master, Ref, Reducer, Chunk ++ Chunks)
  end.

spawn_reducer(Master, Ref, Reducer) ->

  spawn_link(fun() ->
    %% Group the values for each key and apply
    %% The reducer to each K, value list pair
    io:format("ReducerSpawned PID: ~p \n", [self()]),
    io:format("Start reducing ~p\n", [self()]),
    reducing(Master, Ref, Reducer, [])

             end).

spawn_reducers(_Nodes, _Master, _Ref, _Reducer, 0, _I) ->
  io:format("Spawn Complete!\n"),
  [];

spawn_reducers(Nodes, Master, Ref, Reducer, Reducers, I) ->
  [H|T] = Nodes,
  io:format("Nodes to spawn: ~p\n",[Nodes]),
  io:format("Spawn Reduser Node: ~p \n" ,[H]),
  List = [{I, spawn_reducerNodes(H, Master, Ref, Reducer,
    fun(Master, Ref, Reducer, Chunks) -> Fun = fun(F) -> F(F) end, Fun(Fun, Master, Ref, Reducer, Chunks) end) }] ++ spawn_reducers(T ++ [H], Master, Ref, Reducer, Reducers -1, I + 1),
  io:format("ReducerLIst: ~p\n",[List]),
  List.



spawn_reducerNodes(Node, Master, Ref, Reducer, F) ->

  spawn_link(Node, fun() ->
    %% Group the values for each key and apply
    %% The reducer to each K, value list pair
    io:format("ReducerSpawned PID: ~p \n", [self()]),
    io:format("Start reducing ~p\n", [self()]),
    F(Master, Ref, Reducer, [])

                   end).