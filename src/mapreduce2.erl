-module(mapreduce2).
-compile(export_all).

test() ->
  Mapper = fun (_Key, Text) ->
    [{Word, 1} || Word <- Text]
           end,
  Reducer = fun (Word, Counts) ->
    [{Word, lists:sum(Counts)}]
            end,
  mapreduce(Mapper, 2, Reducer, 10, [{a, ["hello", "world", "hello", "text"]}, {b, ["world", "a", "b", "text"]}]).


mapreduce_seq(Mapper, Reducer, Input) ->
  Mapped = [{K2,V2} || {K,V} <- Input, {K2,V2} <- Mapper(K,V)],
  reduce_seq(Reducer, Mapped).

reduce_seq(Reduce,KVs) ->
  [KV || {K,Vs} <- groupkeys(lists:sort(KVs)), KV <- Reduce(K,Vs)].

%% INPUT:  [{K1, V1}, {K1, V2}, {K2, V3}]
%% OUTPUT: [{K1, [V1, V2]}, {K2, [V3]}]
groupkeys([]) ->
  [];
groupkeys([{K, V}|Rest]) ->
  groupkeys(K, [V], Rest).

groupkeys(K, Vs, [{K, V}|Rest]) ->
  groupkeys(K, [V|Vs], Rest);
groupkeys(K, Vs, Rest) ->
  [{K, lists:reverse(Vs)}|groupkeys(Rest)].

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

chunk(_, Chunks, I) when Chunks=:= I ->
  [];
chunk(Data, Chunks, I) ->
  [[Value || Chunk <- Data,
    {MapKey, MapValue} <- Chunk,
    MapKey =:= I,
    Value <- MapValue]|chunk(Data, Chunks, I + 1)].

mapreduce(Mapper, Mappers, Reducer, Reducers, Input) ->
  Self = self(),
  Ref = make_ref(),
  Partitions = partition(Mappers, Input),
  %io:format("partitions: ~p", [Partitions]),

  MapperPids = [spawn_mapper(Self, Ref, Mapper, Reducers, Part) || Part <- Partitions],
  MapperData = [receive
                  {map, {Pid, Ref, Data}} ->
                    Data
                end || Pid <- MapperPids],
  io:format("mapperdata~p", [MapperData]),

  %% Bring all data from the mappers togheter such that all keyvalue
  %% pairs with the same id is assigned to the same reducer
  Chunks = chunk(MapperData, Reducers),
  io:format("chunks: ~p", [Chunks] ),
  ReducerPids = [spawn_reducer(Self, Ref, Reducer, Chunk) || Chunk <- Chunks],
  Output = [receive
              {reduce, {Pid, Ref, Data}} ->
                Data
            end || Pid <- ReducerPids],

  %% Flatten the output from the reducers
  %% sort becouse it looks nice :-)
  lists:sort(lists:flatten(Output)).

%% INPUT: [{DataKey, [DataValue1, ..., DataValueN]}]
spawn_mapper(Master, Ref, Mapper, Reducers, Data) ->
  spawn_link(fun () ->
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

    %% Use `groupkeys` to group each tagged tupe together
    Master ! {map, {self(), Ref, groupkeys(lists:sort(Map))}}
             end).

spawn_reducer(Master, Ref, Reducer, Chunk) ->
  spawn_link(fun () ->
    %% Group the values for each key and apply
    %% The reducer to each K, value list pair
    Reduce = [KV || {K,Vs} <- groupkeys(lists:sort(Chunk)),
      KV <- Reducer(K,Vs)],
    Master ! {reduce, {self(), Ref, Reduce}}
             end).


