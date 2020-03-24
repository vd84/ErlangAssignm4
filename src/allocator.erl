%%%-------------------------------------------------------------------
%%% @author dogge
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. mars 2020 11:07
%%%-------------------------------------------------------------------
-module(allocator).
-author("dogge").

%% API
-export([start/1, request/2, release/2, test2/0, allocate_test/3]).

start(Resources) ->
  spawn_link(fun () ->
    allocator(Resources)
             end).

request(Pid, RequestedResource) ->
  Ref = make_ref(),
  Pid ! {request, {self(), Ref, RequestedResource}},
  receive
    {granted, Ref, Granted} ->
      Granted
  end.

release(Pid, ReleasedResources) ->
  Ref = make_ref(),
  Pid ! {release, {self(), Ref, ReleasedResources}},
  receive
    {released, Ref} ->
      ok
  end.

takeResources(AvailableResources, []) ->
  AvailableResources;

takeResources(AvailableResources, [H|T]) ->
  case maps:is_key(H, AvailableResources) of
    true ->
      takeResources(maps:remove(H,AvailableResources), T);
    false ->
      timer:sleep(5000),
      takeResources(AvailableResources, [H|T])
  end.


allocator(Resources) ->
  receive
  %%{request, {Pid, Ref, N}} when N =< length(Resources) ->
    {request, {Pid, Ref, RequestedResources}} ->
      RemainingResources = takeResources(Resources,RequestedResources),
      Pid ! {granted, Ref, RequestedResources},
      allocator(RemainingResources);

%%      {G, R} = lists:split(N, Resources),
%%      Pid ! {granted, Ref, G},
%%      allocator(R);
    {release, {Pid, Ref, ReleasedResources}} ->
      Pid ! {released, Ref},
      allocator(maps:merge(ReleasedResources, Resources))
  %%allocator(Released ++ Resources)
  end.

test2() ->
  Allocator = allocator:start([a,b,c,d]),
  spawn(?MODULE, allocate_test, [Allocator, "Process A", 2]),
  spawn(?MODULE, allocate_test, [Allocator, "Process B", 2]),
  spawn(?MODULE, allocate_test, [Allocator, "Process C", 4]).

allocate_test(Allocator, Name, N) ->
  io:format("~p requests ~p resources ~n", [Name, N]),
  S = allocator:request(Allocator, N),
  receive
  after 2000 ->
    ok
  end,
  io:format("~p releasing ~p~n", [Name, S]),
  allocator:release(Allocator, S),
  allocate_test(Allocator, Name, N).