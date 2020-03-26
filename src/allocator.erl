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
  Pid = spawn_link(fun() ->
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

takeResources(AvailableResources, [], SelfPid, Pid, Ref) ->
  io:format("After taking resources, these where left: ~p \n", [AvailableResources]),
  AvailableResources;

takeResources(AvailableResources, Requested, RequesterPid, Pid, Ref) ->
  [H | T] = Requested,
  case maps:is_key(H, AvailableResources) of
    true ->
      io:format("Map contains ~p \n", [AvailableResources]),
      takeResources(maps:remove(H, AvailableResources), T, RequesterPid, Pid, Ref);
    false ->
      io:format("Map contains ~p but i want ~p \n", [AvailableResources, Requested]),
      io:format("Trying again"),
      RequesterPid ! {request, {Pid, Ref, Requested}}
    %timer:sleep(5000),
    %takeResources(AvailableResources, [H | T])
  end.

check_all_resources(_Avail, []) ->
  true;

check_all_resources(Avail, Requested) ->
  [H|T] = Requested,
  case maps:is_key(H, Avail) of
    true ->
      io:format("Checking resources true (~p) avail = ~p requested = ~p \n", [H,Avail, Requested]),
      check_all_resources(Avail, T);
    false ->
      io:format("Checking resources false (~p) avail = ~p requested = ~p \n", [H, Avail, Requested]),
      false
  end.
convert_list_to_map([], Map, OldResources) ->
  Map;

convert_list_to_map(Requested, Map, OldResources) ->
  [H | T] = Requested,
  NewMap = maps:put(H, maps:get(H, OldResources), Map),
  convert_list_to_map(T, NewMap, OldResources).

allocator(Resources) ->
  receive
    {request, {Pid, Ref, RequestedResources}} ->
      case check_all_resources(Resources, RequestedResources) of
        true ->
          RemainingResources = takeResources(Resources, RequestedResources, self(), Pid, Ref),
          RequestedResourcesMap = convert_list_to_map(RequestedResources, #{}, Resources),
          Pid ! {granted, Ref, RequestedResourcesMap},
          allocator(RemainingResources);
        false ->
          io:format("Resending request, not all resources available, I wanted ~p, but only ~p was avail \n", [RequestedResources, Resources]),
          timer:sleep(1000),
          self() ! {request, {Pid, Ref, RequestedResources}},
          allocator(Resources)
      end;

    {release, {Pid, Ref, ReleasedResources}} ->
      Pid ! {released, Ref},
      NewMap = maps:merge(ReleasedResources, Resources),
      io:format("Resources after releasing: ~p", [NewMap]),
      allocator(NewMap)
  end.



test2() ->
  Allocator = allocator:start(#{a=>10, b=>20, c=>30}),
  spawn(?MODULE, allocate_test, [Allocator, "Process A", [a, b]]),
  spawn(?MODULE, allocate_test, [Allocator, "Process B", [b]]),
  spawn(?MODULE, allocate_test, [Allocator, "Process C", [a, b, c]]).

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