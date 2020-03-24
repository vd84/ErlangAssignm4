%%%-------------------------------------------------------------------
%%% @author dogge
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. mars 2020 11:07
%%%-------------------------------------------------------------------
-module(barrier).
-author("dogge").

%% API
-export([start/1, wait/2, test/0]).

start(Expected) ->
  ExpectedSorted = lists:sort(Expected),
  spawn_link(fun () -> loop(ExpectedSorted, [], []) end).

loop(Expected, PidRefs, Refs) when Refs =:= Expected ->

  [Pid ! {continue, Ref} || {Pid, Ref} <- PidRefs],
  loop(Expected, [], []);

loop(Expected, PidRefs, Refs) ->
  receive
    {arrive, {Pid, Ref}} ->
      case lists:member(Ref, Expected) of
        true ->
          loop(Expected, [{Pid, Ref}|PidRefs], lists:sort([Ref|Refs]));
        false ->
          Pid ! {continue, Ref},
          loop(Expected, PidRefs, Refs)
      end
  end.

wait(Barrier, Ref) ->
  %%Ref = make_ref(),
  Barrier ! {arrive, {self(), Ref}},
  receive
    {continue, Ref} ->
      ok
  end.

do_a() ->
  io:format("DO A \n"),
  timer:sleep(5000).

do_b() ->
  io:format("DO B \n").

do_c() ->
  io:format("DO C \n").

do_more(Var) ->
  io:format("Do more ~p \n",[Var]).

test() ->
  A = make_ref(), B = make_ref(), C = make_ref(),
  Barrier = barrier:start([A, B]),
  spawn(fun () -> do_a(), barrier:wait(Barrier, A), do_more(a) end),
  spawn(fun () -> do_b(), barrier:wait(Barrier, B), do_more(b) end),
  spawn(fun () -> do_c(), barrier:wait(Barrier, C), do_more(c) end),
  io:format("all spawned and ready to rock \n").