-module(rotating_bloom_filter).

-define(FILTER_FILE, "bloom_filter").
-define(METADATA_FILE, "bloom_filter_meta").
-define(FPP_PROBABILITY, 0.01).
-define(GC_ADDS_FRACTION, 0.6).
-define(GC_REMOVES_FRACTION, 0.3).


-record(bloom_filter_state, {filter_one, filter_two, counters}).
-record(bloom_filter_counter, {adds, removes}).
-record(bloom_filter_active, {active, state}).
-define(WAIT_INTERVAL, 100).

-export([init/1, load/1, save/2]).
-export([add/2, contains/2, record_removal/2]).
-export([record_false_positive/1]).

-export([wait_for_rotated/2]).

%% eBloom filter consumes 1.14 MB per 1 million predicted count
%% and 0.01 probability.
%% Given the constant probability, it will grow linearly when increasing
%% predicted count.
%% This module uses two filter objects, so it will use 2.28 MB of memory with
%% 1 million predicted count.

init(PredCount) ->
    FPProbability = ?FPP_PROBABILITY,
    RandomSeed = rand:uniform(10000),
    {ok, Filter} = ebloom:new(PredCount, FPProbability, RandomSeed),
    {ok, FilterTwo} = ebloom:new(PredCount, FPProbability, RandomSeed),

    BloomCounters = init_metadata_ets(0,0),
    #bloom_filter_state{ filter_one = Filter,
                         filter_two = FilterTwo,
                         counters = BloomCounters }.

load(Dir) ->
    FileName = filename:join(Dir, ?FILTER_FILE),
    case {load_bloom_filter(FileName), load_counters(Dir)} of
        {{ok, Binary}, {ok, {Adds, Removes}}} ->
            {ok, Filter} = ebloom:deserialize(Binary),
            PredCount = ebloom:predicted_elements(Filter),
            FPProbability = ebloom:desired_fpp(Filter),
            RandomSeed = ebloom:random_seed(Filter),
            {ok, FilterTwo} = ebloom:new(PredCount, FPProbability, RandomSeed),
            Counters = init_metadata_ets(Adds,Removes),
            {ok, #bloom_filter_state{ filter_one = Filter,
                                      filter_two = FilterTwo,
                                      counters = Counters }};
        {{error, Err}, _} ->
            {error, Err};
        {_, {error, Err}} ->
            {error, Err}
    end.

save(#bloom_filter_state{counters = Counters} = Bloom, Dir) ->
    %% Save the current filter only.
    ActiveFilterIndex = case ets:lookup(Counters, bloom_filter_active) of
        [#bloom_filter_active{state = rotating}] ->
            wait_for_rotated(Bloom, ?WAIT_INTERVAL),
            %% After rotating it will be clean.
            [#bloom_filter_active{state = clean, active = Filter}] =
                ets:lookup(Counters, bloom_filter_active),
            Filter;
        [#bloom_filter_active{state = clean, active = Filter}] ->
            Filter
    end,

    save_counters(Counters, Dir),

    ActiveFilter = element(ActiveFilterIndex, Bloom),
    FileName = filename:join(Dir, ?FILTER_FILE),
    Serialized = ebloom:serialize(ActiveFilter),
    ok = ebloom:clear(ActiveFilter),
    file:write_file(FileName, Serialized).

add(MsgId, #bloom_filter_state{ filter_one = FOne,
                                                filter_two = FTwo,
                                                counters = Counters }) ->
    %% Adding to both filters is faster than reading
    %% the ETS table and selecting one
    ok = ebloom:insert(FOne, MsgId),
    ok = ebloom:insert(FTwo, MsgId),
    ets:update_counter(Counters,
                       bloom_filter_counter,
                       {#bloom_filter_counter.adds, 1}),
    ok.

contains(MsgId, #bloom_filter_state{ filter_one = FOne,
                                               filter_two = FTwo}) ->
    %% We check both filters.
    %% At least one should contain an entity,
    %% to guarantee no false-negatives
    ebloom:contains(FOne, MsgId) orelse
    ebloom:contains(FTwo, MsgId).

record_removal(#bloom_filter_state{counters = Counters} = Bloom, FoldFun) ->
    ets:update_counter(Counters,
                       bloom_filter_counter,
                       {#bloom_filter_counter.removes, 1}),
    maybe_rotate(Bloom, FoldFun),
    ok.


init_metadata_ets(Adds, Removes) ->
    Counters = ets:new(bloom_filter_ets, [set, public]),
    true = ets:insert(Counters,
                      #bloom_filter_counter{ adds = Adds,
                                             removes = Removes}),
    true = ets:insert(Counters,
                      #bloom_filter_active{
                          active = #bloom_filter_state.filter_one,
                          state = clean }),
    true = ets:insert(Counters, {false_positives, 0}),
    Counters.

load_bloom_filter(FileName) ->
    case file:read_file(FileName) of
        {ok, Binary} ->
            case file:delete(FileName) of
                ok -> {ok, Binary};
                {error, Err} -> {error, Err}
            end;
        {error, Err} -> {error, Err}
    end.

load_counters(Dir) ->
    Path = filename:join(Dir, ?METADATA_FILE),
    case rabbit_file:read_term_file(Path) of
        {ok, [{Adds, Removes}]} when is_integer(Adds), is_integer(Removes) ->
            case file:delete(Path) of
                ok             -> {ok,  {Adds, Removes}};
                {error, Error} -> {error, Error}
            end;
        {error, Error} -> {error, Error}
    end.

save_counters(Counters, Dir) ->
    Path = filename:join(Dir, ?METADATA_FILE),
    [#bloom_filter_counter{adds = Adds, removes = Removes}] =
        ets:lookup(Counters, bloom_filter_counter),
    rabbit_file:write_term_file(Path, [{Adds, Removes}]).

%% ------------------------------------
%% Rotating functions
%% ------------------------------------

maybe_rotate(#bloom_filter_state{counters = Counters} = Bloom, FoldFun) ->
    %% Do not execute the function, if it's called by a non-owner process
    Owner = ets:info(Counters, owner),
    Self = self(),
    case Self =:= Owner of
        false ->
            rabbit_log:warning("Trying to rotate a bloom index from non-owner process ~p."
                               " Owner process is ~p",
                               [Self, Owner]),
            ok;
        true  ->
            [#bloom_filter_counter{adds = Adds, removes = Removes}] =
                ets:lookup(Counters, bloom_filter_counter),
            PredCount = ebloom:predicted_elements(
                Bloom#bloom_filter_state.filter_one),
            %% If number of adds to the filter reaches
            %% predicted size * GC_ADDS_FRACTION, we need to clean it up.
            %% If number of deletes did not reached
            %% predicted size * GC_REMOVES_FRACTION - do nothing and
            %% let filter degrade.
            case Adds > ?GC_ADDS_FRACTION * PredCount andalso
                 Removes > ?GC_REMOVES_FRACTION * PredCount of
                true ->
                    case ets:lookup(Counters, bloom_filter_active) of
                        [#bloom_filter_active{state = rotating}] ->
                            %% If we already rotating an index, we are under
                            %% a high load and probably need to
                            %% increase the filter size.
                            rabbit_log:warning("Bloom filter is already rotating!!!!");
                        [#bloom_filter_active{state = clean}] ->
                            %% Clean up counters and set state to rotating.
                            ok = set_rotating_state(Counters, Adds, Removes),
                            %% Create a one-off rotating process.
                            %% We clean up by creating an additional filter
                            %% and starting a fold over FoldFun
                            %% to add existing entities.
                            spawn_link(fun() -> rotate(Bloom, FoldFun) end)
                    end,
                    ok;
                false -> ok
            end
    end.

rotate(#bloom_filter_state{counters = Counters} = Bloom, FoldFun) ->
    [#bloom_filter_active{state = rotating, active = Active}] =
        ets:lookup(Counters, bloom_filter_active),
    Rotating = case Active of
        #bloom_filter_state.filter_one -> #bloom_filter_state.filter_two;
        #bloom_filter_state.filter_two -> #bloom_filter_state.filter_one
    end,

    ActiveFilter = element(Active, Bloom),
    RotatingFilter = element(Rotating, Bloom),

    %% We insert to both filters, so rotating filter should be cleared
    ebloom:clear(RotatingFilter),
    FoldFun(fun(MsgId, ok) ->
                ok = ebloom:insert(RotatingFilter, MsgId)
            end,
            ok),
    %% When we finish with adding elements to a new filter,
    %% we should rotate it and clean the old active filter.
    ok = set_clean_state(Counters, Rotating),
    ok = ebloom:clear(ActiveFilter).

wait_for_rotated(#bloom_filter_state{counters = Counters} = Bloom, Interval) ->
    timer:sleep(Interval),
    case ets:lookup(Counters, bloom_filter_active) of
        [#bloom_filter_active{state = rotating}] ->
            wait_for_rotated(Bloom, Interval);
        [#bloom_filter_active{state = clean}] ->
            ok
    end.

set_rotating_state(Counters, Adds, Removes) ->
    %% Reset counters
    true = ets:insert(Counters,
                      #bloom_filter_counter{ adds = Adds - Removes,
                                             removes = 0}),
    %% Set state to 'rotating'
    true = ets:update_element(Counters,
                              bloom_filter_active,
                              {#bloom_filter_active.state, rotating}),
    ok.


set_clean_state(Counters, Filter) ->
    %% Set state to 'clean' and 'active' to a new filter position
    true = ets:insert(Counters,
                      #bloom_filter_active{ state = clean, active = Filter}),
    ok.

record_false_positive(#bloom_filter_state{counters = Counters}) ->
    ets:update_counter(Counters, false_positives, {2, 1}).