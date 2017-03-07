-module(bloom_filter_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(FOLD_SLEEP, 2).
-define(CHECK_SLEEP, 20).
-define(ITEMS_BEFORE_ROTATION, 601).
-define(ITEMS_AFTER_ROTATION, 300).

all() ->
    [
        save_load,
        add_remove,
        rotate_filter
    ].

init_per_suite(Config) ->
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    file:make_dir(DataDir),
    Config.

end_per_suite(Config) ->
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    rabbit_file:recursive_delete([DataDir]),
    Config.

add_remove(_Config) ->
    Bloom = rotating_bloom_filter:init(100000),
    Ids = [integer_to_binary(rand:uniform(10000000)) || _ <- lists:seq(1, 100)],
    [ ok = rotating_bloom_filter:add(Id, Bloom) || Id <- Ids ],
    [ true = rotating_bloom_filter:contains(Id, Bloom) || Id <- Ids ],
    [ ok = rotating_bloom_filter:record_removal(Bloom, fun fold_fun/2) || _ <- Ids ],
    %% False positive until next rotation
    [ true = rotating_bloom_filter:contains(Id, Bloom) || Id <- Ids ],
    [Ets] = [Tab || Tab <- ets:all(), ets:info(Tab, name) == bloom_filter_ets],
    [{bloom_filter_counter, 100, 100}] = ets:lookup(Ets, bloom_filter_counter).

save_load(Config) ->
    Bloom = rotating_bloom_filter:init(100000),
    Ids = [integer_to_binary(rand:uniform(10000000)) || _ <- lists:seq(1, 100)],
    [ ok = rotating_bloom_filter:add(Id, Bloom) || Id <- Ids ],
    [ true = rotating_bloom_filter:contains(Id, Bloom) || Id <- Ids ],

    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    ok = rotating_bloom_filter:save(Bloom, DataDir),
    {ok, LoadedBloom} = rotating_bloom_filter:load(DataDir),
    [ true = rotating_bloom_filter:contains(Id, LoadedBloom) || Id <- Ids ].

rotate_filter(_Config) ->
    %% The size is 1000, rotation will be called when there are 600 adds and
    %% 300 removals
    Bloom = rotating_bloom_filter:init(1000),
    Ids = [integer_to_binary(rand:uniform(10000000)) || _ <- lists:seq(1, ?ITEMS_BEFORE_ROTATION)],
    IdsToRemove = lists:nthtail(?ITEMS_AFTER_ROTATION, Ids),
    IdsLeft = Ids -- IdsToRemove,

    [ ok = rotating_bloom_filter:add(Id, Bloom) || Id <- Ids ],

    [ true = rotating_bloom_filter:contains(Id, Bloom) || Id <- Ids ],

    [ ok = rotating_bloom_filter:record_removal(Bloom, fold_fun(IdsLeft))
      || _ <- lists:seq(1, ?ITEMS_BEFORE_ROTATION - ?ITEMS_AFTER_ROTATION) ],

    %% It should rotate here.
    [Ets] = [Tab || Tab <- ets:all(), ets:info(Tab, name) == bloom_filter_ets],

    [{bloom_filter_counter, ?ITEMS_AFTER_ROTATION, 0}] = ets:lookup(Ets, bloom_filter_counter),
    [{bloom_filter_active, _, rotating}] = ets:lookup(Ets, bloom_filter_active),

    wait_for_rotated(Bloom, IdsLeft, IdsToRemove, 2 * ?ITEMS_AFTER_ROTATION * ?FOLD_SLEEP / ?CHECK_SLEEP),

    [ true = rotating_bloom_filter:contains(Id, Bloom) || Id <- IdsLeft ],

    [ false = rotating_bloom_filter:contains(Id, Bloom) || Id <- IdsToRemove ].

wait_for_rotated(_, _, _, 0) -> ok;
wait_for_rotated(Bloom, IdsLeft, IdsToRemove, Attempts) ->
    [ true = rotating_bloom_filter:contains(Id, Bloom) || Id <- IdsLeft ],
    InProgress = lists:any(fun(Id) ->
                               rotating_bloom_filter:contains(Id, Bloom)
                           end,
                           IdsToRemove),
    case InProgress of
        true ->
            [ true = rotating_bloom_filter:contains(Id, Bloom) || Id <- IdsLeft ],
            timer:sleep(?CHECK_SLEEP),
            wait_for_rotated(Bloom, IdsLeft, IdsToRemove, Attempts - 1);
        false -> ok
    end.

fold_fun(Ids) ->
    fun(Fun, Acc) ->
        lists:foldl(
            fun(El, Acc) ->
                timer:sleep(?FOLD_SLEEP),
                Fun(El, Acc)
            end,
            Acc, Ids)
    end.

fold_fun(_, _) -> ok.