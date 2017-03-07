-module(unit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("rabbit_common/include/rabbit_msg_store.hrl").

-compile(export_all).

all() ->
    [
        {group, rabbit_msg_store_eleveldb_index},
        {group, rabbit_msg_store_ets_index}
    ].

groups() ->
    Tests =
    [insert,
     insert_fails_if_exists,
     update,
     update_does_insert,
     update_fields,
     update_fields_requires_existing,
     delete,
     delete_object,
     cleanup_temporary,
     init_new,
     save_load],

    [{rabbit_msg_store_eleveldb_index, [], Tests},
     {rabbit_msg_store_ets_index, [], Tests}].

init_per_suite(Config) ->
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    file:make_dir(DataDir),
    Config.

end_per_suite(Config) ->
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    rabbit_file:recursive_delete([DataDir]),
    Config.

init_per_group(Group, Config) ->
    rabbit_ct_helpers:set_config(Config, {index_module, Group}).

end_per_group(_Group, Config) ->
    Config.

insert(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    MsgId2 = <<"id_2">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    Msg2 = #msg_location{ msg_id = MsgId2 },
    not_found = IndexModule:lookup(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId2, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId2, IndexState),
    ok = IndexModule:insert(Msg2, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState).

insert_fails_if_exists(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    not_found = IndexModule:lookup(MsgId1, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),

    try IndexModule:insert(Msg1, IndexState) of
        ok -> error(insert_should_fail_if_entry_exists)
    catch _:_ -> ok
    end.

update(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    Msg1Updated = #msg_location{ msg_id = MsgId1, file = "file", ref_count = 10 },
    not_found = IndexModule:lookup(MsgId1, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    ok = IndexModule:update(Msg1Updated, IndexState),
    Msg1Updated = IndexModule:lookup(MsgId1, IndexState).

update_does_insert(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    not_found = IndexModule:lookup(MsgId1, IndexState),
    ok = IndexModule:update(Msg1, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    ok = IndexModule:update(Msg1, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState).

update_fields(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    not_found = IndexModule:lookup(MsgId1, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),

    Msg1UpdatedFile = Msg1#msg_location{ file = "file", offset = 10},
    Msg1UpdatedRef = Msg1UpdatedFile#msg_location{ ref_count = 10 },
    FieldsFile = [{#msg_location.file, Msg1UpdatedFile#msg_location.file},
                  {#msg_location.offset, Msg1UpdatedFile#msg_location.offset}],

    FieldsRefCount = {#msg_location.ref_count, Msg1UpdatedRef#msg_location.ref_count},

    ok = IndexModule:update_fields(MsgId1, FieldsFile, IndexState),
    Msg1UpdatedFile = IndexModule:lookup(MsgId1, IndexState),

    ok = IndexModule:update_fields(MsgId1, FieldsRefCount, IndexState),
    Msg1UpdatedRef = IndexModule:lookup(MsgId1, IndexState).

update_fields_requires_existing(Config) ->
    IndexModule = ?config(index_module, Config),
    process_flag(trap_exit, true),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    not_found = IndexModule:lookup(MsgId1, IndexState),
    try IndexModule:update_fields(MsgId1, {#msg_location.ref_count, 2}, IndexState) of
        ok -> error(update_fields_should_fail_if_no_entry)
    catch _:_ -> ok
    end,
    process_flag(trap_exit, false).

delete(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    MsgId2 = <<"id_2">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    Msg2 = #msg_location{ msg_id = MsgId2 },
    not_found = IndexModule:lookup(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId2, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    ok = IndexModule:insert(Msg2, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState),

    ok = IndexModule:delete(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState),

    ok = IndexModule:delete(MsgId2, IndexState),
    not_found = IndexModule:lookup(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId2, IndexState),

    ok = IndexModule:delete(<<"non_existent">>, IndexState).

delete_object(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    MsgId2 = <<"id_2">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    Msg2 = #msg_location{ msg_id = MsgId2 },
    Msg2NotSame = Msg2#msg_location{ file = "updated_file"},
    not_found = IndexModule:lookup(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId2, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    ok = IndexModule:insert(Msg2, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState),

    ok = IndexModule:delete_object(Msg1, IndexState),
    not_found = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState),

    ok = IndexModule:delete_object(Msg2NotSame, IndexState),
    not_found = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState).


cleanup_temporary(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    MsgId2 = <<"id_2">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    Msg2 = #msg_location{ msg_id = MsgId2 },
    Msg2File = Msg2#msg_location{ file = "updated_file"},
    not_found = IndexModule:lookup(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId2, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    ok = IndexModule:insert(Msg2, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState),

    %% Update Msg2 to have the file field

    ok = IndexModule:update(Msg2File, IndexState),
    ok = IndexModule:clean_up_temporary_reference_count_entries_without_file(IndexState),

    not_found = IndexModule:lookup(MsgId1, IndexState),
    Msg2File = IndexModule:lookup(MsgId2, IndexState).

init_new(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    MsgId2 = <<"id_2">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    Msg2 = #msg_location{ msg_id = MsgId2 },
    not_found = IndexModule:lookup(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId2, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    ok = IndexModule:insert(Msg2, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState),

    IndexModule:terminate(IndexState),
    IndexState1 = init_clean(Config),
    not_found = IndexModule:lookup(MsgId1, IndexState1),
    not_found = IndexModule:lookup(MsgId2, IndexState1).

save_load(Config) ->
    IndexModule = ?config(index_module, Config),
    IndexState = init_clean(Config),
    MsgId1 = <<"id_1">>,
    MsgId2 = <<"id_2">>,
    Msg1 = #msg_location{ msg_id = MsgId1 },
    Msg2 = #msg_location{ msg_id = MsgId2 },
    not_found = IndexModule:lookup(MsgId1, IndexState),
    not_found = IndexModule:lookup(MsgId2, IndexState),
    ok = IndexModule:insert(Msg1, IndexState),
    ok = IndexModule:insert(Msg2, IndexState),
    Msg1 = IndexModule:lookup(MsgId1, IndexState),
    Msg2 = IndexModule:lookup(MsgId2, IndexState),

    IndexModule:terminate(IndexState),
    IndexState1 = init_recover(Config),
    Msg1 = IndexModule:lookup(MsgId1, IndexState1),
    Msg2 = IndexModule:lookup(MsgId2, IndexState1).

init_clean(Config) ->
    IndexModule = ?config(index_module, Config),
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    IndexModule:new(DataDir).

init_recover(Config) ->
    IndexModule = ?config(index_module, Config),
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    {ok, State} = IndexModule:recover(DataDir),
    State.
