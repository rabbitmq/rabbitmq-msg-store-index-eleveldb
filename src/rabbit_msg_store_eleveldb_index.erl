%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_msg_store_eleveldb_index).

-include_lib("rabbit_common/include/rabbit_msg_store.hrl").

-behaviour(rabbit_msg_store_index).

-export([new/1, recover/1,
         lookup/2, insert/2,
         update_ref_count/3, update_file_location/5,
         delete/2, delete_object/2,
         clean_up_temporary_reference_count_entries_without_file/1,
         terminate/1]).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


%% This module implements rabbit_msg_store_index using ElevelDB as a storage.
%% The goal is to achieve bigger message store with smaller memory consumption.
%% For operations which require atomicity, gen_server process is used.
%% The module uses rotating bloom filter to improve lookup performance
%% for non-existent entries.
%% The bloom filter memory consumption is around 2 bytes per entry and
%% predicted number of entries should be configured using app environment.
%% If number of saved messages gets much higher than predicted, bloom filter
%% will report much more false-positives and performance of lookups will
%% suffer.

%% Directories inside message store directory, to store leveldb data.
-define(REF_COUNT_DIR, "rc_eleveldb").
-define(FILE_LOCATION_DIR, "fl_eleveldb").
-define(TMP_RECOVER_INDEX, "recover_no_file_eleveldb").

%% The module uses `open_options`, `read_options` and `write_options`
%% app environment variables, merging them with this default values.
-define(OPEN_OPTIONS,
  [{create_if_missing, true},
   {write_buffer_size, 5242880},
   {compression,  false},
   {use_bloomfilter, true},
   {paranoid_checks, false},
   {verify_compactions, false}]).
-define(READ_OPTIONS, []).
-define(WRITE_OPTIONS, []).

%% Default size of bloom filter. Will consume ~ 2 MB of memory.
%% If message store is to contain more than 1 million messages, this setting
%% should be increased using `bloom_filter_size` app environment variable.
-define(BLOOM_FILTER_SIZE, 1000000).


%% Internal state for gen_server process
-record(internal_state, {
    ref_count_db,
    file_location_db,
    base_dir,
    ref_count_dir,
    file_location_dir,
    read_options,
    write_options,
    recovery_index,
    bloom_filter
}).

%% Index state to be returned to message store
-record(index_state, {
    ref_count_db,
    file_location_db,
    server,
    read_options,
    write_options,
    bloom_filter
}).

%% ------------------------------------
%% rabbit_msg_store_index API
%% ------------------------------------

-type index_state() :: #index_state{}.

-spec new(file:filename()) -> index_state().
new(BaseDir) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [BaseDir, new], []),
    index_state(Pid).

-spec recover(file:filename()) -> {ok, index_state()} | {error, term()}.
recover(BaseDir) ->
    case gen_server:start_link(?MODULE, [BaseDir, recover], []) of
        {ok, Pid}    -> {ok, index_state(Pid)};
        {error, Err} -> {error, Err}
    end.

-spec lookup(rabbit_types:msg_id(), index_state()) -> ('not_found' | tuple()).
lookup(MsgId, #internal_state{ref_count_db = RCDB, file_location_db = FLDB,
                              read_options = ReadOptions, bloom_filter = Bloom}) ->
    do_lookup(MsgId, RCDB, FLDB, ReadOptions, Bloom);
lookup(MsgId, #index_state{ref_count_db = RCDB, file_location_db = FLDB,
                           read_options = ReadOptions, bloom_filter = Bloom}) ->
    do_lookup(MsgId, RCDB, FLDB, ReadOptions, Bloom).

%% This function can fail if the object exists
-spec insert(tuple(), index_state()) -> 'ok'.
insert(Obj, State) ->
    MsgId = Obj#msg_location.msg_id,
    %% We need the file to update recovery index.
    %% File can be undefined, in that case it
    %% should be deleted at the end of a recovery
    %% File can become defined, and should not be deleted.
    File = Obj#msg_location.file,
    %% Fail if the same key entry exists
    not_found = lookup(MsgId, State),
    ok = do_insert(MsgId, Obj, File, State),
    ok = rotating_bloom_filter:add(MsgId, State#index_state.bloom_filter).

-spec update_ref_count(rabbit_types:msg_id(), integer(), index_state()) -> 'ok'.
update_ref_count(MsgId, RefCount, #index_state{ server = Server }) ->
    Val = encode_val(RefCount),
    ok = gen_server:call(Server, {update_ref_count, MsgId, Val}).

-spec update_file_location(rabbit_types:msg_id(), string(), integer(),
                           integer(), index_state()) -> 'ok'.
update_file_location(MsgId, File, Offset, TotalSize, #index_state{ server = Server }) ->
    Val = encode_val({File, Offset, TotalSize}),
    ok = gen_server:call(Server, {update_file_location, MsgId, Val, File}).

-spec delete(rabbit_types:msg_id(), index_state()) -> 'ok'.
delete(MsgId, #index_state{ server = Server }) ->
    gen_server:call(Server, {delete, MsgId}).

-spec delete_object(tuple(), index_state()) -> 'ok'.
delete_object(Obj, #index_state{ server = Server }) ->
    MsgId = Obj#msg_location.msg_id,
    gen_server:call(Server, {delete_object, MsgId, Obj}).

%% Clean up is using temporary recovery_index to store entries with `undefined`
%% file field. After recovery, the entries left in this index are deleted.
-spec clean_up_temporary_reference_count_entries_without_file(index_state()) -> 'ok'.
clean_up_temporary_reference_count_entries_without_file(#index_state{ server = Server }) ->
    {ok, RecoveryIndex, BaseDir} = gen_server:call(Server, clean_up_temporary_reference_count_entries_without_file),
    %% Destroy the recovery DB in a one-off process
    Pid = self(),
    spawn_link(fun() -> clear_recovery_index(RecoveryIndex, BaseDir), unlink(Pid) end),
    ok.

-spec terminate(index_state()) -> any().
terminate(#index_state{ server = Server }) ->
    ok = gen_server:stop(Server).

%% ------------------------------------
%% Gen-server API
%% ------------------------------------

%% Non-clean shutdown. We create recovery index
init([BaseDir, new]) ->
    RCDir = ref_count_dir(BaseDir),
    FLDir = file_location_dir(BaseDir),
    rabbit_file:recursive_delete([RCDir]),
    rabbit_file:recursive_delete([FLDir]),
    {ok, RecoveryIndex} = init_recovery_index(BaseDir),
    {ok, RCDB} = eleveldb:open(RCDir, open_options()),
    {ok, FLDB} = eleveldb:open(FLDir, open_options()),
    BloomFilterPredictedCount =
        application:get_env(rabbitmq_msg_store_index_eleveldb,
                            bloom_filter_size,
                            ?BLOOM_FILTER_SIZE),
    Bloom = rotating_bloom_filter:init(BloomFilterPredictedCount),
    rabbit_log:info("INIT INDEX ~p~n", [self()]),
    {ok,
     #internal_state{
        ref_count_db = RCDB,
        file_location_db = FLDB,
        ref_count_dir = RCDir,
        file_location_dir = FLDir,
        base_dir = BaseDir,
        read_options = read_options(),
        write_options = write_options(),
        recovery_index = RecoveryIndex,
        bloom_filter = Bloom }};

%% Clean shutdown. We don't need recovery index
init([BaseDir, recover]) ->
    RCDir = ref_count_dir(BaseDir),
    FLDir = file_location_dir(BaseDir),
    case {eleveldb:open(RCDir, open_options()),
          eleveldb:open(FLDir, open_options()),
          rotating_bloom_filter:load(BaseDir)} of
        {{ok, RCDB}, {ok, FLDB}, {ok, Bloom}}  ->
            {ok,
             #internal_state{
                ref_count_db = RCDB,
                file_location_db = FLDB,
                ref_count_dir = RCDir,
                file_location_dir = FLDir,
                base_dir = BaseDir,
                read_options = read_options(),
                write_options = write_options(),
                bloom_filter = Bloom }};
        {{error, Err}, _, _} ->
            rabbit_log:error("Error trying to recover a LevelDB after graceful shutdown ~p~n", [Err]),
            {stop, Err};
        {_, {error, Err}, _} ->
            rabbit_log:error("Error trying to recover a LevelDB after graceful shutdown ~p~n", [Err]),
            {stop, Err};
        {_, _, {error, Err}} ->
            rabbit_log:error("Error trying to recover a bloom filter after graceful shutdown ~p~n", [Err]),
            {stop, Err}
    end.

handle_call({update_ref_count, MsgId, Val}, _From, State) ->
    {reply, do_update_ref_count(MsgId, Val, State), State};

handle_call({update_file_location, MsgId, Val, File}, _From, State) ->
    {reply, do_update_file_location(MsgId, Val, File, State), State};

handle_call({delete, MsgId}, _From, State) ->
    do_delete(MsgId, State),
    {reply, ok, State};

handle_call({delete_object, MsgId, Obj}, _From,
            #internal_state{ref_count_db = RCDB,
                            file_location_db = FLDB,
                            read_options = ReadOptions} = State) ->
    RefCount = ref_count(Obj),
    FileLocation = file_location(Obj),
    RefCVal = encode_val(RefCount),
    FileLVal = encode_val(FileLocation),
    case {eleveldb:get(RCDB, MsgId, ReadOptions), eleveldb:get(FLDB, MsgId, ReadOptions)} of
        {{ok, RefCVal}, {ok, FileLVal}} -> do_delete(MsgId, State);
        {not_found, not_found}          -> ok
    end,
    {reply, ok, State};

handle_call(clean_up_temporary_reference_count_entries_without_file, _From,
            #internal_state{ref_count_db = RCDB,
                            file_location_db = FLDB,
                            base_dir = BaseDir,
                            recovery_index = RecoveryIndex,
                            read_options = ReadOptions,
                            write_options = WriteOptions} = State) ->
    eleveldb:fold_keys(
        RecoveryIndex,
        fun(MsgId, nothing) ->
            ok = eleveldb:delete(RCDB, MsgId, WriteOptions),
            ok = eleveldb:delete(FLDB, MsgId, WriteOptions),
            nothing
        end,
        nothing,
        ReadOptions),
    {reply, {ok, RecoveryIndex, BaseDir}, State#internal_state{ recovery_index = undefined }};

handle_call(references, _From, #internal_state{ ref_count_db = RCDB,
                                                file_location_db = FLDB } = State) ->
    {reply, {ok, RCDB, FLDB}, State};
handle_call(bloom_filter, _From, #internal_state{ bloom_filter = Bloom } = State) ->
    {reply, {ok, Bloom}, State}.

handle_cast({maybe_update_recovery_index, MsgId, File}, State) ->
    maybe_update_recovery_index(MsgId, File, State),
    {noreply, State};
handle_cast(_, State) ->
    {noreply, State}.


handle_info(_, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(normal, State) ->
    do_terminate(State);
%% Close DB references without saving index to disk.
%% If references are not saved - ElevelDB will fail to open DB directory.
terminate(Error, #internal_state{ ref_count_db = RCDB,
                                  file_location_db = FLDB,
                                  recovery_index = Recover }) ->
    rabbit_log:error("Message store ElevelDB index terminated with error ~p~n",
                     [Error]),
    ok = eleveldb:close(RCDB),
    ok = eleveldb:close(FLDB),
    case Recover of
        undefined -> ok;
        _         -> ok = eleveldb:close(Recover)
    end,
    ok.

%% ------------------------------------
%% Internal functions
%% ------------------------------------

do_insert(MsgId, Obj, File,
          State = #index_state{ ref_count_db = RCDB,
                                file_location_db = FLDB,
                                write_options = WriteOptions}) ->
    RefCount = ref_count(Obj),
    FileLocation = file_location(Obj),
    RefCVal = encode_val(RefCount),
    FileLVal = encode_val(FileLocation),
    maybe_update_recovery_index(MsgId, File, State),
    ok = eleveldb:put(RCDB, MsgId, RefCVal, WriteOptions),
    ok = eleveldb:put(FLDB, MsgId, FileLVal, WriteOptions).

do_lookup(MsgId, RCDB, FLDB, ReadOptions, Bloom) ->
    case rotating_bloom_filter:contains(MsgId, Bloom) of
        true ->
            case eleveldb:get(RCDB, MsgId, ReadOptions) of
                not_found    ->
                    rotating_bloom_filter:record_false_positive(Bloom),
                    not_found;
                {ok, RCVal}    ->
                    RC = decode_val(RCVal),
                    {ok, FLVal} = eleveldb:get(FLDB, MsgId, ReadOptions),
                    FL = decode_val(FLVal),
                    make_obj(MsgId, RC, FL);
                {error, Err} -> {error, Err}
            end;
        false -> not_found
    end.

do_update_ref_count(MsgId, Val, #internal_state{ ref_count_db = DB, write_options = WriteOptions }) ->
    ok = eleveldb:put(DB, MsgId, Val, WriteOptions).

do_update_file_location(MsgId, Val, File, State = #internal_state{ file_location_db = DB, write_options = WriteOptions }) ->
    maybe_update_recovery_index(MsgId, File, State),
    ok = eleveldb:put(DB, MsgId, Val, WriteOptions).

do_delete(MsgId, #internal_state{ ref_count_db = RCDB,
                                  file_location_db = FLDB,
                                  write_options = WriteOptions,
                                  bloom_filter = Bloom} = State) ->
    maybe_delete_recovery_index(MsgId, State),
    ok = eleveldb:delete(RCDB, MsgId, WriteOptions),
    ok = eleveldb:delete(FLDB, MsgId, WriteOptions),
    FoldFun = fun(Fun, Acc) ->
        eleveldb:fold_keys(RCDB,
            Fun,
            Acc,
            ?READ_OPTIONS)
    end,
    ok = rotating_bloom_filter:record_removal(Bloom, FoldFun).

do_terminate(#internal_state { ref_count_db = RCDB,
                               file_location_db = FLDB,
                               recovery_index = RecoveryIndex,
                               base_dir = BaseDir,
                               ref_count_dir = RCDir,
                               file_location_dir = FLDir,
                               bloom_filter = Bloom }) ->
    clear_recovery_index(RecoveryIndex, BaseDir),
    case {eleveldb:close(RCDB), eleveldb:close(FLDB), rotating_bloom_filter:save(Bloom, BaseDir)} of
        {ok, ok, ok}                 -> ok;
        {{error, Err}, _, BloomRes} ->
            rabbit_log:error("Unable to stop message store index"
                             " for directory ~p.~nError: ~p~n"
                             "Bloom filter save: ~p~n",
                             [filename:dirname(RCDir), Err, BloomRes]),
            error(Err);
        {_, {error, Err}, BloomRes} ->
            rabbit_log:error("Unable to stop message store index"
                             " for directory ~p.~nError: ~p~n"
                             "Bloom filter save: ~p~n",
                             [filename:dirname(FLDir), Err, BloomRes]),
            error(Err);
        {_, _, {error, Err}} ->
            rabbit_log:error("Unable to save a bloom filter for message store index"
                             " for directory ~p.~nError: ~p~n",
                             [filename:dirname(BaseDir), Err]),
            error(Err)
    end.

index_state(Pid) ->
    {ok, RCDB, FLDB} = gen_server:call(Pid, references),
    {ok, Bloom} = gen_server:call(Pid, bloom_filter),
    #index_state{ ref_count_db = RCDB,
                  file_location_db = FLDB,
                  server = Pid,
                  read_options = read_options(),
                  write_options = write_options(),
                  bloom_filter = Bloom}.


%% ------------------------------------
%% Recovery index functions
%% ------------------------------------


maybe_update_recovery_index(MsgId, File, #index_state{server = Server}) ->
    gen_server:cast(Server, {maybe_update_recovery_index, MsgId, File});
maybe_update_recovery_index(_MsgId, _File,
                            #internal_state{recovery_index = undefined}) ->
    ok;
maybe_update_recovery_index(MsgId, File,
                            #internal_state{recovery_index = RecoveryIndex}) ->
    case File of
        undefined ->
            ok = eleveldb:put(RecoveryIndex, MsgId, <<>>, []);
        _ ->
            ok = eleveldb:delete(RecoveryIndex, MsgId, [])
    end.

maybe_delete_recovery_index(_MsgId,
                            #internal_state{recovery_index = undefined}) ->
    ok;
maybe_delete_recovery_index(MsgId,
                            #internal_state{recovery_index = RecoveryIndex}) ->
    ok = eleveldb:delete(RecoveryIndex, MsgId, []).

clear_recovery_index(undefined, _) -> ok;
clear_recovery_index(RecoveryIndex, BaseDir) ->
    ok = eleveldb:close(RecoveryIndex),
    ok = eleveldb:destroy(recover_index_dir(BaseDir),
                          open_options()).

%% ------------------------------------
%% Configuration functions
%% ------------------------------------

ref_count_dir(BaseDir) ->
    filename:join(BaseDir, ?REF_COUNT_DIR).

file_location_dir(BaseDir) ->
    filename:join(BaseDir, ?FILE_LOCATION_DIR).

recover_index_dir(BaseDir) ->
    filename:join(BaseDir, ?TMP_RECOVER_INDEX).

init_recovery_index(BaseDir) ->
    RecoverNoFileDir = recover_index_dir(BaseDir),
    ok = rabbit_file:recursive_delete([RecoverNoFileDir]),
    eleveldb:open(RecoverNoFileDir, open_options()).

decode_val(Val) ->
    binary_to_term(Val).

encode_val(Obj) ->
    term_to_binary(Obj).

open_options() ->
    lists:ukeymerge(1,
        lists:usort(application:get_env(rabbitmq_msg_store_index_eleveldb, open_options, [])),
        lists:usort(?OPEN_OPTIONS)).

write_options() ->
    lists:ukeymerge(1,
        lists:usort(application:get_env(rabbitmq_msg_store_index_eleveldb, write_options, [])),
        lists:usort(?WRITE_OPTIONS)).

read_options() ->
    lists:ukeymerge(1,
        lists:usort(application:get_env(rabbitmq_msg_store_index_eleveldb, read_options, [])),
        lists:usort(?READ_OPTIONS)).



make_obj(MsgId, RefCount, {File, Offset, TotalSize}) ->
    #msg_location{msg_id = MsgId,
                  ref_count = RefCount,
                  file = File,
                  offset = Offset,
                  total_size = TotalSize}.


ref_count(#msg_location{ ref_count = RC }) -> RC.

file_location(#msg_location{file = File,
                            offset = Offset,
                            total_size = TotalSize}) ->
    {File, Offset, TotalSize}.
