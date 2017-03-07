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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_msg_store_eleveldb_index).

-include_lib("rabbit_common/include/rabbit_msg_store.hrl").

-behaviour(rabbit_msg_store_index).

-export([new/1, recover/1,
         lookup/2, insert/2, update/2, update_fields/3, delete/2,
         delete_object/2, clean_up_temporary_reference_count_entries_without_file/1, terminate/1]).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(INDEX_DIR, "eleveldb").
-define(TMP_RECOVER_INDEX, "recover_no_file").

-compile(export_all).

-record(internal_state, {
    db,
    dir,
    read_options,
    write_options,
    recovery_index,
    bloom_filter
}).

-record(index_state, {
    db,
    server,
    read_options,
    bloom_filter,
    write_options
}).

-define(OPEN_OPTIONS,
  [{create_if_missing, true},
   {write_buffer_size, 5242880},
   {compression,  false},
   {use_bloomfilter, true},
   {paranoid_checks, false},
   {verify_compactions, false}]).

-define(READ_OPTIONS, []).
-define(WRITE_OPTIONS, []).

%% rabbit_msg_store_index API

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
lookup(MsgId, #internal_state{db = DB, read_options = ReadOptions, bloom_filter = Bloom}) ->
    do_lookup(MsgId, DB, ReadOptions, Bloom);
lookup(MsgId, #index_state{db = DB, read_options = ReadOptions, bloom_filter = Bloom}) ->
    do_lookup(MsgId, DB, ReadOptions, Bloom).

%% Inserts are executed by a message store process only.
%% GC cannot call insert,
%% Clients cannot call insert

% fail if object exists
-spec insert(tuple(), index_state()) -> 'ok'.
insert(Obj, State) ->
    MsgId = Obj#msg_location.msg_id,
    Val = encode_val(Obj),
%% We need the file to update recovery index.
%% File can be undefined, in that case it
%% should be deleted at the end of a recovery
%% File can become defined, and should not be deleted.
    File = Obj#msg_location.file,
    ok = do_insert(MsgId, Val, File, State),
    ok = rotating_bloom_filter:add(MsgId, State#index_state.bloom_filter).

%% Updates are executed by a message store process, just like inserts.

-spec update(tuple(), index_state()) -> 'ok'.
update(Obj, #index_state{ server = Server }) ->
    MsgId = Obj#msg_location.msg_id,
    Val = encode_val(Obj),
    File = Obj#msg_location.file,
    gen_server:call(Server, {update, MsgId, Val, File}).

%% update_fields can be executed by message store or GC

% fail if object does not exist
-spec update_fields(rabbit_types:msg_id(), ({integer(), any()} |
                                            [{integer(), any()}]),
                        index_state()) -> 'ok'.
update_fields(MsgId, Updates, #index_state{ server = Server }) ->
    gen_server:call(Server, {update_fields, MsgId, Updates}).

%% Deletes are performed by message store only, GC is using delete_object

-spec delete(rabbit_types:msg_id(), index_state()) -> 'ok'.
delete(MsgId, #index_state{ server = Server }) ->
    gen_server:call(Server, {delete, MsgId}).

%% Delete object is performed by GC

% do not delete different object
-spec delete_object(tuple(), index_state()) -> 'ok'.
delete_object(Obj, #index_state{ server = Server }) ->
    MsgId = Obj#msg_location.msg_id,
    Val = encode_val(Obj),
    gen_server:call(Server, {delete_object, MsgId, Val}).

%% clean_up_temporary_reference_count_entries_without_file is called by message store after recovery from scratch

-spec clean_up_temporary_reference_count_entries_without_file(index_state()) -> 'ok'.
clean_up_temporary_reference_count_entries_without_file(#index_state{ server = Server }) ->
    {ok, RecoveryIndex, Dir} = gen_server:call(Server, clean_up_temporary_reference_count_entries_without_file),
    %% Destroy the recovery DB in a one-off process
    Pid = self(),
    spawn_link(fun() -> clear_recovery_index(RecoveryIndex, Dir), unlink(Pid) end),
    ok.

-spec terminate(index_state()) -> any().
terminate(#index_state{ server = Server }) ->
    ok = gen_server:stop(Server).

%% ------------------------------------

%% Gen-server API
-define(BLOOM_PREDICTED_COUNT, 1000000).
%% Non-clean shutdown. We create recovery index
init([BaseDir, new]) ->
    % TODO: recover after crash
    Dir = index_dir(BaseDir),
    rabbit_file:recursive_delete([Dir]),
    {ok, RecoveryIndex} = init_recovery_index(BaseDir),
    {ok, DbRef} = eleveldb:open(Dir, open_options()),
    BloomFilterPredictedCount =
        application:get_env(rabbitmq_msg_store_index_eleveldb,
                            bloom_filter_size,
                            ?BLOOM_PREDICTED_COUNT),
    Bloom = rotating_bloom_filter:init(BloomFilterPredictedCount),
    rabbit_log:info("INIT INDEX ~p~n", [self()]),
    {ok,
     #internal_state{
        db = DbRef,
        dir = Dir,
        read_options = read_options(),
        write_options = write_options(),
        recovery_index = RecoveryIndex,
        bloom_filter = Bloom }};

%% Clean shutdown. We don't need recovery index
init([BaseDir, recover]) ->
    Dir = index_dir(BaseDir),
    case {eleveldb:open(Dir, open_options()), rotating_bloom_filter:load(Dir)} of
        {{ok, DbRef}, {ok, Bloom}}  ->
            {ok,
             #internal_state{
                db = DbRef,
                dir = Dir,
                read_options = read_options(),
                write_options = write_options(),
                bloom_filter = Bloom }};
        {{error, Err}, _} ->
            rabbit_log:error("Error trying to recover a LevelDB after graceful shutdown ~p~n", [Err]),
            {stop, Err};
        {_, {error, Err}} ->
            rabbit_log:error("Error trying to recover a bloom filter after graceful shutdown ~p~n", [Err]),
            {stop, Err}
    end.

handle_call({update, MsgId, Val, File}, _From, State) ->
    {reply, do_insert(MsgId, Val, File, State), State};

handle_call({update_fields, MsgId, Updates}, _From, State) ->
    #msg_location{} = Old = lookup(MsgId, State),
    New = update_elements(Old, Updates),
    File = New#msg_location.file,
    {reply, do_insert(MsgId, encode_val(New), File, State), State};

handle_call({delete, MsgId}, _From, State) ->
    do_delete(MsgId, State),
    {reply, ok, State};

handle_call({delete_object, MsgId, Val}, _From,
            #internal_state{db = DB,
                            read_options = ReadOptions} = State) ->
    case eleveldb:get(DB, MsgId, ReadOptions) of
        {ok, Val} ->
            do_delete(MsgId, State);
        _ -> ok
    end,
    {reply, ok, State};



% This function is called only with `undefined` file name
% TODO: refactor index recovery to avoid additional table
% TODO: refactor index to modify the state.
handle_call(clean_up_temporary_reference_count_entries_without_file, _From,
            #internal_state{db = DB,
                            dir  = Dir,
                            recovery_index = RecoveryIndex,
                            read_options = ReadOptions,
                            write_options = WriteOptions} = State) ->
    eleveldb:fold_keys(
        RecoveryIndex,
        fun(MsgId, nothing) ->
            ok = eleveldb:delete(DB, MsgId, WriteOptions),
            nothing
        end,
        nothing,
        ReadOptions),
    {reply, {ok, RecoveryIndex, Dir}, State#internal_state{ recovery_index = undefined }};

handle_call(reference, _From, #internal_state{ db = DB } = State) ->
    {reply, {ok, DB}, State};
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
    do_terminate(State).

%% ------------------------------------

do_insert(MsgId, Val, File,
          State = #index_state{ db = DB,
                                write_options = WriteOptions}) ->
    do_insert(MsgId, Val, File, State, DB, WriteOptions);

do_insert(MsgId, Val, File,
          State = #internal_state{db = DB,
                                  write_options = WriteOptions}) ->
    do_insert(MsgId, Val, File, State, DB, WriteOptions).

do_insert(MsgId, Val, File, State, DB, WriteOptions) ->
    maybe_update_recovery_index(MsgId, File, State),
    ok = eleveldb:put(DB, MsgId, Val, WriteOptions).

do_lookup(MsgId, DB, ReadOptions, Bloom) ->
    % rabbit_log:error("Read index ~p~n", [{MsgId, DB}]),
    case rotating_bloom_filter:contains(MsgId, Bloom) of
        true ->
            case eleveldb:get(DB, MsgId, ReadOptions) of
                not_found    ->
                    rotating_bloom_filter:record_false_positive(Bloom),
                    not_found;
                {ok, Val}    -> decode_val(Val);
                {error, Err} -> {error, Err}
            end;
        false -> not_found
    end.

do_delete(MsgId, #internal_state{ db = DB,
                                  write_options = WriteOptions,
                                  bloom_filter = Bloom} = State) ->
    % rabbit_log:error("Delete index ~p~n", [{MsgId, DB}]),
    maybe_delete_recovery_index(MsgId, State),
    ok = eleveldb:delete(DB, MsgId, WriteOptions),
    FoldFun = fun(Fun, Acc) ->
        eleveldb:fold_keys(DB,
            Fun,
            Acc,
            ?READ_OPTIONS)
    end,
    ok = rotating_bloom_filter:record_removal(Bloom, FoldFun).

do_terminate(#internal_state { db = DB,
                               recovery_index = RecoveryIndex,
                               dir = Dir,
                               bloom_filter = Bloom }) ->
    clear_recovery_index(RecoveryIndex, Dir),
    case {eleveldb:close(DB), rotating_bloom_filter:save(Bloom, Dir)} of
        {ok, ok}                 -> ok;
        {{error, Err}, BloomRes} ->
            rabbit_log:error("Unable to stop message store index"
                             " for directory ~p.~nError: ~p~n"
                             "Bloom filter save: ~p~n",
                             [filename:dirname(Dir), Err, BloomRes]),
            error(Err);
        {ok, {error, Err}} ->
            rabbit_log:error("Unable to save a bloom filter for message store index"
                             " for directory ~p.~nError: ~p~n",
                             [filename:dirname(Dir), Err]),
            error(Err)
    end.

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
clear_recovery_index(RecoveryIndex, Dir) ->
    ok = eleveldb:close(RecoveryIndex),
    ok = eleveldb:destroy(recover_index_dir(filename:dirname(Dir)),
                          open_options()).

index_dir(BaseDir) ->
    filename:join(BaseDir, ?INDEX_DIR).

recover_index_dir(BaseDir) ->
    filename:join(BaseDir, ?TMP_RECOVER_INDEX).

init_recovery_index(BaseDir) ->
    RecoverNoFileDir = recover_index_dir(BaseDir),
    rabbit_file:recursive_delete([RecoverNoFileDir]),
    eleveldb:open(RecoverNoFileDir, open_options()).

index_state(Pid) ->
    {ok, DB} = gen_server:call(Pid, reference),
    {ok, Bloom} = gen_server:call(Pid, bloom_filter),
    #index_state{ db = DB,
                  server = Pid,
                  read_options = read_options(),
                  write_options = write_options(),
                  bloom_filter = Bloom}.

decode_val(Val) ->
    binary_to_term(Val).

encode_val(Obj) ->
    term_to_binary(Obj).

update_elements(Old, Update) when is_tuple(Update) ->
    update_elements(Old, [Update]);
update_elements(Old, Updates) when is_list(Updates) ->
    lists:foldl(fun({Index, Val}, Rec) ->
                    erlang:setelement(Index, Rec, Val)
                end,
                Old,
                Updates).

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




