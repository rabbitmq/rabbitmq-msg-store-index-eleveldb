## ElevelDB Message Store Index for RabbitMQ

This plugin provides a message store index implementation (module) for RabbitMQ, based on
[ELevelDB](https://github.com/basho/eleveldb).

The goal of the plugin is to provide "infinite" (limited by disk size only)
message store index with constant memory usage.
Default message store index implementation in RabbitMQ is RAM only.


## Supported RabbitMQ Versions

This plugin targets RabbitMQ 3.7.


## Installation

There are no binary builds of this plugin yet. Build it from source
[like any other plugin](http://www.rabbitmq.com/plugin-development.html).


## Configuration

To configure RabbitMQ to use the index module:

``` erlang
[{rabbit, [{msg_store_index_module, rabbit_msg_store_eleveldb_index}]}].
```

To set ElevelDB memory limit to 100MB:

``` erlang
[{rabbit,   [{msg_store_index_module, rabbit_msg_store_eleveldb_index}]},
 {eleveldb, [{total_leveldb_mem, 104857600}]}].
```

To set ElevelDB memory limit to 100MB and
bloom filter size to 10 million messages (will consume ~ 23MB),
so the total index memory consumption will be ~ 123MB:

``` erlang
[{rabbit, [{msg_store_index_module, rabbit_msg_store_eleveldb_index}]},
 {rabbitmq_msg_store_index_eleveldb, [{bloom_filter_size, 10000000}]},
 {eleveldb, [{total_leveldb_mem, 104857600}]}].
```


LevelDB supports several [configuration options](https://github.com/google/leveldb/blob/master/include/leveldb/options.h),
which can be configured for the index:

``` erlang
[{rabbit, [{msg_store_index_module, rabbit_msg_store_eleveldb_index}]},
 {rabbitmq_msg_store_index_eleveldb, [
    % Set the write buffer size to 50MB to have less disk flushes and bigger
    % initial file size.
    {open_options, [{write_buffer_size, 52428800}]},
    % Will verify all read data. Can use more CPU and make reads slower.
    % False by default.
    {read_options, [{verify_checksums, true}]},
    % Will sync all writes to disk.
    % Disabled by default, because index is being recalculated if message store
    % wasn't stopped gracefully.
    {write_options, [{sync, true}]}
    ]}].
```


## Implementation Details

The plugin uses a rotating bloom filter to detect entries which weren't added,
or were removed from the index. This reduce a throughput for messages that are
consumed before being synced to disk.

Bloom filter is based on [ebloom library](https://github.com/basho/ebloom)

Message store index serves as a synchronization point of a message store process
and a message store GC process. This plugin uses a gen_server process to provide
this synchronization.

### Memory Usage

RabbitMQ relies on message store index to keep track of messages reference counting
and locating message position on disk. Each message location record takes approximately
128 bytes.
This means that 1 million messages in the default ETS-backed index will consume ~ 120MB of memory,
10 million will require a little over 1GB, and so on.

The amount of memory consumed by the ElevelDB-based index depends on
several settings.  You can configure `total_leveldb_mem` application
environment variable to set memory limit for LevelDB
itself. Increasing the limit can increase the throughput for large
message stores.

#### Bloom Filters

A pair of Bloom filters used by this implementation will consume 2.28
MB per million messages and its size should be configured using
`bloom_filter_size` application environment variable.  The default
value for this variable is 1 million.

**If there are more messages in a message store, false positives rate will increase
and throughput will drop.**

The effective bloom filter is being rotated when messages are
deleted. If the number of messages added since the last rotation is
higher than 60% of prediceted size, and number of messages removed is
higher than 30% of that size - the filter will be rotated to reflect
the new set of messages.


## LICENSE

See the LICENSE file.

## Copyright

(c) Pivotal Software Inc., 2007-2018.
