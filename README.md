## ElevelDB message store index for RabbitMQ.

This plugin provides a message store index module for RabbitMQ, based on
[ElevelDB](https://github.com/basho/eleveldb).

The goal of the plugin is to provide "infinite" (limited by disk size only)
message store with constant memory usage.

The plugin uses a rotating bloom filter to detact entries which weren't added,
or were removed from the index. This reduce a throughput for messages that are
consumed before being synced to disk.

Bloom filter is based on [ebloom library](https://github.com/basho/ebloom)

Message store index serves as a synchronization point of a message store process
and a message store GC process. This plugin uses a gen_server process to provide
this synchronization.

### Memory usage

RabbitMQ relies on message store index to keep track of messages reference counting
and locating message position on disk. Each message location record takes approximately
128 bytes.
So 1 million messages in ETS index, will require ~ 120MB of memory,
10 million will require ~ 1GB.

The memory consumed by ElebelDB index depends on several settings.
You can configure `total_leveldb_mem` application environment variable to set
memory limit for LevelDB itself. Increasing the limit can increase the throughput
for large message stores.

Bloom filter will consume 2.28 MB per million messages and it's size should be
configured using `bloom_filter_size` application environment variable.
The default value for this variable is 1 million.

If there are more messages in a message store, false positives rate will increase
and throughput will suffer.

The bloom filter is being rotated when messages are deleted. If number of messages
added since the last rotation is more than 0.6 of size, and number of messages
removed is more than 0.3 of size - it will be rotated to reflect the new set of
messages.

### Configuration

To configure RabbitMQ to use the index module:

```
[{rabbit, [{msg_store_index_module, rabbit_msg_store_eleveldb_index}]}].
```

To set ElevelDB memory limit to 100MB:

```
[{rabbit,   [{msg_store_index_module, rabbit_msg_store_eleveldb_index}]},
 {eleveldb, [{total_leveldb_mem, 104857600}]}].
```

To set ElevelDB memory limit to 100MB and
bloom filter size to 10 million messages (will consume ~ 23MB),
so the total index memory consumption will be ~ 123MB:

```
[{rabbit,   [{msg_store_index_module, rabbit_msg_store_eleveldb_index}]},
 {rabbitmq_msg_store_index_eleveldb, [{bloom_filter_size, 10000000}]},
 {eleveldb, [{total_leveldb_mem, 104857600}]}].
```
