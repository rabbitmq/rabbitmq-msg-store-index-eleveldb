PROJECT = rabbitmq_msg_store_index_eleveldb
PROJECT_DESCRIPTION = RabbitMQ message store index based on eleveldb

DEPS = rabbit_common rabbit eleveldb

dep_eleveldb_commit = 2.0.34

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
