#!/usr/bin/env bash
# Drives FireMQ's management API with RabbitMQ's own rabbitmqadmin (v1) script.
set -euo pipefail
admin() { python3 /tmp/rabbitmqadmin -H 127.0.0.1 -P 15672 -u guest -p guest "$@"; }
curl -fsSL -o /tmp/rabbitmqadmin \
  https://raw.githubusercontent.com/rabbitmq/rabbitmq-server/v3.13.7/deps/rabbitmq_management/bin/rabbitmqadmin
admin show overview
admin declare exchange name=compat.events type=fanout durable=true
admin declare queue name=compat.audit durable=true
admin declare binding source=compat.events destination=compat.audit
admin publish exchange=compat.events routing_key= payload="hello from rabbitmqadmin"
sleep 0.2
admin list queues name messages | tee /tmp/queues.txt
grep -q "compat.audit" /tmp/queues.txt
admin get queue=compat.audit ackmode=ack_requeue_false | tee /tmp/get.txt
grep -q "hello from rabbitmqadmin" /tmp/get.txt
admin list exchanges name type | grep -q "compat.events"
admin list bindings source destination | grep -q "compat.audit"
admin declare policy name=compat-ttl pattern='^compat\.' definition='{"message-ttl": 60000}' apply-to=queues
admin list policies | grep -q compat-ttl
admin export /tmp/definitions.json
grep -q '"compat.audit"' /tmp/definitions.json
admin delete queue name=compat.audit
admin delete exchange name=compat.events
admin list queues name | (! grep -q "compat.audit")
echo "rabbitmqadmin: all commands succeeded"
