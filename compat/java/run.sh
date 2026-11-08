#!/bin/sh
# Downloads the RabbitMQ Java client and runs Compat.java. Needs Java 17+.
set -e
cd "$(dirname "$0")"
mkdir -p lib
[ -f lib/amqp-client.jar ] || curl -fsSL -o lib/amqp-client.jar https://repo1.maven.org/maven2/com/rabbitmq/amqp-client/5.22.0/amqp-client-5.22.0.jar
[ -f lib/slf4j-api.jar ] || curl -fsSL -o lib/slf4j-api.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
exec java -cp lib/amqp-client.jar:lib/slf4j-api.jar Compat.java
