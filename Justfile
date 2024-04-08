# start kafka server (assumes running docker host)
start-server:
    @docker run --name kafka-server -p 9092:9092 apache/kafka:3.7.0

# create a "test" topic
create-test-topic:
    @docker exec -it kafka-server \
        sh -c '/opt/kafka/bin/kafka-topics.sh bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic test --create --partitions 3 --replication-factor 1'

create-topic-message:
    @docker exec -it kafka-server \
        sh -c '/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test --property parse.key=true --property key.separator=:'

# debug messages see the following for formatting options https://github.com/a0x8o/kafka/blob/54eff6af115ee647f60129f2ce6a044cb17215d0/core/src/main/scala/kafka/tools/ConsoleConsumer.scala#L511-L520
consume-messages:
    @docker exec -it kafka-server \
        sh -c '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --property --property print.partition=true --property print.offset=true --from-beginning'


# list current topics
list-topics:
    @docker exec -it kafka-server \
        sh -c '/opt/kafka/bin/kafka-topics.sh bin/kafka-topics.sh --list --bootstrap-server localhost:9092'

docs:
    @zig build-lib -femit-docs src/main.zig

