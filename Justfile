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

consume-messages:
    @docker exec -it kafka-server \
        sh -c '/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --from-beginning'


# list current topics
list-topics:
    @docker exec -it kafka-server \
        sh -c '/opt/kafka/bin/kafka-topics.sh bin/kafka-topics.sh --list --bootstrap-server localhost:9092'

docs:
    @zig build-lib -femit-docs src/main.zig

