start-server:
    @docker run --name kafka-server -p 9092:9092 apache/kafka:3.7.0

create-test-topic:
    @docker exec -it kafka-server \
        sh -c '/opt/kafka/bin/kafka-topics.sh bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092'

docs:
    @zig build-lib -femit-docs src/main.zig

