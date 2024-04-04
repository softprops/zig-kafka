start-server:
    @docker run -p 9092:9092 apache/kafka:3.7.0

docs:
    @zig build-lib -femit-docs src/main.zig