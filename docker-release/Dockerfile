FROM maven:3-openjdk-11-slim AS builder

ARG mavenDir

COPY ${mavenDir}/kafka-sink-graphdb-plugin.zip /tmp

WORKDIR /tmp

RUN \
  apt-get update && apt-get install unzip && \
  unzip kafka-sink-graphdb-plugin.zip -d /tmp

# Final stage - kafka sink worker
FROM confluentinc/cp-kafka-connect:6.2.0

COPY --from=builder /tmp/kafka-sink-graphdb /usr/share/java/kafka-sink-graphdb
