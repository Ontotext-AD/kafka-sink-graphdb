FROM maven:3-openjdk-11-slim AS builder

COPY ./ /tmp/kafka-sink-graphdb/

WORKDIR /tmp/kafka-sink-graphdb

RUN \
  apt-get update && apt-get install unzip && \
  mvn -B clean package -DskipTests=true && \
  unzip /tmp/kafka-sink-graphdb/target/kafka-sink-graphdb-plugin.zip -d target/

# Final stage - kafka sink worker
FROM confluentinc/cp-kafka-connect:7.6.1

COPY --from=builder /tmp/kafka-sink-graphdb/target/kafka-sink-graphdb /usr/share/java/kafka-sink-graphdb
