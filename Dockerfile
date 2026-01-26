FROM maven:3-openjdk-11-slim AS builder

COPY ./ /tmp/kafka-sink-graphdb/

WORKDIR /tmp/kafka-sink-graphdb

RUN \
  apt-get update && apt-get install unzip && \
  mvn -U -B -DskipTests clean package && \
  unzip /tmp/kafka-sink-graphdb/target/kafka-sink-graphdb-plugin.zip -d target/

# Final stage - kafka sink worker
FROM confluentinc/cp-kafka-connect:7.8.0

COPY --from=builder /tmp/kafka-sink-graphdb/target/kafka-sink-graphdb /usr/share/java/kafka-sink-graphdb

# Add Mapped Diagnostic Context (MDC) to loggers (%X)
RUN sed -i -e 's/log4j\.appender\.stdout\.layout\.ConversionPattern \=.*/log4j.appender.stdout.layout.ConversionPattern = [%d] %p %m (%c:%L) [%X]%n/g' /etc/kafka/connect-log4j.properties
