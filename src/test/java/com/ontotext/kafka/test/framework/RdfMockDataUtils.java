package com.ontotext.kafka.test.framework;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static com.ontotext.kafka.test.framework.TestUtils.getRandomString;

public class RdfMockDataUtils {

	private RdfMockDataUtils() {
	}

	public static String generateRDFStatements(int quantity) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < quantity; i++) {
			builder.append(String.format("<urn:%s>", getRandomString(10)))
				.append(String.format("<urn:%s>", getRandomString(10)))
				.append(String.format("<urn:%s>", getRandomString(10)))
				.append(". \n");
		}
		return builder.toString();
	}

	public static String generateRDFStatementsWithGraphContext(int quantity) {
		StringBuilder builder = new StringBuilder();
		String graphContext = "<urn:graph>";
		for (int i = 0; i < quantity; i++) {
			builder.append(String.format("<urn:%s> ", getRandomString(10)))
				.append(String.format("<urn:%s> ", getRandomString(10)))
				.append(String.format("<urn:%s> ", getRandomString(10)))
				.append(graphContext)
				.append(" .\n");
		}
		return builder.toString();
	}

	public static String generateRDFStatementsWithMultipleGraphContexts(int graphContextSize, int graphContextCount) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < graphContextCount; i++) {
			String graphContext = String.format("<urn:%s>", getRandomString(10));
			for (int j = 0; j < graphContextSize; j++) {
				builder.append(String.format("<urn:%s> ", getRandomString(10)))
					.append(String.format("<urn:%s> ", getRandomString(10)))
					.append(String.format("<urn:%s> ", getRandomString(10)))
					.append(graphContext)
					.append(" .\n");
			}

		}
		return builder.toString();
	}

	public static Collection<SinkRecord> generateSinkRecords(int recordsSize, int statementsSize) {
		return generateSinkRecords(recordsSize, statementsSize, "topic", 0, null, "key", null, 12);
	}

	public static Collection<SinkRecord> generateSinkRecords(int recordsSize, int statementsSize, String key) {
		return generateSinkRecords(recordsSize, statementsSize, "topic", 0, null, key, null, 12);
	}

	public static Collection<SinkRecord> generateSinkRecords(int recordsSize, int statementsSize, String topic,
		int partition, Schema keySchema, String key,
		Schema valueSchema, long kafkaOffset) {
		Collection<SinkRecord> records = new ArrayList<>();
		for (int i = 0; i < recordsSize; i++) {
			records.add(generateSinkRecord(statementsSize, topic, partition, keySchema, key, valueSchema, kafkaOffset));
		}
		return records;
	}

	public static SinkRecord generateSinkRecord(int statementsSize) {
		return generateSinkRecord(statementsSize, "topic", 0, null, "key", null, 12);
	}

	public static SinkRecord generateSinkRecord(int statementsSize, String key) {
		return generateSinkRecord(statementsSize, "topic", 0, null, key, null, 12);
	}

	public static SinkRecord generateSinkRecord(int statementsSize, String topic, int partition, Schema keySchema,
		String key, Schema valueSchema,
		long kafkaOffset) {
												String key, Schema valueSchema,
		return new SinkRecord(topic, partition, keySchema, key, valueSchema,
			generateRDFStatements(statementsSize).getBytes(), kafkaOffset);
	}

	public static SinkRecord generateSinkRecordWithGraphContext(int statementsSize) {
		if (statementsSize == 0) {
			return new SinkRecord("topic", 0, null, "<urn:graph>", null,
				null, 12);
		}
		return new SinkRecord("topic", 0, null, "<urn:graph>", null,
			generateRDFStatementsWithGraphContext(statementsSize).getBytes(), 12);
	}
}
