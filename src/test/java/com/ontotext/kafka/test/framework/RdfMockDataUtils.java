package com.ontotext.kafka.test.framework;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collection;

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


	public static Collection<SinkRecord> generateSinkRecords(int recordsSize, int statementsSize) {
		Collection<SinkRecord> records = new ArrayList<>();
		for (int i = 0; i < recordsSize; i++) {
			records.add(generateSinkRecord(statementsSize));
		}
		return records;
	}

	public static SinkRecord generateSinkRecord(int statementsSize) {
		return new SinkRecord("topic", 0, null, "key", null,
			generateRDFStatements(statementsSize).getBytes(), 12);
	}
}
