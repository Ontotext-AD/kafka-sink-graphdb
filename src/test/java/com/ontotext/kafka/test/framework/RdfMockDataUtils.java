package com.ontotext.kafka.test.framework;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.Queue;

public class RdfMockDataUtils {

	private RdfMockDataUtils() {
	}

	public static String generateRDFStatements(int quantity) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < quantity; i++) {
			builder.append("<urn:one")
				.append(i)
				.append("> <urn:two")
				.append(i)
				.append("> <urn:three")
				.append(i)
				.append("> . \n");
		}
		return builder.toString();
	}


	public static void generateSinkRecords(Queue<Collection<SinkRecord>> sinkRecords, int recordsSize, int statementsSize) {
		for (int i = 0; i < recordsSize; i++) {
			SinkRecord sinkRecord = new SinkRecord("topic", 0, null, "key", null,
				generateRDFStatements(statementsSize).getBytes(),
				12);
			sinkRecords.add(Collections.singleton(sinkRecord));
		}
	}
}
