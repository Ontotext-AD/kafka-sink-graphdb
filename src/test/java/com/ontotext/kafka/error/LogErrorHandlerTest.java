package com.ontotext.kafka.error;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

public class LogErrorHandlerTest {

	LogErrorHandler handler = new LogErrorHandler();

/*	@Test
	public void test() {
		SinkRecord record = new SinkRecord("This is a failed record",
				1,
				null,
				"key",
				null,
				"value",
				0L);
		handler.handleFailingRecord(record, new IllegalArgumentException("test ex"));
	}*/
}
