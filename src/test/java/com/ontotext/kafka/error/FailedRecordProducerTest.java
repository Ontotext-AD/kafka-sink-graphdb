package com.ontotext.kafka.error;

import com.ontotext.kafka.mocks.DummyProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class FailedRecordProducerTest {

	private List<SinkRecord> records;
	private FailedProducer producer;
	private final SinkRecord record = new SinkRecord("topic", 1, null, null, null, "value", 1);

	@BeforeEach
	public void setup() {
		records = new ArrayList<>();
	}

	@Test
	@DisplayName("Test Producer should send records")
	@Timeout(5)
	public void testProducerReturningRecords() {
		producer = new FailedRecordProducer(new DummyProducer(records));
		producer.returnFailed(record);

		Assertions.assertEquals(1, records.size());
	}

	@Test
	@DisplayName("Test Producer should log error")
	@Timeout(5)
	public void testProducerThrowingException() {
		producer = new FailedRecordProducer(throwingProducer());
		producer.returnFailed(record);
		producer.returnFailed(record);

		Assertions.assertEquals(0, records.size());
	}

	private DummyProducer throwingProducer() {
		return new DummyProducer(new ArrayList<>()) {

			@Override
			public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
				callback.onCompletion(null, new IOException());
				return null;
			}
		};
	}

}
