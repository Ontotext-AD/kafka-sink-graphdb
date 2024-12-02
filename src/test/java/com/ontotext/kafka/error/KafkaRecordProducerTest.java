package com.ontotext.kafka.error;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class KafkaRecordProducerTest {

	private static final String TOPIC = "topic";
	private List<SinkRecord> records;
	private KafkaRecordProducer producer;
	private final SinkRecord record = new SinkRecord(TOPIC, 1, null, "key", null, "value", 1);

	@BeforeEach
	public void setup() {
		records = new ArrayList<>();
	}

	@Test
	@DisplayName("Test Producer should send records")
	@Timeout(5)
	public void testProducerReturningRecords() {
		Producer<String, String> mock = mock(Producer.class);
		doAnswer(invocation -> {
			ProducerRecord<String, String> record = invocation.getArgument(0);
			Callback callback = invocation.getArgument(1, Callback.class);

			SinkRecord sr = new SinkRecord(TOPIC, 1, null, record.key(), null, record.value(), 1);
			records.add(sr);
			TopicPartition partition = new TopicPartition(TOPIC, 1);
			RecordMetadata md = new RecordMetadata(partition, 1, 1, 1, 1L, 1, 1);
			callback.onCompletion(md, null);
			return null;
		}).when(mock).send(any(ProducerRecord.class), any(Callback.class));

		producer = new KafkaRecordProducer(mock);
		producer.returnFailed(record);

		assertThat(records).hasSize(1);
	}

	@Test
	@DisplayName("Test Producer should log error")
	@Timeout(5)
	public void testProducerThrowingException() {
		Producer<String, String> mock = mock(Producer.class);
		doAnswer(invocation -> {
			Callback callback = invocation.getArgument(1, Callback.class);
			callback.onCompletion(null, new IOException());
			return null;
		}).when(mock).send(any(ProducerRecord.class), any(Callback.class));

		producer = new KafkaRecordProducer(mock);
		producer.returnFailed(record);
		producer.returnFailed(record);

		Assertions.assertEquals(0, records.size());
	}
}
