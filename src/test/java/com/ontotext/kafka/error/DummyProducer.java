package com.ontotext.kafka.error;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class DummyProducer implements Producer<String, String> {
	protected final List<SinkRecord> records;
	private static final String TOPIC = "topic";

	public DummyProducer(List<SinkRecord> records) {
		this.records = records;
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
		SinkRecord sr = new SinkRecord(TOPIC, 1, null, record.key(), null, record.value(), 1);
		records.add(sr);

		TopicPartition partition = new TopicPartition(TOPIC, 1);
		RecordMetadata md = new RecordMetadata(partition, 1, 1, 1, 1L, 1, 1);
		callback.onCompletion(md, null);
		return null;
	}

	@Override
	public void initTransactions() {
	}

	@Override
	public void beginTransaction() throws ProducerFencedException {
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) throws ProducerFencedException {
	}

	@Override
	public void commitTransaction() throws ProducerFencedException {
	}

	@Override
	public void abortTransaction() throws ProducerFencedException {
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<String, String> record) {
		return null;
	}

	@Override
	public void flush() {
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return null;
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return null;
	}

	@Override
	public void close() {
	}

	@Override
	public void close(Duration timeout) {
	}
}
