package com.ontotext.kafka.error;

import org.apache.kafka.connect.sink.SinkRecord;

public interface FailedProducer {
	void returnFailed(SinkRecord record);
}
