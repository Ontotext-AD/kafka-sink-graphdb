package com.ontotext.kafka.error;

import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;

public interface ErrorHandler {

	void handleFailingRecord(SinkRecord record);

	void handleFailingRecords(Collection<SinkRecord> records);
}
