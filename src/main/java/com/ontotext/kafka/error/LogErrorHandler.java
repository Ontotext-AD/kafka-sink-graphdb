package com.ontotext.kafka.error;

import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;

public class LogErrorHandler implements ErrorHandler{

	@Override
	public void handleFailingRecord(SinkRecord record) {

	}

	@Override
	public void handleFailingRecords(Collection<SinkRecord> records) {

	}
}
