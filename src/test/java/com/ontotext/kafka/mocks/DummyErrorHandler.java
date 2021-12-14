package com.ontotext.kafka.mocks;

import com.ontotext.kafka.error.ErrorHandler;
import org.apache.kafka.connect.sink.SinkRecord;

public class DummyErrorHandler implements ErrorHandler {
	@Override
	public void handleFailingRecord(SinkRecord record, Throwable ex) {
	}

	@Override
	public boolean isTolerable() {
		return false;
	}
}
