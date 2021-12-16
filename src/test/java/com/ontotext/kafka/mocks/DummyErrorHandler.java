package com.ontotext.kafka.mocks;

import com.ontotext.kafka.error.ErrorHandler;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

public class DummyErrorHandler implements ErrorHandler {

	private final Map<SinkRecord, Throwable> handled = new HashMap<>();

	@Override
	public void handleFailingRecord(SinkRecord record, Throwable ex) {
		handled.put(record, ex);
	}

	@Override
	public boolean isTolerable() {
		return false;
	}

	public int numberOfHandled() {
		return handled.size();
	}

	public <E extends Throwable> boolean hasHandled(Class<E> ex) {
		if (numberOfHandled() == 0) {
			return false;
		}
		for (Throwable t : handled.values()) {
			if (ex.equals(t.getClass())) {
				return true;
			}
		}
		return false;
	}
}
