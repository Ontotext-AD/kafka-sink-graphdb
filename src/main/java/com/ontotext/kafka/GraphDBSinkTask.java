package com.ontotext.kafka;

import com.ontotext.kafka.service.GraphDBService;
import com.ontotext.kafka.util.PropertiesUtil;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

/**
 * {@link SinkTask} implementation that sends the incoming {@link SinkRecord} messages to {@link GraphDBService}'s
 * queue to be processed.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkTask extends SinkTask {

	@Override
	public String version() {
		return PropertiesUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		// no need to do anything as records are simply added to a concurrent queue
	}

	@Override
	public void put(Collection<SinkRecord> collection) {
		if (collection.isEmpty()) {
			return;
		}
		processRecords(collection);
	}

	@Override
	public void stop() {
		// no need to do anything as records are simply added to a concurrent queue
	}

	private void processRecords(Collection<SinkRecord> collection) {
		try {
			GraphDBService.connectorService().addData(collection);
		} catch (Exception e) {
			throw new RetriableException(e.getMessage());
		}
	}
}