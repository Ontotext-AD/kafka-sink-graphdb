package com.ontotext.kafka;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.ontotext.kafka.util.PropertiesUtil;

public class GraphDBSinkTask extends SinkTask {

	private Map<String, String> properties;

	@Override
	public String version() {
		return PropertiesUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		this.properties = properties;
	}

	@Override
	public void put(Collection<SinkRecord> collection) {

	}

	@Override
	public void stop() {}
}
