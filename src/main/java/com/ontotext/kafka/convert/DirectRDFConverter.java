package com.ontotext.kafka.convert;

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * Simple converter that maintains the record key/value as a byte[]
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class DirectRDFConverter implements Converter {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {}

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		//todo implement
		return new byte[0];
	}

	@Override
	public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
		//todo implement
		return new byte[0];
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		return new SchemaAndValue(null, value);
	}

	@Override
	public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
		return new SchemaAndValue(null, value);
	}
}
