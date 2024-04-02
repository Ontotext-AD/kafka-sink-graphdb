package com.ontotext.kafka.convert;

import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

public class RDFFromAvroConverter implements Converter {
	@Override
	public void configure(Map<String, ?> map, boolean b) {

	}

	@Override
	public byte[] fromConnectData(String s, Schema schema, Object o) {
		return new byte[0];
	}

	@Override
	public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
		return Converter.super.fromConnectData(topic, headers, schema, value);
	}

	@Override
	public SchemaAndValue toConnectData(String s, byte[] bytes) {
		return null;
	}

	@Override
	public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
		return Converter.super.toConnectData(topic, headers, value);
	}
}
