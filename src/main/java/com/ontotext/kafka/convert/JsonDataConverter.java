package com.ontotext.kafka.convert;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.util.Map;

public final class JsonDataConverter extends RdfFormatConverter {

	private final ObjectMapper objectMapper;

	public JsonDataConverter(RDFFormat inputFormat) {
		super(inputFormat, RDFFormat.JSONLD);
		this.objectMapper = new ObjectMapper();
	}

	public <T> T convert(byte[] data, Class<T> cls) {
		try {
			byte[] converted = convertData(data);
			return objectMapper.readValue(converted, cls);
		} catch (DatabindException e) {
			log.error("Could not deserialize data to type {}", cls, e);
		} catch (StreamReadException e) {
			log.error("Caught exception while reading data stream", e);
		} catch (IOException e) {
			log.error("Could not convert data from {} to JSON", getInputFormat(), e);
		}
		return null;
	}

	public Map<String, Object> convert(byte[] data) {
		try {
			byte[] converted = convertData(data);
			return objectMapper.readValue(converted, new TypeReference<>() {
			});
		} catch (DatabindException e) {
			log.error("Could not deserialize data to a generic map", e);
		} catch (StreamReadException e) {
			log.error("Caught exception while reading data stream", e);
		} catch (IOException e) {
			log.error("Could not convert data from {} to JSON", getInputFormat(), e);
		}
		return null;
	}
}
