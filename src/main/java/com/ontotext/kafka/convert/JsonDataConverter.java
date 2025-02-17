package com.ontotext.kafka.convert;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.util.ArrayList;
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
		byte[] converted = null;
		try {
			converted = convertData(data);
			ArrayList<Map<String, Object>> res = objectMapper.readValue(converted, new TypeReference<>() {
			});
			if (CollectionUtils.isNotEmpty(res)) {
				return res.get(0);
			}
		} catch (DatabindException e) {
			// Just in case try to deserialize straight to a map without the outer list wrapper.
			log.error("Could not deserialize data to a list of items. Will repeat and try to deserialize as a map instead.");
			try {
				return objectMapper.readValue(converted, new TypeReference<>() {
				});
			} catch (Exception e1) {
				log.error("Could not deserialize data to a map. Cannot proceed", e);
			}
		} catch (StreamReadException e) {
			log.error("Caught exception while reading data stream", e);
		} catch (IOException e) {
			log.error("Could not convert data from {} to JSON", getInputFormat(), e);
		}
		return null;
	}
}
