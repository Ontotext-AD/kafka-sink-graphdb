package com.ontotext.kafka.convert;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ontotext.kafka.graphdb.template.ValueParser;
import com.ontotext.kafka.util.ValueUtil;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

	public static void main(String[] args) throws IOException {
		byte[] data = Files.readAllBytes(Paths.get(args[0]));
		JsonDataConverter converter = new JsonDataConverter(ValueUtil.getRDFFormat(args[1]));
		Map<String, Object> map = converter.convert(data);
		ObjectMapper objectMapper1 = new ObjectMapper();
		System.out.println(objectMapper1.writerWithDefaultPrettyPrinter().writeValueAsString(map));
		for (String key : map.keySet()) {
			Object obj = map.get(key);
			Value value = ValueParser.getValue(obj);
			if (value == null) {
				continue;
			}
			System.out.printf("%s : %s\n", key, value);
		}
	}


}
