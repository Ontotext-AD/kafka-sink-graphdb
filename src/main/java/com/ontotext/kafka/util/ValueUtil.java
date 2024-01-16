package com.ontotext.kafka.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Objects;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueUtil {

	private static final Logger LOG = LoggerFactory.getLogger(ValueUtil.class);

	private ValueUtil() {
	}

	public static RDFFormat getRDFFormat(String format) {
		if (RDFFormat.RDFXML.getDefaultFileExtension().contains(format)) {
			return RDFFormat.RDFXML;
		} else if (RDFFormat.NTRIPLES.getDefaultFileExtension().contains(format)) {
			return RDFFormat.NTRIPLES;
		} else if (RDFFormat.TURTLE.getDefaultFileExtension().contains(format)) {
			return RDFFormat.TURTLE;
		} else if (RDFFormat.TURTLESTAR.getDefaultFileExtension().contains(format)) {
			return RDFFormat.TURTLESTAR;
		} else if (RDFFormat.N3.getDefaultFileExtension().contains(format)) {
			return RDFFormat.N3;
		} else if (RDFFormat.TRIX.getDefaultFileExtension().contains(format)) {
			return RDFFormat.TRIX;
		} else if (RDFFormat.TRIG.getDefaultFileExtension().contains(format)) {
			return RDFFormat.TRIG;
		} else if (RDFFormat.TRIGSTAR.getDefaultFileExtension().contains(format)) {
			return RDFFormat.TRIGSTAR;
		} else if (RDFFormat.BINARY.getDefaultFileExtension().contains(format)) {
			return RDFFormat.BINARY;
		} else if (RDFFormat.NQUADS.getDefaultFileExtension().contains(format)) {
			return RDFFormat.NQUADS;
		} else if (RDFFormat.JSONLD.getDefaultFileExtension().contains(format)) {
			return RDFFormat.JSONLD;
		} else if (RDFFormat.NDJSONLD.getDefaultFileExtension().contains(format)) {
			return RDFFormat.NDJSONLD;
		} else if (RDFFormat.RDFJSON.getDefaultFileExtension().contains(format)) {
			return RDFFormat.RDFJSON;
		} else if (RDFFormat.RDFA.getDefaultFileExtension().contains(format)) {
			return RDFFormat.RDFA;
		} else if (RDFFormat.HDT.getDefaultFileExtension().contains(format)) {
			return RDFFormat.HDT;
		} else {
			throw new IllegalArgumentException("Invalid RDF Format " + format);
		}
	}

	public static Reader convertRDFData(Object obj) {
		Objects.requireNonNull(obj, "Cannot parse null objects");
		if (obj instanceof byte[]) {
			var inputStream = new ByteArrayInputStream((byte[]) obj);
			var reader = new BufferedReader(new InputStreamReader(inputStream));
			StringBuilder content = new StringBuilder();
			String line;

			// Read each line from the BufferedReader
			try {
				var supportsMark = inputStream.markSupported();
				if (supportsMark) {
					inputStream.mark(1024);
				}
				while ((line = reader.readLine()) != null) {
					content.append(line).append("\n");
				}
				if (supportsMark) {
					inputStream.reset();
				}
			} catch (IOException e) {
				throw new RuntimeException("Could not read line\n" + e.getMessage());
			}
			LOG.warn("The byte array record value is\n {}", content);
			return reader;
		} else {
			var stringValue = convertValueToString(obj);
			LOG.warn("The record value is\n {}", stringValue);
			return new StringReader(stringValue);
		}
	}

	public static Resource convertIRIKey(Object obj) {
		return SimpleValueFactory
				.getInstance()
				.createIRI(convertValueToString(obj));
	}

	public static String convertValueToString(Object value) {
		Objects.requireNonNull(value, "Cannot convert value of null objects");
		if (value instanceof byte[]) {
			return new String((byte[]) value);
		} else if (value instanceof String) {
			return (String) value;
		} else {
			throw new DataException("error: no value converter present due to unexpected object type "
					+ value.getClass().getName());
		}
	}

	public static String recordInfo(SinkRecord record) {
		return String.format("Record: {topic='%s', kafkaPartition=%d, key=%s, keySchema=%s, value=%s, valueSchema=%s, timestamp=%d}",
			record.topic(),
			record.kafkaPartition(),
			convertValueToStringNullable(record.key()),
			record.keySchema(),
			convertValueToStringNullable(record.value()),
			record.valueSchema(),
			record.timestamp()
		);
	}

	public static String convertValueToStringNullable(Object obj) {
		return obj == null ? "null" : convertValueToString(obj);
	}

}
