package com.ontotext.kafka.util;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.Objects;

public class ValueUtil {

	private static final StringBuilder updateQueryBuilder = new StringBuilder();

	private ValueUtil() {
	}

	public static RDFFormat getRDFFormat(String format) {
		if (format == null) {
			throw new IllegalArgumentException("RDF format cannot be null");
		}

		String normalized = format.trim().toUpperCase();
		switch (normalized) {
			case "RDFXML":
				return RDFFormat.RDFXML;
			case "NTRIPLES":
				return RDFFormat.NTRIPLES;
			case "TURTLE":
				return RDFFormat.TURTLE;
			case "TURTLESTAR":
				return RDFFormat.TURTLESTAR;
			case "N3":
				return RDFFormat.N3;
			case "TRIX":
				return RDFFormat.TRIX;
			case "TRIG":
				return RDFFormat.TRIG;
			case "TRIGSTAR":
				return RDFFormat.TRIGSTAR;
			case "BINARY":
				return RDFFormat.BINARY;
			case "NQUADS":
				return RDFFormat.NQUADS;
			case "JSONLD":
				return RDFFormat.JSONLD;
			case "NDJSONLD":
				return RDFFormat.NDJSONLD;
			case "RDFJSON":
				return RDFFormat.RDFJSON;
			case "RDFA":
				return RDFFormat.RDFA;
			case "HDT":
				return RDFFormat.HDT;
			default:
				throw new IllegalArgumentException("Invalid RDF format: " + format);
		}
	}

	public static ByteArrayInputStream convertRDFDataToBytes(Object obj) {
		Objects.requireNonNull(obj, "Cannot parse null objects");
		if (obj instanceof byte[]) {
			return new ByteArrayInputStream((byte[]) obj);
		} else {
			return new ByteArrayInputStream(convertValueToString(obj).getBytes(Charset.defaultCharset()));
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
		return String.format(
			"Record: {topic='%s', kafkaPartition=%d, key=%s, keySchema=%s, value=%s, valueSchema=%s, timestamp=%d}",
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

	public static String createRecordUpdateQuery(Object key, String templateId) {
		String templateBinding = convertValueToString(key);

		updateQueryBuilder.setLength(0);
		return updateQueryBuilder.append("PREFIX onto: <http://www.ontotext.com/>\n")
			.append("insert data {\n")
			.append("    onto:smart-update onto:sparql-template <").append(templateId).append(">;\n")
			.append("               onto:template-binding-id <").append(templateBinding).append("> .\n")
			.append("}\n")
			.toString();
	}
}
