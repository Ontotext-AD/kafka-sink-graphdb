package com.ontotext.kafka.util;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Objects;

public class ValueUtil {

	private static final StringBuilder updateQueryBuilder = new StringBuilder();

	private ValueUtil() {
	}

	public static RDFFormat getRDFFormat(String format) {
		String cleanedFormat = format.trim().replace("\\.", "");
		if (RDFFormat.RDFXML.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.RDFXML;
		} else if (RDFFormat.NTRIPLES.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.NTRIPLES;
		} else if (RDFFormat.TURTLE.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.TURTLE;
		} else if (RDFFormat.TURTLESTAR.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.TURTLESTAR;
		} else if (RDFFormat.N3.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.N3;
		} else if (RDFFormat.TRIX.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.TRIX;
		} else if (RDFFormat.TRIG.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.TRIG;
		} else if (RDFFormat.TRIGSTAR.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.TRIGSTAR;
		} else if (RDFFormat.BINARY.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.BINARY;
		} else if (RDFFormat.NQUADS.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.NQUADS;
		} else if (RDFFormat.JSONLD.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.JSONLD;
		} else if (RDFFormat.NDJSONLD.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.NDJSONLD;
		} else if (RDFFormat.RDFJSON.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.RDFJSON;
		} else if (RDFFormat.RDFA.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.RDFA;
		} else if (RDFFormat.HDT.getDefaultFileExtension().equalsIgnoreCase(cleanedFormat)) {
			return RDFFormat.HDT;
		} else {
			throw new IllegalArgumentException("Invalid RDF Format " + format);
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
