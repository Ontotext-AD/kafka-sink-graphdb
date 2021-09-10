package com.ontotext.kafka.util;

import org.apache.kafka.connect.errors.DataException;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.*;
import java.util.Objects;

public class ValueUtil {

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
			return new BufferedReader(new InputStreamReader(new ByteArrayInputStream((byte[]) obj)));
		} else if (obj instanceof String) {
			return new StringReader((String) obj);
		} else {
			throw new DataException("error: no converter present due to unexpected object type "
					+ obj.getClass().getName());
		}
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

}
