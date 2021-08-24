package com.ontotext.kafka.util;

import org.eclipse.rdf4j.rio.RDFFormat;

import com.ontotext.kafka.GraphDBSinkConfig;

public class RDFValueUtil {

	public static void parseRDFData(Object data) {

	}

	public static RDFFormat getRDFFormat() {
		String format = PropertiesUtil.getProperty(GraphDBSinkConfig.RDF_FORMAT);
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
}
