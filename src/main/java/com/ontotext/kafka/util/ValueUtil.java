package com.ontotext.kafka.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.*;
import org.eclipse.rdf4j.rio.Rio;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueUtil {

	private static final Logger LOG = LoggerFactory.getLogger(ValueUtil.class);
	private static final String AVRO_CONTEXT = PropertiesUtil.getProperty("avro.context");

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
		} else {
			return new StringReader(convertValueToString(obj));
		}
	}

	public static Model convertToModel(Object obj, Schema schema)
		throws IOException, URISyntaxException {
		Objects.requireNonNull(obj, "Cannot parse null objects");
		JSONObject contextJson = null;
		try (FileInputStream fis = new FileInputStream(new File(new URL(AVRO_CONTEXT).toURI()))) {
			contextJson = new JSONObject(IOUtils.toString(fis, StandardCharsets.UTF_8));
		} catch (IOException e) {
			throw new ConfigException(
				"Trying to carry out AVRO conversion, but no mapping exists!");
		}
		ByteArrayInputStream bais = null;
		if (obj instanceof byte[]) {
			bais = new ByteArrayInputStream((byte[]) obj);
		}
		Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
		GenericData.Record avroRecord =
			(GenericData.Record) new GenericDatumReader<>(schema).read(null, decoder);
		JSONObject jsonObject = new JSONObject(new String(avroRecord.toString()));
//		ByteArrayOutputStream out = new ByteArrayOutputStream();
//		DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
//		JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
//
//		writer.write(avroRecord, encoder);
//		encoder.flush();
//
//		out.close();
//		JSONObject jsonObject = new JSONObject(new String(out.toByteArray()));
		if (LOG.isDebugEnabled()) {
			LOG.debug(jsonObject.toString());
		}
		String alias = (String) jsonObject.remove("companyAlias");
		jsonObject.put("@id", alias);
		JSONObject finalObject = new JSONObject();
		finalObject.put("@id", alias);
		for (String key : contextJson.keySet()) {
			finalObject.put(key, contextJson.get(key));
		}
		finalObject.put("@graph", jsonObject);
		return Rio.parse(new ByteArrayInputStream(finalObject.toString().getBytes()),
			RDFFormat.JSONLD);
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
		} else if (value instanceof Struct) {
			return value.toString();
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
