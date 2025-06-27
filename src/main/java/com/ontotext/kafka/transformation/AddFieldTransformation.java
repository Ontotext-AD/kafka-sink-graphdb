package com.ontotext.kafka.transformation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ontotext.kafka.util.ValueUtil;

/**
 * Executes preconfigured actions on the record value.
 */
public class AddFieldTransformation implements Transformation<SinkRecord> {
	private static final Logger log = LoggerFactory.getLogger(AddFieldTransformation.class);
	private static final ValueFactory vf = SimpleValueFactory.getInstance();
	private static final ConfigDef CONFIG_DEF = new ConfigDef()
		.define("predicate.iri",
			ConfigDef.Type.STRING,
			ConfigDef.Importance.HIGH,
			"The IRI of the RDF predicate where the timestamp will be inserted.")
		.define(
			"rdf.format",
			ConfigDef.Type.STRING,
			ConfigDef.Importance.HIGH,
			"The RDF format in which the record value is")
		.define(
			"transformation.type",
			ConfigDef.Type.STRING,
			ConfigDef.Importance.HIGH,
			"The type of transformation to be executed on the record");
	private String predicate;
	private RDFFormat rdfFormat;
	private TransformationType transformation;

	@Override
	public SinkRecord apply(SinkRecord sinkRecord) {
		Model model = convertToRDF(sinkRecord.value());
		log.trace("Converted record to rdf");
		executeTransformation(model);
		log.trace("Transformation executed on record");
		byte[] updatedValue = convertToBytes(model);
		log.trace("Record RDF converted back to byte array");
		return sinkRecord.newRecord(
			sinkRecord.topic(),
			sinkRecord.kafkaPartition(),
			sinkRecord.keySchema(),
			sinkRecord.key(),
			sinkRecord.valueSchema(),
			updatedValue,
			sinkRecord.kafkaOffset()
		);
	}

	/**
	 * Converts a given record value to its corresponding RDF format.
	 * @param recordValue record value in byte[]
	 * @return the RDF model of the record value
	 */
	private Model convertToRDF(Object recordValue) {
		Objects.requireNonNull(recordValue, "Record value must not be null");
		if (recordValue instanceof byte[]) {
			try (InputStream is = new ByteArrayInputStream((byte[]) recordValue)) {
				return Rio.parse(is, "", rdfFormat);
			} catch (RDFParseException | RDFHandlerException | IOException e) {
				throw new DataException("Failed to parse RDF from byte[]", e);
			}
		} else {
			throw new ConnectException("Transformation works only with byte[] record value");
		}
	}

	/**
	 * Executes the configured transformation on the record value
	 * @param model the record value in RDF.
	 */
	private void executeTransformation(Model model) {
		BNode blankNode = vf.createBNode();
		IRI predicate = vf.createIRI(this.predicate);
		Literal timestamp = vf.createLiteral(transformation.value.get(), vf.createIRI(transformation.dataType));
		model.add(blankNode, predicate, timestamp);
	}

	/**
	 * Converts the RDF back to a byte[]
	 * @param model the RDF model
	 * @return RDF model to byte[]
	 */
	private byte[] convertToBytes(Model model) {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			Rio.write(model, out, this.rdfFormat);
			return out.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException("Failed to serialize RDF Model to byte[]", e);
		}
	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void close() {
	}

	@Override
	public void configure(Map<String, ?> map) {
		// Add validation for converter
		SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
		// change to path
		this.predicate = config.getString("predicate.iri");
		if (this.predicate == null || this.predicate.trim().isEmpty()) {
			throw new ConfigException("Predicate IRI cannot be null or empty");
		}
		try {
			this.rdfFormat = ValueUtil.getRDFFormat(config.getString("rdf.format"));
		} catch (IllegalArgumentException e) {
			throw new ConfigException("Invalid Transformation RDF Format: " + e.getMessage());
		}
		try {
			this.transformation = TransformationType.valueOf(config.getString("transformation.type").toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new ConfigException("Invalid transformation.type value. Must be one of: " +
				Arrays.toString(TransformationType.values()));
		}
	}

	/**
	 * Defines the types of transformations that can be done on the record.
	 * Currently, the only type of supported transformation is TIMESTAMP which adds a timestamp to the record value.
	 */
	private enum TransformationType {
		TIMESTAMP("http://www.w3.org/2001/XMLSchema#dateTime", () -> Instant.now().toString());

		private final String dataType;
		private final Supplier<String> value;

		TransformationType(String type, Supplier<String> value) {
			this.dataType = type;
			this.value = value;
		}
	}
}
