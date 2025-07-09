package com.ontotext.kafka.transformation;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.util.EnumValidator;
import com.ontotext.kafka.util.RDFFormatValidator;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Executes preconfigured transformations on the record value. Currently, only the byte[] format is supported for the
 * record value.
 */
public class AddFieldRdfTransformation extends RdfTransformation {
	private static final Logger log = LoggerFactory.getLogger(AddFieldRdfTransformation.class);
	private static final ValueFactory VF = SimpleValueFactory.getInstance();
	public static final String SUBJECT_IRI = "subject.iri";
	public static final String PREDICATE_IRI = "predicate.iri";
	public static final String RDF_FORMAT = "rdf.format";
	public static final String DEFAULT_RDF_TYPE = "jsonld";
	public static final String DEFAULT_TRANSFORMATION_TYPE = "TIMESTAMP";
	private static final ConfigDef CONFIG_DEF = new ConfigDef()
		.define(
			SUBJECT_IRI,
			ConfigDef.Type.STRING,
			ConfigDef.Importance.HIGH,
			"The IRI of the RDF subject where the timestamp will be inserted. If left empty, a blank node will be created.")
		.define(PREDICATE_IRI,
			ConfigDef.Type.STRING,
			ConfigDef.Importance.HIGH,
			"The IRI of the RDF predicate where the timestamp will be inserted.")
		.define(
			RDF_FORMAT,
			ConfigDef.Type.STRING,
			DEFAULT_RDF_TYPE,
			new RDFFormatValidator(),
			ConfigDef.Importance.HIGH,
			"The RDF format in which the record value is.")
		.define(
			"transformation.type",
			ConfigDef.Type.STRING,
			DEFAULT_TRANSFORMATION_TYPE,
			new EnumValidator(TransformationType.class),
			ConfigDef.Importance.HIGH,
			"The type of transformation to be executed on the record value.");
	private String subjectIRI;
	private String predicateIRI;
	private RDFFormat rdfFormat;
	private TransformationType transformation;

	@Override
	public SinkRecord apply(SinkRecord sinkRecord) {
		Model model = convertToRDF(sinkRecord.value());
		log.debug("Converted record value to rdf.");
		executeTransformation(model);
		log.debug("Transformation applied on record value.");
		byte[] updatedValue = convertToBytes(model);
		log.debug("Record RDF converted back to byte array.");
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
	 *
	 * @param recordValue record value in byte[]
	 * @return the RDF model of the record value
	 */
	private Model convertToRDF(Object recordValue) {
		if (recordValue == null) {
			log.error("Record value must not be null.");
			throw new DataException("Record value must not be null.");
		}
		if (recordValue instanceof byte[]) {
			try (InputStream is = new ByteArrayInputStream((byte[]) recordValue)) {
				return Rio.parse(is, "", rdfFormat);
			} catch (RDFParseException | RDFHandlerException | IOException e) {
				log.error("Error while converting record value to RDF.", e);
				throw new DataException("Failed to parse RDF from byte[]", e);
			}
		} else {
			log.error("Record value must be of type byte[].");
			throw new ConnectException("Transformation supports only byte[] record values!");
		}
	}

	/**
	 * Executes the configured transformation on the record value.
	 *
	 * @param model the record value in RDF.
	 */
	private void executeTransformation(Model model) {
		Resource subject = createSubject();
		IRI predicate = VF.createIRI(this.predicateIRI);
		Literal transformation = VF.createLiteral(this.transformation.value.get(),
			VF.createIRI(this.transformation.dataType));
		Set<Resource> namedContexts = model.contexts().stream()
			.filter(Objects::nonNull) // exclude the default graph
			.collect(Collectors.toSet());
		if (!namedContexts.isEmpty()) {
			for (Resource graphContext : namedContexts) {
				model.add(subject, predicate, transformation, graphContext);
			}
		} else {
			log.error("Record value must contain named graph.");
			throw new ConnectException("No named graph found. Transformation only works with named graphs.");
		}
	}

	/**
	 * Creates the subject for the inserted field.
	 *
	 * @return created subject
	 */
	private Resource createSubject() {
		if (this.subjectIRI.isEmpty()) {
			return VF.createBNode();
		} else if (this.subjectIRI.startsWith("_:")) {
			return VF.createBNode(this.subjectIRI.substring(2));
		} else {
			return VF.createIRI(this.subjectIRI);
		}
	}

	/**
	 * Converts the RDF back to a byte[].
	 *
	 * @param model the RDF model
	 * @return RDF model to byte[]
	 */
	private byte[] convertToBytes(Model model) {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			Rio.write(model, out, this.rdfFormat);
			return out.toByteArray();
		} catch (IOException e) {
			log.error("Error while converting updated record value to byte[].", e);
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
		SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
		this.subjectIRI = config.getString("subject.iri");
		if (this.subjectIRI == null) {
			log.error("Subject IRI is null.");
			throw new ConfigException("subject.iri must be set (use empty string for blank node)");
		}
		this.predicateIRI = config.getString("predicate.iri");
		if (StringUtils.isEmpty(this.predicateIRI)) {
			log.error("Predicate IRI is empty.");
			throw new ConfigException("Predicate cannot be null or empty");
		}
		this.rdfFormat = ValueUtil.getRDFFormat(config.getString("rdf.format"));
		this.transformation = TransformationType.valueOf(config.getString("transformation.type").toUpperCase());
	}

	/**
	 * Checks if there is an RDF format mismatch between Sink Connector and AddFieldTransformation
	 * Checks for equality based on String value (assuming that no two format strings point to the same RdfFormat)
	 *
	 * @param connectorConfigs the config of the Sink Connector
	 * @return a list containing an error if there is RDF format mismatch
	 */
	@Override
	public void validateConfig(String transformationName, final Map<String, String> connectorConfigs) throws ConfigException {
		String connectorConfigRdfFormat = connectorConfigs.get(GraphDBSinkConfig.RDF_FORMAT);
		String transformFormatKey = String.format("transforms.%s.%s", transformationName, AddFieldRdfTransformation.RDF_FORMAT);
		String transformationConfigRdfFormat = connectorConfigs.get(transformFormatKey);
		if (!StringUtils.equals(transformationConfigRdfFormat, connectorConfigRdfFormat)) {
			throw new ConfigException(String.format("Connector RDF format (%s) must match Transformation RDF Format (%s)", connectorConfigRdfFormat, transformationConfigRdfFormat));
		}
	}

	/**
	 * Defines the types of transformations that can be applied on the record value.
	 * Currently, the only supported type of transformation is TIMESTAMP which adds a timestamp to the record value.
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
