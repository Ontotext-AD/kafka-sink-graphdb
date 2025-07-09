package com.ontotext.kafka.transformation;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static com.ontotext.kafka.test.framework.RdfMockDataUtils.*;
import static org.assertj.core.api.Assertions.*;

public class AddFieldTransformationTest {

	private RdfTransformation transformation;
	private Map<String, String> config;

	@BeforeEach
	void setUp() {
		transformation = new AddFieldRdfTransformation();
		config = new HashMap<>();
		config.put("subject.iri", "http://example.com/subject");
		config.put("predicate.iri", "http://example.com/predicate");
		config.put("transformation.type", "TIMESTAMP");
		config.put("rdf.format", "nq");
	}

	@Test
	void test_transformation_adds_timestamp_correctly_if_record_value_contains_single_graph() throws IOException {
		transformation.configure(config);
		SinkRecord record = generateSinkRecordWithGraphContext(10);
		SinkRecord transformedRecord = transformation.apply(record);
		Model transformedRecordValue;
		try (InputStream inputStream = new ByteArrayInputStream((byte[]) transformedRecord.value())) {
			transformedRecordValue = Rio.parse(inputStream, "", RDFFormat.NQUADS);
		}
		ValueFactory vf = SimpleValueFactory.getInstance();
		IRI subject = vf.createIRI("http://example.com/subject");
		IRI predicate = vf.createIRI("http://example.com/predicate");
		Resource graphContext = vf.createIRI("urn:graph");
		boolean hasTimestampInGraph = transformedRecordValue.filter(subject, predicate, null, graphContext)
			.stream()
			.anyMatch(statement -> {
				Value obj = statement.getObject();
				return obj instanceof Literal &&
					((Literal) obj).getDatatype().equals(XMLSchema.DATETIME);
			});
		assertThat(hasTimestampInGraph)
			.as("Model should contain a timestamp object for the given subject and predicate in the correct graph")
			.isTrue();
	}

	@Test
	void test_transformation_adds_timestamp_correctly_if_record_value_contains_multiple_graphs() throws IOException {
		transformation.configure(config);
		SinkRecord record = generateSinkRecordWithMultipleGraphContexts(10, 5);
		SinkRecord transformedRecord = transformation.apply(record);
		Model transformedRecordValue;
		try (InputStream inputStream = new ByteArrayInputStream((byte[]) transformedRecord.value())) {
			transformedRecordValue = Rio.parse(inputStream, "", RDFFormat.NQUADS);
		}
		ValueFactory vf = SimpleValueFactory.getInstance();
		IRI subject = vf.createIRI("http://example.com/subject");
		IRI predicate = vf.createIRI("http://example.com/predicate");
		Set<Resource> namedGraphs = transformedRecordValue.contexts().stream()
			.filter(Objects::nonNull) // exclude the default graph
			.collect(Collectors.toSet());
		for (Resource graphContext : namedGraphs) {
			boolean hasTimestampInGraph = transformedRecordValue.filter(subject, predicate, null, graphContext)
				.stream()
				.anyMatch(statement -> {
					Value obj = statement.getObject();
					return obj instanceof Literal &&
						((Literal) obj).getDatatype().equals(XMLSchema.DATETIME);
				});
			assertThat(hasTimestampInGraph)
				.as("Graph <%s> must contain the timestamp triple", graphContext.stringValue())
				.isTrue();
		}
	}

	@Test
	void test_transformation_creates_blank_node_if_subject_iri_is_empty() throws IOException {
		// Set up config such that subject IRI is intentionally blank
		config.put("subject.iri", "");
		transformation.configure(config);
		SinkRecord record = generateSinkRecordWithGraphContext(10);
		SinkRecord transformedRecord = transformation.apply(record);
		Model model;
		try (InputStream inputStream = new ByteArrayInputStream((byte[]) transformedRecord.value())) {
			model = Rio.parse(inputStream, "", RDFFormat.NQUADS);
		}
		ValueFactory vf = SimpleValueFactory.getInstance();
		IRI predicate = vf.createIRI("http://example.com/predicate");
		List<Statement> bnodeStatements = model.stream()
			.filter(st -> st.getSubject() instanceof BNode)
			.filter(st -> st.getPredicate().equals(predicate))
			.filter(st -> {
				Value obj = st.getObject();
				return obj instanceof Literal &&
					((Literal) obj).getDatatype().equals(XMLSchema.DATETIME);
			})
			.collect(Collectors.toList());
		assertThat(bnodeStatements)
			.as("Model should contain a triple with a blank node subject and timestamp object")
			.isNotEmpty();
	}

	@Test
	void test_transformation_fails_if_record_value_is_null() {
		SinkRecord nullValueRecord = new SinkRecord("test", 0, null, "key", null, null, 12);
		assertThatThrownBy(() -> {
			transformation.configure(config);
			transformation.apply(nullValueRecord);
		})
			.isInstanceOf(DataException.class)
			.hasMessageContaining("Record value must not be null");
	}

	@Test
	void test_transformation_fails_if_record_value_invalid_rdf() {
		// This test will log an RDF4J parse error — this is expected since RDF4J logs parser errors by default
		transformation.configure(config);
		SinkRecord invalidRecord = new SinkRecord(null, 0, null, "key", null,
			"urn: graph".getBytes(StandardCharsets.UTF_8), 12);
		assertThatThrownBy(() -> transformation.apply(invalidRecord)).isInstanceOf(DataException.class)
			.hasMessageContaining("Failed to parse RDF from byte[]");
	}

	@Test
	void test_transformation_fails_if_record_value_not_byte_array() {
		transformation.configure(config);
		SinkRecord invalidRecord = new SinkRecord(null, 0, null, "key", null,
			"<urn:test>", 12);
		assertThatThrownBy(() -> transformation.apply(invalidRecord)).isInstanceOf(ConnectException.class)
			.hasMessageContaining("Transformation supports only byte[] record values!");
	}

	@Test
	void test_transformation_fails_if_transformation_type_not_supported() {
		config.put("transformation.type", "Not supported");
		assertThatThrownBy(() -> transformation.configure(config))
			.isInstanceOf(ConfigException.class)
			.hasMessageContaining(
				"Invalid value Not supported for configuration transformation.type: 'Not supported' is not a valid value for TransformationType. Should be one of: [timestamp]");
	}

	@Test
	void test_transformation_fails_if_rdf_format_is_invalid_or_not_supported() {
		config.put("rdf.format", "not supported");
		assertThatThrownBy(() -> transformation.configure(config))
			.isInstanceOf(ConfigException.class)
			.hasMessageContaining(
				"Invalid value not supported for configuration rdf.format: Invalid RDF Format not supported");
	}

	@Test
	void test_transformation_fails_if_predicate_is_emtpy() {
		config.put("predicate.iri", "");
		assertThatThrownBy(() -> transformation.configure(config))
			.isInstanceOf(ConfigException.class)
			.hasMessageContaining("Predicate cannot be null or empty");
	}

	@Test
	void test_transformation_config_validation_creates_error() {
		Map<String, String> connectorConfigs = new HashMap<>();
		String transformationName = "AddFieldTransformation";
		connectorConfigs.put("graphdb.update.rdf.format", "jsonld");
		connectorConfigs.put("transforms." + transformationName + ".rdf.format", "ttl");
		assertThatCode(() -> transformation.validateConfig(transformationName, connectorConfigs)).isInstanceOf(ConfigException.class)
			.hasMessageContaining("Connector RDF format (jsonld) must match Transformation RDF Format (ttl)");
	}

	@Test
	void test_transformation_fails_if_subject_is_null() {
		config.put("subject.iri", null);
		assertThatThrownBy(() -> transformation.configure(config))
			.isInstanceOf(ConfigException.class)
			.hasMessageContaining("subject.iri must be set (use empty string for blank node)");
	}

	@Test
	void test_transformation_fails_if_record_value_does_not_contain_named_graph() {
		config.put("rdf.format", "nt");
		SinkRecord sinkRecordWithNoNamedGraph = generateSinkRecord(10);
		assertThatThrownBy(() -> {
			transformation.configure(config);
			transformation.apply(sinkRecordWithNoNamedGraph);
		})
			.isInstanceOf(ConnectException.class)
			.hasMessageContaining("No named graph found. Transformation only works with named graphs.");
	}
}
