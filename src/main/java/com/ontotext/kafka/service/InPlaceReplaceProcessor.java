package com.ontotext.kafka.service;


import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;


import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.OperationHandler;
import com.ontotext.kafka.util.PropertiesUtil;
import com.ontotext.kafka.util.ValueUtil;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleIRI;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InPlaceReplaceProcessor extends SinkRecordsProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(InPlaceReplaceProcessor.class);
	private final String context;
	private final String mapping;
	private final SchemaRegistryClient schemaRegistry;

	public InPlaceReplaceProcessor(Queue<Collection<SinkRecord>> sinkRecords,
								   AtomicBoolean shouldRun,
								   Repository repository,
								   RDFFormat format, int batchSize, long timeoutCommitMs,
								   ErrorHandler errorHandler, OperationHandler operator,
								   String context, String mapping,
								   SchemaRegistryClient schemaRegistry) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler,
			operator);
		this.context = context;
		this.mapping = mapping;
		if (schemaRegistry == null) {
			this.schemaRegistry = SchemaRegistryClientFactory.newClient(Collections.singletonList(
					PropertiesUtil.getProperty(SCHEMA_REGISTRY_URL_CONFIG)), 1,
				Collections.singletonList(new AvroSchemaProvider()), Collections.emptyMap(),
				Collections.emptyMap());
		} else {
			this.schemaRegistry = schemaRegistry;
		}
	}

	private static void cleanOldInstances(RepositoryConnection connection, Model incomingModel,
										  Resource rootIri, Resource context) {
		Iterable<Statement> stmnts = incomingModel.getStatements(rootIri, null, null, context);
		stmnts.forEach(statement -> {
			if (statement.getObject().isIRI()) {
				Resource nestedSubject = (Resource) statement.getObject();
				cleanOldInstances(connection, incomingModel, nestedSubject, context);
				connection.remove(nestedSubject, null, null, context);
				connection.remove(statement.getSubject(), statement.getPredicate(),
					statement.getObject(),
					context);
			} else {
				if (statement.getObject().isBNode()) {
					RepositoryResult<Statement> nestedBlanks =
						connection.getStatements(statement.getSubject(), statement.getPredicate(),
							null,
							context);
					if (nestedBlanks != null) {
						nestedBlanks.forEach(blankStatement -> {
							cleanOldInstances(connection,
								(Resource) blankStatement.getObject(), context);
							connection.remove(
								(Resource) blankStatement.getObject(), null, null, context);
						});
					}
				}
				connection.remove(statement.getSubject(), statement.getPredicate(), null, context);
			}
		});
	}

	private static void cleanOldInstances(RepositoryConnection connection, Resource rootIri,
										  Resource context) {
		Iterable<Statement> stmnts = connection.getStatements(rootIri, null, null, context);
		stmnts.forEach(statement -> {
			if (statement.getObject().isIRI()) {
				Resource nestedSubject = (Resource) statement.getObject();
				cleanOldInstances(connection, nestedSubject, context);
				connection.remove(nestedSubject, null, null, context);
				connection.remove(statement.getSubject(), statement.getPredicate(),
					statement.getObject(),
					context);
			} else {
				if (statement.getObject().isBNode()) {
					RepositoryResult<Statement> nestedBlanks =
						connection.getStatements(statement.getSubject(), statement.getPredicate(),
							null,
							context);
					if (nestedBlanks != null) {
						nestedBlanks.forEach(blankStatement -> {
							cleanOldInstances(connection, (Resource) blankStatement.getObject(),
								context);
							connection.remove(
								(Resource) blankStatement.getObject(), null, null, context);
						});
					}
				}
				connection.remove(statement.getSubject(), statement.getPredicate(), null, context);
			}
		});
	}

	@Override
	protected void handleRecord(SinkRecord record, RepositoryConnection connection)
		throws RetriableException {
		long start = System.currentTimeMillis();
		if (LOG.isDebugEnabled()) {
			LOG.debug(record.toString());
		}
		Model incomingModel = null;
		try {
			incomingModel =
				ValueUtil.convertToModel(record.value(), schemaRegistry.getByID(1), context,
					mapping);
		} catch (IOException | URISyntaxException | RestClientException e) {
			throw new RuntimeException(e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Produced RDF model from AVRO schema in {} ms",
				(System.currentTimeMillis() - start));
		}
		try {
			SimpleIRI rootIri = (SimpleIRI) incomingModel.contexts().toArray()[0];
			cleanOldInstances(connection, incomingModel, rootIri, rootIri);
			incomingModel.forEach(connection::add);
		} catch (RepositoryException e) {
			throw new RetriableException("Failed to handle record: " + ValueUtil.recordInfo(record),
				e);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Processed record in {} ms", (System.currentTimeMillis() - start));
		}
	}
}
