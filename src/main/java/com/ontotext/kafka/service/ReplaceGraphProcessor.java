package com.ontotext.kafka.service;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.GraphDBOperator;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SinkRecordsProcessor} implementation that directly replaces a graph from {@link SinkRecord} key which
 * indicates the IRI of the named graph and value describing the new RDF contents of the named graph
 * through a {@link RepositoryConnection} to a GraphDB repository.
 * <p>
 *
 * @author Denitsa Stoyanova denitsa.stoyanova@ontotext.com
 */
public class ReplaceGraphProcessor extends SinkRecordsProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(ReplaceGraphProcessor.class);

	public ReplaceGraphProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository,
								 GraphDBSinkConfig config) {
		super(sinkRecords, shouldRun, repository, config);
	}

	ReplaceGraphProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository, RDFFormat nquads, int batchSize,
						  long commitTimeout, ErrorHandler errorHandler, GraphDBOperator operator) {
		super(sinkRecords, shouldRun, repository, nquads, batchSize, commitTimeout, errorHandler, operator);
	}

	@Override
	protected void handleRecord(SinkRecord record, RepositoryConnection connection) {
		try {
			LOG.trace("Executing replace graph operation......");
			Resource context = ValueUtil.convertIRIKey(record.key());
			connection.clear(context);
			if (LOG.isTraceEnabled()) {
				LOG.trace("Connection cleared context(IRI): {}", context.stringValue());
			}
			if (record.value() != null) {
				long start = System.currentTimeMillis();
				connection.add(ValueUtil.convertRDFData(record.value()), format, context);
				if (LOG.isTraceEnabled()) {
					LOG.trace("Record info: {}", ValueUtil.recordInfo(record));
					long finish = System.currentTimeMillis();
					LOG.trace("Converted the record and added it to the RDF4J connection for {} ms", finish - start);
				}
			}
		} catch (IOException e) {
			throw new RetriableException(e.getMessage());
		} catch (Exception e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			handleFailedRecord(record, e);
		}
	}

}
