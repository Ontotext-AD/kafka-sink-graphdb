package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operations.GraphDBOperator;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.jetty.util.IO;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;

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

	protected ReplaceGraphProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
									Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs,
									ErrorHandler errorHandler, GraphDBOperator operator) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler, operator);
	}

	@Override
	protected void handleRecord(SinkRecord record, RepositoryConnection connection) {
		try {
			if (!failedRecords.contains(record)) {
				Resource context = ValueUtil.convertIRIKey(record.key());
				connection.clear(context);
				connection.add(ValueUtil.convertRDFData(record.value()), format, context);
			}
		} catch (IOException e) {
			throw new RetriableException(e.getMessage());
		} catch (Exception e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			failedRecords.add(record);
			errorHandler.handleFailingRecord(record, e);
		}
	}

}