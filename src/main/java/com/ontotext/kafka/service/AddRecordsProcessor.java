package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.Operation;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SinkRecordsProcessor} implementation that directly flushes RDF data from {@link SinkRecord} values
 * through a {@link RepositoryConnection} to a GraphDB repository.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class AddRecordsProcessor extends SinkRecordsProcessor implements Operation<Object> {


	public AddRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository,
							   RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler);
	}

	@Override
	public void flushRecordUpdates() {
		failedRecords.clear();
		if (!recordsBatch.isEmpty() &&
				errorHandler.handleRetry(this) == null) {

			LOGGER.error("Flushing run out of attempts");
			throw new RuntimeException("Flushing run out of attempts");
		}
	}

	@Override
	public Object call() throws RetriableException {
		try (RepositoryConnection connection = repository.getConnection()) {
			connection.begin();
			for (SinkRecord record : recordsBatch) {
				addRecord(record, connection);
			}
			connection.commit();
			recordsBatch.clear();
			failedRecords.clear();
			return new Object();

		} catch (RepositoryException e) {
			throw new RetriableException(e);
		}
	}

	private void addRecord(SinkRecord record, RepositoryConnection connection) {
		try {
			if (!failedRecords.contains(record)) {
				connection.add(ValueUtil.convertRDFData(record.value()), format);
			}
		} catch (Exception e) {
			if (e instanceof IOException)
				throw new RetriableException(e.getMessage());
			// Catch records that caused exceptions we can't recover from by retrying the connection
			failedRecords.add(record);
			errorHandler.handleFailingRecord(record, e);
		}
	}

}