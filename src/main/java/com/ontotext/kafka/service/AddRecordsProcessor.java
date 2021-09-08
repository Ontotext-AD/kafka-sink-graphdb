package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.error.LogErrorHandler;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SinkRecordsProcessor} implementation that directly flushes RDF data from {@link SinkRecord} values
 * through a {@link RepositoryConnection} to a GraphDB repository.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class AddRecordsProcessor extends SinkRecordsProcessor {

	public AddRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository,
							   RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler);
	}

	@Override
	public void flushRecordUpdates() {
		try {
			if (!recordsBatch.isEmpty()) {
				flush();
				recordsBatch.clear();
			}
		} catch (TimeoutException e) {
			LogErrorHandler.logError(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private void flush() throws TimeoutException {
		long retryCount = 1L;
		Queue<SinkRecord> failedRecords = new ArrayDeque<>(recordsBatch.size());

		do {
			if (!failedRecords.isEmpty()) {
				removeFailed(failedRecords);
			}

			long startAttempt = System.currentTimeMillis();
			try (RepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				for (SinkRecord record : recordsBatch) {
					addRecord(record, connection, failedRecords);
				}
				connection.commit();
				return;

			} catch (IOException | RepositoryException e) {
				LogErrorHandler.logWarning("Exception while flushing records " + e.getMessage());
				//retry sending all valid records in the batch until successful or timeout
				while (DEFERRED_TIME_BETWEEN_RETRIES > System.currentTimeMillis() - startAttempt) {
					//do nothing
				}
				if (NUMBER_OF_CONNECTION_RETRIES <= retryCount) {
					throw new TimeoutException("Flushing run out of time due to: " + e.getMessage());
				}
				LogErrorHandler.logInfo("Retrying connection...");
				retryCount++;
			}
		} while (true);
	}

	private void addRecord(SinkRecord record, RepositoryConnection connection, Queue<SinkRecord> failedRecords) throws IOException {
		try {
			connection.add(ValueUtil.convertRDFData(record.value()), format);
		} catch (NullPointerException | RDFParseException | UnsupportedRDFormatException | DataException | RepositoryException e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			errorHandler.handleFailingRecord(record, e);
			failedRecords.offer(record);
		}
	}

	private void removeFailed(Queue<SinkRecord> failedRecords) {
		while (failedRecords.peek() != null)
			recordsBatch.remove(failedRecords.poll()); //remove malformed record when retrying the connection
	}
}