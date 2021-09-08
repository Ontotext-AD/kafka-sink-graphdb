package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.util.ValueUtil;
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
			LOGGER.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private void flush() throws TimeoutException {
		int retryCount = 1;
		Set<SinkRecord> failedRecords = new HashSet<>();
		SinkRecord currentRecord = null;
		do {
			try (RepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				for (SinkRecord record : recordsBatch) {
					currentRecord = record;
					addRecord(record, connection, failedRecords);
				}
				connection.commit();
				return;

			} catch (IOException | RepositoryException e) {
				long startAttempt = System.currentTimeMillis();
				LOGGER.warn("Exception while flushing records " + e.getMessage());

				if (ERROR_TOLERANCE.equalsIgnoreCase("none"))
					throw new RuntimeException(e);

				//retry sending all valid records in the batch until successful or timeout
				if (NUMBER_OF_CONNECTION_RETRIES <= retryCount) {
					String msg = String.format("Tried adding record%s but ran out of attempt retries due to: %s",
							currentRecord == null ? "" : " " + currentRecord,
							e.getMessage());
					throw new TimeoutException(msg);
				}
				while (DEFERRED_TIME_BETWEEN_RETRIES > System.currentTimeMillis() - startAttempt) {
					//do nothing
				}
				LOGGER.info("Retrying connection...");
				retryCount++;
			}
		} while (true);
	}

	private void addRecord(SinkRecord record, RepositoryConnection connection, Set<SinkRecord> failedRecords) throws IOException {
		try {
			if (!failedRecords.contains(record)) {
				connection.add(ValueUtil.convertRDFData(record.value()), format);
			}
		} catch (Exception e) {
			if (ERROR_TOLERANCE.equalsIgnoreCase("none"))
				throw new RuntimeException(e);
			if (e instanceof IOException)
				throw new IOException(e.getMessage());
			// Catch records that caused exceptions we can't recover from by retrying the connection
			errorHandler.handleFailingRecord(record, e);
			failedRecords.add(record);
		}
	}

}