package com.ontotext.kafka.service;

import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.util.PropertiesUtil.TEMPLATE_ID;
import static com.ontotext.kafka.util.PropertiesUtil.getProperty;
import static com.ontotext.kafka.util.ValueUtil.convertValueToString;

public class UpdateRecordsProcessor extends SinkRecordsProcessor {
	//todo remove @merge
	long NUMBER_OF_CONNECTION_RETRIES = 5L;
	long DEFERRED_TIME_BETWEEN_RETRIES = 100L;

	private final StringBuilder sb = new StringBuilder();

	protected UpdateRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
									 Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs);
	}

	@Override
	public void flushRecordUpdates() {
		try {
			if (!recordsBatch.isEmpty()) {
				flush();
				recordsBatch.clear();
			}
		} catch (TimeoutException e) {
			//todo LogErrorHandler.logError(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private void flush() throws TimeoutException {
		long retryCount = 1L;
		Queue<SinkRecord> failedRecords = new ArrayDeque<>(recordsBatch.size());

		do {
			removeFailed(failedRecords);
			long startAttempt = System.currentTimeMillis();
			try (RepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				for (SinkRecord record : recordsBatch) {
					updateRecord(record, connection, failedRecords);
				}
				connection.commit();
				return;

			} catch (Exception e) {
				//todo LogErrorHandler.logWarning("Exception while flushing records " + e.getMessage());
				//retry sending all valid records in the batch until successful or timeout
				while (DEFERRED_TIME_BETWEEN_RETRIES > System.currentTimeMillis() - startAttempt) {
					//do nothing
				}
				if (NUMBER_OF_CONNECTION_RETRIES <= retryCount) {
					throw new TimeoutException("Flushing run out of time due to: " + e.getMessage());
				}
				//todo	LogErrorHandler.logInfo("Retrying connection...");
				retryCount++;
			}
		} while (true);
	}

	private void removeFailed(Queue<SinkRecord> failedRecords) {
		while (!failedRecords.isEmpty()) {
			recordsBatch.remove(failedRecords.poll()); //remove malformed record when retrying the connection
		}
	}

	private void updateRecord(SinkRecord record, RepositoryConnection connection,
							  Queue<SinkRecord> failedRecords) throws IOException {
		try {
			String query = getQuery(record);
			connection.prepareUpdate(query)
					.execute();
			connection.add(ValueUtil.convertRDFData(record.value()), format);

		} catch (Exception e) {
			if (e instanceof IOException)
				throw new IOException("IOException during updateRecord");
			// Catch records that caused exceptions we can't recover from by retrying the connection
			//todo errorHandler.handleFailingRecord(record, e);
			failedRecords.offer(record);
		}
	}

	private String getQuery(SinkRecord record) throws NullPointerException {
		String templateId = Objects
				.requireNonNull(getProperty(TEMPLATE_ID), "Cannot update with empty templateId");
		String templateBinding = convertValueToString(record.key());

		sb.setLength(0);
		return sb.append("PREFIX onto: <http://www.ontotext.com/>\n")
				.append("insert data {\n")
				.append("    onto:smart-update onto:sparql-template <").append(templateId).append(">;\n")
				.append("               onto:template-binding-id <").append(templateBinding).append("> .\n")
				.append("}\n")
				.toString();
	}

}
