package com.ontotext.kafka.service;

import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.util.PropertiesUtil.TEMPLATE_ID;
import static com.ontotext.kafka.util.PropertiesUtil.getProperty;
import static com.ontotext.kafka.util.ValueUtil.convertValueToString;

public class UpdateRecordsProcessor extends SinkRecordsProcessor {
	//todo remove @merge
	long NUMBER_OF_CONNECTION_RETRIES = 5L;
	long DEFERRED_TIME_BETWEEN_RETRIES = 100L;
	String ERROR_TOLERANCE = "all";
	Logger LOGGER = LoggerFactory.getLogger(UpdateRecordsProcessor.class);

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
					updateRecord(record, connection, failedRecords);
				}
				connection.commit();
				return;

			} catch (IOException | RepositoryException e) {
				long startAttempt = System.currentTimeMillis();
				LOGGER.warn("Exception while flushing records " + e.getMessage());

				if (ERROR_TOLERANCE.equalsIgnoreCase("none")) {
					throw new RuntimeException(e);
				}
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

	private void updateRecord(SinkRecord record, RepositoryConnection connection,
							  Set<SinkRecord> failedRecords) throws IOException {
		try {
			if (!failedRecords.contains(record)) {
				String query = getQuery(record);
				connection.prepareUpdate(query)
						.execute();
				connection.add(ValueUtil.convertRDFData(record.value()), format);
			}
		} catch (Exception e) {
			if (ERROR_TOLERANCE.equalsIgnoreCase("none")) {
				throw new RuntimeException(e);
			}
			if (e instanceof IOException) {
				throw new IOException("IOException during updateRecord");
			}
			// Catch records that caused exceptions we can't recover from by retrying the connection
			//todo errorHandler.handleFailingRecord(record, e);
			failedRecords.add(record);
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
