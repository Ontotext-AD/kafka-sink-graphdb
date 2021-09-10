package com.ontotext.kafka.service;

import com.ontotext.kafka.util.PropertiesUtil;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.util.PropertiesUtil.TEMPLATE_ID;
import static com.ontotext.kafka.util.PropertiesUtil.getProperty;
import static com.ontotext.kafka.util.ValueUtil.convertValueToString;

public class UpdateRecordsProcessor extends SinkRecordsProcessor {

	private final long LINGER_BETWEEN_RETRIES = 100L;
	private final long TOTAL_RETRY_TIME = 30_000L;

	protected UpdateRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
									 Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs);
	}

	@Override
	protected void flushRecordUpdates() {
		try {
			if (!recordsBatch.isEmpty()) {//or 'while'?
				Queue<SinkRecord> records = new LinkedBlockingQueue<>(recordsBatch);
				int flushed = records.size();

				flush(records);
				//removeFlushedRecords(flushed);
				//clearFailed();
			}
		} catch (TimeoutException e) {
			//todo handle timeout?
			throw new RuntimeException(e);
		}
	}

	private void flush(Queue<SinkRecord> records) throws TimeoutException {
		long startFlush = System.currentTimeMillis();

		do {
			long startAttempt = System.currentTimeMillis();
			try (RepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				for (SinkRecord record : records) {
					updateRecord(record, connection);
				}
				connection.commit();
				return;

			} catch (IOException | RepositoryException e) {
				//retry sending all records in the batch until successful or timeout
				while (LINGER_BETWEEN_RETRIES < System.currentTimeMillis() - startAttempt) {
					//do nothing
				}
				if (TOTAL_RETRY_TIME > System.currentTimeMillis() - startFlush) {
					throw new TimeoutException("Flushing run out of time due to: " + e.getMessage());
				}
			}
		} while (true);
	}

	private void updateRecord(SinkRecord record, RepositoryConnection connection) throws IOException {
		try {
			String query = getQuery(record);
			connection.prepareUpdate(query)
					.execute();

			connection.add(ValueUtil
					.convertRDFData(record.value()), format);

		} catch (UpdateExecutionException | NullPointerException | RDFParseException |
				UnsupportedRDFormatException | DataException | RepositoryException e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			//catchMalformedRecords(record, e);
		}
	}

	private String getQuery(SinkRecord record) {
		String templateId = Objects.requireNonNull(getProperty(TEMPLATE_ID), "Cannot update with empty templateId");
		String templateBinding = convertValueToString(record.key());

		return "PREFIX onto: <http://www.ontotext.com/>\n" +
				"insert data {\n" +
				"    onto:smart-update onto:sparql-template <" + templateId + ">;\n" +
				"               onto:template-binding-id <" + templateBinding + "> .\n" +
				"}\n";
	}

/*

	private void updateRecord(SinkRecord record, RepositoryConnection connection) throws IOException {
		try {
			String query = getQuery(record);
			connection.prepareUpdate(query)
					.execute();

		} catch (UpdateExecutionException | NullPointerException | RDFParseException |
				UnsupportedRDFormatException | DataException | RepositoryException e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			//catchMalformedRecords(record, e);
		}
	}

	private String getQuery(SinkRecord record) {
		String templateId = Objects.requireNonNull(getProperty(TEMPLATE_ID), "Cannot update with empty templateId");

		String templateBinding = convertValueToString(record.key());
		String recordRDFData = convertValueToString(record.value());

		return "PREFIX onto: <http://www.ontotext.com/>\n" +
				"insert data {\n" +
				"    onto:smart-update onto:sparql-template <" + templateId + ">;\n" +
				"               onto:template-binding-id <" + templateBinding + "> .\n" +
				recordRDFData +
				"}\n";
	}
*/

}
