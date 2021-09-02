package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
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
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SinkRecordsProcessor} implementation that directly flushes RDF data from {@link SinkRecord} values
 * through a {@link RepositoryConnection} to a GraphDB repository.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class AddRecordsProcessor extends SinkRecordsProcessor {

	private static final long TOTAL_RETRY_TIME = 30_000L; //todo set/get in/from property
	private static final long LINGER_BETWEEN_RETRIES = 100L; //todo set/get in/from property

	public AddRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository,
							   RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler);
	}

	@Override
	public void flushRecordUpdates() {
		try {
			if (!recordsBatch.isEmpty()) {//or 'while'?
				Queue<SinkRecord> records = new LinkedBlockingQueue<>(recordsBatch);
				int flushed = records.size();

				flush(records);
				removeFlushedRecords(flushed);
				clearFailed();
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
					addRecord(record, connection);
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

	private void addRecord(SinkRecord record, RepositoryConnection connection) throws IOException {
		try {
			connection.add(ValueUtil.convertRDFData(record.value()), format);
		} catch (RDFParseException | UnsupportedRDFormatException | DataException | RepositoryException e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			catchMalformedRecords(record, e);
		}
	}

}