package com.ontotext.kafka.service;

import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.*;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link SinkRecordsProcessor} implementation that directly flushes RDF data from {@link SinkRecord} values
 * through a {@link RepositoryConnection} to a GraphDB repository.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class AddRecordsProcessor extends SinkRecordsProcessor {

	public AddRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
			Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs);
	}

	@Override
	public void flushRecordUpdates() {
		//no need to create connection if already empty
		if (!recordsBatch.isEmpty()) {
			try (RepositoryConnection connection = repository.getConnection()) {
				connection.begin();
				while (recordsBatch.peek() != null) {
					connection.add(ValueUtil.convertRDFData(recordsBatch.poll().value()), format);
				}
				connection.commit();
			} catch (IOException e) {
				throw new RuntimeException(e);
				//todo first add retries
				//todo inject error handler
			}
		}
	}
}