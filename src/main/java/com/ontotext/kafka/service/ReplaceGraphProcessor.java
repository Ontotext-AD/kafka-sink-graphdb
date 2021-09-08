package com.ontotext.kafka.service;

import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.sink.SinkRecord;
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

	private boolean shouldClearGraph = true;

	protected ReplaceGraphProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
									Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs);
	}

	protected void flushRecordUpdates() {
		//no need to create connection if already empty
		if (!recordsBatch.isEmpty()) {
			try (RepositoryConnection connection = repository.getConnection()) {
				connection.begin();

				if (shouldClearGraph && recordsBatch.peek() != null) {
					connection.clear(ValueUtil.convertIRIKey(recordsBatch.peek().key()));
					shouldClearGraph = false;
				}

				while (recordsBatch.peek() != null) {
					SinkRecord record = recordsBatch.poll();
					connection.add(ValueUtil.convertRDFData(record.value()), format, ValueUtil.convertIRIKey(record.key()));
				}
				connection.commit();
			} catch (IOException e) {
				throw new RuntimeException(e);
				//todo first add retries
				//todo inject error handler
			}
		} else {
			shouldClearGraph = true;
		}
	}
}