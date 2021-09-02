package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class UpdateRecordsProcessor extends SinkRecordsProcessor {

	// smart update template id

	public UpdateRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository,
								  RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler);
	}

	@Override
	public void flushRecordUpdates() {

	}
}
