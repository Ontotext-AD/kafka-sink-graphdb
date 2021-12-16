package com.ontotext.kafka.mocks;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.OperationHandler;
import com.ontotext.kafka.service.AddRecordsProcessor;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThrowingProcessor extends AddRecordsProcessor {

	private int numberOfThrows = 0;

	public ThrowingProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler, OperationHandler operator) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler, operator);
		sinkRecords.forEach(recordsBatch::addAll);
	}

	@Override
	public void handleRecord(SinkRecord record, RepositoryConnection connection) {
		if (numberOfThrows == 0) {
			return;
		}
		if (numberOfThrows > 0) {
			numberOfThrows--;
		}
		throw new RetriableException("dummy");
	}

	public ThrowingProcessor setNumberOfThrows(int numberOfThrows) {
		this.numberOfThrows = numberOfThrows;
		return this;
	}
}
