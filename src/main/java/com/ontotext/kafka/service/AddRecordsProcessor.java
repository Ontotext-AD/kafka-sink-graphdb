package com.ontotext.kafka.service;

import static com.ontotext.kafka.service.ReplaceGraphProcessor.convertReaderToString;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.OperationHandler;
import com.ontotext.kafka.util.ValueUtil;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
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

	private static final Logger LOG = LoggerFactory.getLogger(AddRecordsProcessor.class);

	public AddRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
		Repository repository,
		RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler, OperationHandler operator) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler, operator);
	}

	@Override
	protected void handleRecord(SinkRecord record, RepositoryConnection connection) {
		try {
			LOG.trace("Executing add graph operation......");
			long start = System.currentTimeMillis();
			connection.add(ValueUtil.convertRDFData(record.value()), format);
			long finish = System.currentTimeMillis();
			if (LOG.isTraceEnabled()) {
				Reader recordValue = ValueUtil.convertRDFData(record.value());
				String recordValueString = convertReaderToString(recordValue);
				String context = ValueUtil.convertIRIKey(record.key()).stringValue();
				LOG.trace("Processed record context(IRI): {}", context);
				LOG.trace("Processed record value: {}", recordValueString);
				LOG.trace("Converted the record and added it to the RDF4J connection for {} ms", finish - start);
			}
		} catch (IOException e) {
			if(LOG.isTraceEnabled()) {
				LOG.debug("Caught an I/O exception while processing record");
			}
			throw new RetriableException(e.getMessage(), e);
		} catch (Exception e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			if(LOG.isTraceEnabled()) {
				LOG.debug("Caught non retriable exception while processing record");
			}
			handleFailedRecord(record, e);
		}
	}

}
