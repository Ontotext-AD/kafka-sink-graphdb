package com.ontotext.kafka.service;

import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.OperationHandler;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.model.Resource;
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
 * {@link SinkRecordsProcessor} implementation that directly replaces a graph from {@link SinkRecord} key which
 * indicates the IRI of the named graph and value describing the new RDF contents of the named graph
 * through a {@link RepositoryConnection} to a GraphDB repository.
 * <p>
 *
 * @author Denitsa Stoyanova denitsa.stoyanova@ontotext.com
 */
public class ReplaceGraphProcessor extends SinkRecordsProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(ReplaceGraphProcessor.class);

	protected ReplaceGraphProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun,
									Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs,
									ErrorHandler errorHandler, OperationHandler operator) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler, operator);
	}

	@Override
	protected void handleRecord(SinkRecord record, RepositoryConnection connection) {
		try {
			LOG.trace("Executing replace graph operation......");
			Resource context = ValueUtil.convertIRIKey(record.key());
			connection.clear(context);
			if (LOG.isTraceEnabled()) {
				LOG.trace("IRI of the graph that is being replaced: {}", context.stringValue());
			}
			if (record.value() != null) {
				long start = System.currentTimeMillis();
				connection.add(ValueUtil.convertRDFData(record.value()), format, context);
				long finish = System.currentTimeMillis();
				if (LOG.isTraceEnabled()) {
					Reader recordValue = ValueUtil.convertRDFData(record.value());
					String recordValueString = convertReaderToString(recordValue);
					LOG.trace("Added record value (body): {}", recordValueString);
					LOG.trace("Converted the record and added it to the RDF4J connection for {} ms", finish - start);
				}
			}
		} catch (IOException e) {
			throw new RetriableException(e.getMessage());
		} catch (Exception e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			handleFailedRecord(record, e);
		}
	}

	public static String convertReaderToString(Reader reader) throws IOException {
		StringBuilder stringBuilder = new StringBuilder();
		char[] buffer = new char[1024];
		int numCharsRead;

		while ((numCharsRead = reader.read(buffer)) != -1) {
			stringBuilder.append(buffer, 0, numCharsRead);
		}

		return stringBuilder.toString();
	}

}
