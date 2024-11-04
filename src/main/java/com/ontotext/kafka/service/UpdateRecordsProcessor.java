package com.ontotext.kafka.service;

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
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.util.ValueUtil.convertValueToString;

public class UpdateRecordsProcessor extends SinkRecordsProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(UpdateRecordsProcessor.class);
	private final String templateId;
	private final StringBuilder sb;

	protected UpdateRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository,
									 RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler, OperationHandler operator,
									 String templateId) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler, operator);
		Objects.requireNonNull(templateId, "Cannot create update processor with empty graphdb.template.id property");
		this.templateId = templateId;
		this.sb = new StringBuilder();
	}

	@Override
	protected void handleRecord(SinkRecord record, RepositoryConnection connection) {
		try {
			LOG.trace("Executing update graph operation......");
			long start = System.currentTimeMillis();
			String query = getQuery(record);
			connection.prepareUpdate(query)
				.execute();
			connection.add(ValueUtil.convertRDFData(record.value()), format);
			long finish = System.currentTimeMillis();
			if (LOG.isTraceEnabled()) {
				LOG.trace("Record info: {}", ValueUtil.recordInfo(record));
				LOG.trace("Converted the record and added it to the RDF4J connection for {} ms", finish - start);
			}
		} catch (IOException e) {
			if(LOG.isTraceEnabled()) {
				LOG.debug("Caught an I/O exception while processing record");
			}
			throw new RetriableException(e.getMessage());
		} catch (Exception e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			if(LOG.isTraceEnabled()) {
				LOG.debug("Caught non retriable exception while processing record");
			}
			handleFailedRecord(record, e);
		}
	}

	private String getQuery(SinkRecord record) throws NullPointerException {
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
