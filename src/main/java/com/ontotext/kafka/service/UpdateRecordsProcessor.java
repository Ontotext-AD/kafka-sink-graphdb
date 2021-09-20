package com.ontotext.kafka.service;

import com.google.common.annotations.VisibleForTesting;
import com.ontotext.kafka.error.ErrorHandler;
import com.ontotext.kafka.operation.OperationHandler;
import com.ontotext.kafka.util.PropertiesUtil;
import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ontotext.kafka.util.PropertiesUtil.getProperty;
import static com.ontotext.kafka.util.ValueUtil.convertValueToString;

public class UpdateRecordsProcessor extends SinkRecordsProcessor {

	private static String templateId = getProperty(PropertiesUtil.TEMPLATE_ID);
	private final StringBuilder sb;

	protected UpdateRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository,
									 RDFFormat format, int batchSize, long timeoutCommitMs, ErrorHandler errorHandler, OperationHandler operator) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs, errorHandler, operator);
		Objects.requireNonNull(templateId, "Cannot create update processor with empty graphdb.template.id property");
		this.sb = new StringBuilder();
	}

	@Override
	protected void handleRecord(SinkRecord record, RepositoryConnection connection){
		try {
			String query = getQuery(record);
			connection.prepareUpdate(query)
				.execute();
			connection.add(ValueUtil.convertRDFData(record.value()), format);
		} catch (IOException e) {
			throw new RetriableException(e.getMessage());
		} catch (Exception e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
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

	@VisibleForTesting
	static void setTemplateId(String templateId){
		UpdateRecordsProcessor.templateId = templateId;
	}

}
