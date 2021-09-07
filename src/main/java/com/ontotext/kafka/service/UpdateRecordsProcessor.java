package com.ontotext.kafka.service;

import com.ontotext.kafka.util.ValueUtil;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class UpdateRecordsProcessor extends SinkRecordsProcessor {

	private final long LINGER_BETWEEN_RETRIES = 100L;
	private final long TOTAL_RETRY_TIME = 30_000L;

	private static final String TEMPLATE = "template";
	private static final String TEMPLATE_ID_PLACEHOLDER = "{{templateIdPlaceholder}}";
	private static final String TEMPLATE_QUERY = "select ?template {" +
			"<{{templateIdPlaceholder}}> <http://www.ontotext.com/sparql/template> ?template" +
			"}";

	protected UpdateRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs) {
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
			String templateId = (String) record.key();
			String template = getTemplate(templateId);

			Update update = connection.prepareUpdate(QueryLanguage.SPARQL, template);
			update.execute();
			Reader valueReader = ValueUtil.convertRDFData(record.value());

			connection.add(valueReader, format);
		} catch (RDFParseException | UnsupportedRDFormatException | DataException | RepositoryException e) {
			// Catch records that caused exceptions we can't recover from by retrying the connection
			//catchMalformedRecords(record, e);
		}
	}

	private String getTemplate(String templateId) {
		List<String> templateIds = new ArrayList<>();
		String query = getQuery(templateId);

		try (RepositoryConnection connection = repository.getConnection()) {
			TupleQueryResult templates = connection.prepareTupleQuery(query).evaluate();
			while (templates.hasNext()) {
				BindingSet template = templates.next();
				templateIds.add(template.getValue(TEMPLATE).stringValue());
			}
		}

		if (templateIds.size() == 1) {
			return templateIds.get(0);
		}
		if (templateIds.isEmpty()) {
			throw new RuntimeException("Template with Id: " + templateId + " not found!");
		}
		throw new RuntimeException("Multiple templates with Id: " + templateId + " found!");
	}

	private String getQuery(String templateId) {
		return TEMPLATE_QUERY.replace(TEMPLATE_ID_PLACEHOLDER, templateId);
	}
}
