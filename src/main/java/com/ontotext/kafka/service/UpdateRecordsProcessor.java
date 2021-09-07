package com.ontotext.kafka.service;

import org.apache.kafka.connect.sink.SinkRecord;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class UpdateRecordsProcessor extends SinkRecordsProcessor {

	private static final String TEMPLATE_ID = "{{templateId}}";
	private static final String TEMPLATE_QUERY = "select ?template {" +
			"<{{templateId}}> <http://www.ontotext.com/sparql/template> ?template" +
			"}";

	protected UpdateRecordsProcessor(Queue<Collection<SinkRecord>> sinkRecords, AtomicBoolean shouldRun, Repository repository, RDFFormat format, int batchSize, long timeoutCommitMs) {
		super(sinkRecords, shouldRun, repository, format, batchSize, timeoutCommitMs);
	}

	@Override
	protected void flushRecordUpdates() {

	}

	private String getTemplate(String binding) {
		List<String> templateIds = new ArrayList<>();
		String query = getQuery(binding);

		try (RepositoryConnection connection = repository.getConnection()) {
			TupleQueryResult templates = connection.prepareTupleQuery(query).evaluate();
			while (templates.hasNext()) {
				BindingSet template = templates.next();
				templateIds.add(template.getValue(binding).stringValue());
			}
		}

		if (templateIds.size() == 1) {
			return templateIds.get(0);
		}
		if (templateIds.size() == 0) {
			throw new RuntimeException("Template with Id: " + binding + " not found!");
		}
		throw new RuntimeException("Multiple templates with Id: " + binding + " found!");
	}

	private String getQuery(String templateId) {
		return TEMPLATE_QUERY.replace(TEMPLATE_ID, templateId);
	}
}
