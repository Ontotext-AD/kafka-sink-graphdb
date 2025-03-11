package com.ontotext.kafka.graphdb.template;

import org.apache.kafka.common.config.ConfigException;
import org.eclipse.rdf4j.http.protocol.UnauthorizedException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class TemplateUtil {

	private static final Logger LOG = LoggerFactory.getLogger(TemplateUtil.class);

	private TemplateUtil() {
		throw new IllegalStateException("Utility class");
	}

	private static final String TEMPLATE_CONTENT_QUERY = "select ?template {\n <%s> <http://www.ontotext.com/sparql/template> ?template\n}";

	public static void executeUpdate(RepositoryConnection connection, TemplateInput input) {
		try {
			Update update = prepareUpdate(input, connection);
			bindInput(input.getData(), update);
			update.execute();
		} catch (Exception e) {
			throw new RepositoryException("Caught exception while executing update", e);
		}
	}

	private static void bindInput(Map<String, Object> input, Update update) {
		for (String key : input.keySet()) {
			Object obj = input.get(key);
			Value value = ValueParser.getValue(obj);
			if (value == null) {
				LOG.info("Binding for {} is null or empty. Skipping binding.", key);
				continue;
			}
			update.setBinding(key, value);
		}
	}


	private static Update prepareUpdate(TemplateInput input, RepositoryConnection connection) {
		return connection.prepareUpdate(QueryLanguage.SPARQL, getSparqlTemplateContent(input.getTemplateId(), connection));
	}

	private static String getSparqlTemplateContent(String templateId, RepositoryConnection connection) {
		LOG.info("Querying template ID {}", templateId);
		try (TupleQueryResult templates = connection.prepareTupleQuery(String.format(TEMPLATE_CONTENT_QUERY, templateId)).evaluate()) {
			if (templates.hasNext()) {
				// Only interested in first result
				return templates.next().getValue("template").stringValue();
			}
			throw new ConfigException("Did not find template with ID {}", templateId);
		} catch (RepositoryException e) {
			if (e instanceof UnauthorizedException) {
				throw new ConfigException("Invalid credentials", e);
			}
			throw e;
		}
	}
}
