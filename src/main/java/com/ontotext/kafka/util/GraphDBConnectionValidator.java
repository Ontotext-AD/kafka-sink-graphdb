package com.ontotext.kafka.util;

import com.ontotext.kafka.gdb.GDBConnectionManager;
import com.ontotext.kafka.gdb.GdbConnectionConfig;
import com.ontotext.kafka.gdb.GdbConnectionConfigException;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.eclipse.rdf4j.http.protocol.UnauthorizedException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ontotext.kafka.GraphDBSinkConfig.*;

public final class GraphDBConnectionValidator {
	private static final Logger LOG = LoggerFactory.getLogger(GraphDBConnectionValidator.class);


	private GraphDBConnectionValidator() {
		throw new IllegalStateException("Utility class");
	}


	public static Config validateGraphDBConnection(Config validatedConnectorConfigs) {

		Map<String, ConfigValue> configValues = validatedConnectorConfigs.configValues().stream()
			.collect(Collectors.toMap(ConfigValue::name, Function.identity()));

		ConfigValue repository = configValues.get(REPOSITORY);
		ConfigValue serverIri = configValues.get(SERVER_URL);

		try {
			GDBConnectionManager manager = new GDBConnectionManager(new GdbConnectionConfig(validatedConnectorConfigs));
			manager.validateGDBVersion();
			doValidate(configValues, manager);
		} catch (GdbConnectionConfigException e) {
			configValues.get(e.getValueName()).addErrorMessage(e.getMessage());
		} catch (RepositoryException e) {
			repository.addErrorMessage(new ConfigException(REPOSITORY, repository.value(),
				e.getMessage() + ": Invalid repository").getMessage());
		} catch (Exception e) {
			serverIri.addErrorMessage(e.getMessage());
		}
		return validatedConnectorConfigs;
	}
	private static void validateTemplate(RepositoryConnection connection, String templateId) {
		String templateContentQ = "select ?template {\n <%s> <http://www.ontotext.com/sparql/template> ?template\n}";
		try (TupleQueryResult templates = connection.prepareTupleQuery(String.format(templateContentQ, templateId)).evaluate()) {
			String template = null;
			if (templates.hasNext()) {
				// Only interested in first result
				template = templates.next().getValue("template").stringValue();
			}
			if (StringUtils.isEmpty(template)) {
				throw new ConfigException("Did not find template with ID {}", templateId);
			}
			LOG.info("Found template {}", template);
		}
	}

	private static void doValidate(Map<String, ConfigValue> configValues, GDBConnectionManager manager) {
		LOG.trace("Validating GraphDB authentication and repository");


		ConfigValue templateIdValue = configValues.get(TEMPLATE_ID);
		try (RepositoryConnection connection = manager.newConnection()) {
			LOG.trace("Starting repository connection test");
			connection.begin();
			if (templateIdValue != null && StringUtils.isNotEmpty((String) templateIdValue.value())) {
				String templateId = (String) templateIdValue.value();
				LOG.info("Querying template ID {} from repository {}", templateId, manager.getRepositoryURL());
				validateTemplate(connection, templateId);
			}
			connection.rollback();
			LOG.trace("Rolled back repository connection test");
		} catch (RepositoryException e) {
			if (e instanceof UnauthorizedException) {
				throw new GdbConnectionConfigException(AUTH_TYPE, null, "Invalid credentials" + e.getMessage());
			}
			throw e;
		}
	}
}
