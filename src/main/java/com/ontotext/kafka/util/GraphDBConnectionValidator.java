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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ontotext.kafka.GraphDBSinkConfig.*;

public final class GraphDBConnectionValidator extends AbstractConfigValidator {


	@Override
	protected void doValidate(Config config, Map<String, String> connectorConfigs) {
		Map<String, ConfigValue> configValues = config.configValues().stream()
			.collect(Collectors.toMap(ConfigValue::name, Function.identity()));

		ConfigValue repository = configValues.get(REPOSITORY);
		ConfigValue serverIri = configValues.get(SERVER_URL);

		try {
			GDBConnectionManager manager = new GDBConnectionManager(new GdbConnectionConfig(config));
			manager.validateGDBVersion();
			getLog().trace("Validating GraphDB authentication and repository");
			ConfigValue templateIdValue = configValues.get(TEMPLATE_ID);
			try (RepositoryConnection connection = manager.newConnection()) {
				getLog().trace("Starting repository connection test");
				connection.begin();
				if (templateIdValue != null && StringUtils.isNotEmpty((String) templateIdValue.value())) {
					String templateId = (String) templateIdValue.value();
					getLog().info("Querying template ID {} from repository {}", templateId, manager.getRepositoryURL());
					validateTemplate(connection, templateId);
				}
				connection.rollback();
				getLog().trace("Rolled back repository connection test");
			} catch (RepositoryException e) {
				if (e instanceof UnauthorizedException) {
					throw new GdbConnectionConfigException(AUTH_TYPE, null, "Invalid credentials" + e.getMessage());
				}
				throw e;
			}
		} catch (GdbConnectionConfigException e) {
			configValues.get(e.getValueName()).addErrorMessage(e.getMessage());
		} catch (RepositoryException e) {
			repository.addErrorMessage(new ConfigException(REPOSITORY, repository.value(),
				e.getMessage() + ": Invalid repository").getMessage());
		} catch (Exception e) {
			serverIri.addErrorMessage(e.getMessage());
		}
	}

	private void validateTemplate(RepositoryConnection connection, String templateId) {
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
			getLog().info("Found template {}", template);
		}
	}

}
