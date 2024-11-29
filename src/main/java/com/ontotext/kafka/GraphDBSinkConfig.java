package com.ontotext.kafka;

import com.ontotext.kafka.util.ValidateEnum;
import com.ontotext.kafka.util.ValidateRDFFormat;
import com.ontotext.kafka.util.ValueUtil;
import com.ontotext.kafka.util.VisibleIfRecommender;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.eclipse.rdf4j.rio.RDFFormat;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.*;

/**
 * Config implementation used to store and validate main Connector properties including Authentication and Transaction Type.
 *
 * This class is re-using the same {@link org.apache.kafka.common.config.Config:originals} map, without adding any new state,
 * therefore {@link Object#equals(Object)} and {@link Object#hashCode()} are calculated by simply calling super()
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkConfig extends AbstractConfig {

	public static final ConfigDef CONFIG_DEFINITION = createConfigDef();

	private final int batchSize;
	private final Long timeoutCommitMs;
	private final TransactionType transactionType;
	private final RDFFormat rdfFormat;
	private final String templateId;
	private final ToleranceType tolerance;
	private final String topicName;
	private final List<String> bootstrapServers;
	private final long errorRetryTimeout;
	private final long errorMaxDelayInMillis;
	private final String serverUrl;
	private final AuthenticationType authType;
	private final String repositoryId;
	private final String authBasicUser;
	private final Password authBasicPassword;
	private final String connectorName;


	public enum AuthenticationType {
		NONE,
		BASIC,
		CUSTOM;
	}

	public enum TransactionType {
		ADD,
		REPLACE_GRAPH,
		SMART_UPDATE;

	}

	public static final String SERVER_URL = "graphdb.server.url";
	public static final String SERVER_URL_DOC = "GraphDB Server connection URL";

	public static final String REPOSITORY = "graphdb.server.repository";
	public static final String REPOSITORY_DOC = "Repository to which data is streamed";

	public static final String AUTH_TYPE = "graphdb.auth.type";
	public static final String AUTH_TYPE_DOC = "The authentication type used by GraphDB";
	public static final String DEFAULT_AUTH_TYPE = "NONE";

	public static final String AUTH_BASIC_USER = "graphdb.auth.basic.username";
	public static final String AUTH_BASIC_USER_DOC = "GraphDB basic authentication username";
	public static final String DEFAULT_AUTH_BASIC_USER = "admin";

	public static final String AUTH_BASIC_PASS = "graphdb.auth.basic.password";
	public static final String AUTH_BASIC_PASS_DOC = "GraphDB basic authentication password";
	public static final String DEFAULT_AUTH_BASIC_PASS = "root";

	public static final String AUTH_HEADER_TOKEN = "graphdb.auth.header.token";
	public static final String AUTH_HEADER_TOKEN_DOC = "GraphDB custom header token";

	public static final String RDF_FORMAT = "graphdb.update.rdf.format";
	public static final String DEFAULT_RDF_TYPE = "ttl";
	public static final String RDF_FORMAT_DOC = "The RDF format for streaming data to GraphDB";

	public static final String TRANSACTION_TYPE = "graphdb.update.type";
	public static final String TRANSACTION_TYPE_DOC = "The RDF update type";
	public static final String DEFAULT_TRANSACTION_TYPE = "ADD";

	public static final String BATCH_SIZE = "graphdb.batch.size";
	public static final int DEFAULT_BATCH_SIZE = 64;
	public static final String BATCH_SIZE_DOC = "The number of sink records aggregated before being committed";

	public static final String BATCH_COMMIT_SCHEDULER = "graphdb.batch.commit.limit.ms";
	public static final long DEFAULT_BATCH_COMMIT_SCHEDULER = 3000;
	public static final String BATCH_COMMIT_SCHEDULER_DOC = "The timeout applied per batch that is not full before it is committed";

	public static final String TEMPLATE_ID = "graphdb.template.id";
	public static final String TEMPLATE_ID_DOC = "The id(IRI) of GraphDB Template to be used by in SPARQL Update";

	public static final String ERROR_GROUP = "Error Handling";
	public static final String DLQ_TOPIC_DISPLAY = "Dead Letter Queue Topic Name";

	public GraphDBSinkConfig(Map<?, ?> originals) {
		super(CONFIG_DEFINITION, originals, true); // log the configuration after setup is complete
		batchSize = getInt(BATCH_SIZE);
		timeoutCommitMs = getLong(BATCH_COMMIT_SCHEDULER);
		transactionType = TransactionType.valueOf(getString(TRANSACTION_TYPE).toUpperCase()); // NPE-safe because a default value is set for this field
		rdfFormat = ValueUtil.getRDFFormat(getString(RDF_FORMAT));
		templateId = getString(TEMPLATE_ID);
		this.topicName = getString(DLQ_TOPIC_NAME_CONFIG);
		this.tolerance = parseTolerance();
		this.bootstrapServers = getList(BOOTSTRAP_SERVERS_CONFIG);
		this.errorRetryTimeout = getLong(ERRORS_RETRY_TIMEOUT_CONFIG);
		this.errorMaxDelayInMillis = getLong(ERRORS_RETRY_MAX_DELAY_CONFIG);
		this.serverUrl = getString(SERVER_URL);
		this.authType = AuthenticationType.valueOf(getString(AUTH_TYPE).toUpperCase()); // NPE-safe because a default value is set for this field
		this.repositoryId = getString(REPOSITORY);
		this.authBasicUser = getString(AUTH_BASIC_USER);
		this.authBasicPassword = getPassword(AUTH_BASIC_PASS);
		this.connectorName = getString(NAME_CONFIG);
	}

	private ToleranceType parseTolerance() {
		String tolerance = getString(ConnectorConfig.ERRORS_TOLERANCE_CONFIG);
		if (tolerance == null || "none".equalsIgnoreCase(tolerance)) {
			return ToleranceType.NONE;
		} else if ("all".equalsIgnoreCase(tolerance)) {
			return ToleranceType.ALL;
		} else {
			throw new DataException("error: Tolerance can be \"none\" or \"all\". Not supported for - "
				+ tolerance);
		}
	}

	public static ConfigDef createConfigDef() {
		int orderInErrorGroup = 0;
		return new GraphDBConfigDef()
			.define(
				SERVER_URL,
				ConfigDef.Type.STRING,
				ConfigDef.Importance.HIGH,
				SERVER_URL_DOC
			)
			.define(
				REPOSITORY,
				ConfigDef.Type.STRING,
				ConfigDef.Importance.HIGH,
				REPOSITORY_DOC
			)
			.define(
				RDF_FORMAT,
				ConfigDef.Type.STRING,
				DEFAULT_RDF_TYPE,
				new ValidateRDFFormat(),
				ConfigDef.Importance.HIGH,
				RDF_FORMAT_DOC
			)
			.define(
				TRANSACTION_TYPE,
				ConfigDef.Type.STRING,
				DEFAULT_TRANSACTION_TYPE,
				new ValidateEnum(TransactionType.class),
				ConfigDef.Importance.HIGH,
				TRANSACTION_TYPE_DOC
			)
			.define(
				BATCH_SIZE,
				ConfigDef.Type.INT,
				DEFAULT_BATCH_SIZE,
				ConfigDef.Importance.HIGH,
				BATCH_SIZE_DOC
			)
			.define(
				BATCH_COMMIT_SCHEDULER,
				ConfigDef.Type.LONG,
				DEFAULT_BATCH_COMMIT_SCHEDULER,
				ConfigDef.Importance.HIGH,
				BATCH_COMMIT_SCHEDULER_DOC
			)
			.define(
				AUTH_TYPE,
				ConfigDef.Type.STRING,
				DEFAULT_AUTH_TYPE,
				new ValidateEnum(AuthenticationType.class),
				ConfigDef.Importance.HIGH,
				AUTH_TYPE_DOC
			)
			.define(
				AUTH_BASIC_USER,
				ConfigDef.Type.STRING,
				DEFAULT_AUTH_BASIC_USER,
				ConfigDef.Importance.HIGH,
				AUTH_BASIC_USER_DOC,
				null,
				-1,
				ConfigDef.Width.NONE,
				AUTH_BASIC_USER,
				new VisibleIfRecommender(AUTH_TYPE, AuthenticationType.BASIC)
			)
			.define(
				AUTH_BASIC_PASS,
				ConfigDef.Type.PASSWORD,
				DEFAULT_AUTH_BASIC_PASS,
				ConfigDef.Importance.HIGH,
				AUTH_BASIC_PASS_DOC,
				null,
				-1,
				ConfigDef.Width.NONE,
				AUTH_BASIC_PASS,
				new VisibleIfRecommender(AUTH_TYPE, AuthenticationType.BASIC)
			)
			.define(
				AUTH_HEADER_TOKEN,
				ConfigDef.Type.STRING,
				"",
				ConfigDef.Importance.LOW,
				AUTH_HEADER_TOKEN_DOC
			)
			.define(
				TEMPLATE_ID,
				ConfigDef.Type.STRING,
				null,
				ConfigDef.Importance.MEDIUM,
				TEMPLATE_ID_DOC
			)
			//error handling
			.define(
				DLQ_TOPIC_NAME_CONFIG,
				ConfigDef.Type.STRING,
				DLQ_TOPIC_DEFAULT,
				ConfigDef.Importance.MEDIUM,
				DLQ_TOPIC_NAME_DOC,
				ERROR_GROUP,
				++orderInErrorGroup,
				ConfigDef.Width.MEDIUM,
				DLQ_TOPIC_DISPLAY
			)
			.define(
				ERRORS_RETRY_TIMEOUT_CONFIG,
				ConfigDef.Type.LONG,
				ERRORS_RETRY_TIMEOUT_DEFAULT,
				ConfigDef.Importance.MEDIUM,
				ERRORS_RETRY_TIMEOUT_DOC,
				ERROR_GROUP,
				++orderInErrorGroup,
				ConfigDef.Width.MEDIUM,
				ERRORS_RETRY_TIMEOUT_DISPLAY
			)
			.define(
				ERRORS_RETRY_MAX_DELAY_CONFIG,
				ConfigDef.Type.LONG,
				ERRORS_RETRY_MAX_DELAY_DEFAULT,
				ConfigDef.Importance.MEDIUM,
				ERRORS_RETRY_MAX_DELAY_DOC,
				ERROR_GROUP,
				++orderInErrorGroup,
				ConfigDef.Width.MEDIUM, ERRORS_RETRY_MAX_DELAY_DISPLAY
			)
			.define(
				ERRORS_TOLERANCE_CONFIG,
				ConfigDef.Type.STRING,
				ERRORS_TOLERANCE_DEFAULT.value(),
				in(ToleranceType.NONE.value(), ToleranceType.ALL.value()),
				ConfigDef.Importance.MEDIUM,
				ERRORS_TOLERANCE_DOC,
				ERROR_GROUP,
				++orderInErrorGroup,
				ConfigDef.Width.SHORT,
				ERRORS_TOLERANCE_DISPLAY
			)
			.define(
				WorkerConfig.BOOTSTRAP_SERVERS_CONFIG,
				ConfigDef.Type.LIST,
				Collections.emptyList(),
				new ConfigDef.NonNullValidator(),
				ConfigDef.Importance.HIGH,
				CommonClientConfigs.BOOTSTRAP_SERVERS_DOC
			);
	}

	public int getBatchSize() {
		return batchSize;
	}

	public Long getTimeoutCommitMs() {
		return timeoutCommitMs;
	}

	public TransactionType getTransactionType() {
		return transactionType;
	}

	public RDFFormat getRdfFormat() {
		return rdfFormat;
	}

	public String getTemplateId() {
		return templateId;
	}

	public ToleranceType getTolerance() {
		return tolerance;
	}

	public String getTopicName() {
		return topicName;
	}


	public List<String> getBootstrapServers() {
		return bootstrapServers;
	}


	public long getErrorRetryTimeout() {
		return errorRetryTimeout;
	}

	public long getErrorMaxDelayInMillis() {
		return errorMaxDelayInMillis;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public AuthenticationType getAuthType() {
		return authType;
	}

	public String getRepositoryId() {
		return repositoryId;
	}

	public String getAuthBasicUser() {
		return authBasicUser;
	}

	public Password getAuthBasicPassword() {
		return authBasicPassword;
	}

	public String getConnectorName() {
		return connectorName;
	}

	public static class GraphDBConfigDef extends ConfigDef {
		@Override
		public Map<String, ConfigValue> validateAll(Map<String, String> props) {
			return super.validateAll(props);
		}
	}


	/**
	 * Use the {@link org.apache.kafka.common.config.Config} equals implementation,
	 * which calculates the hash on the {@link org.apache.kafka.common.config.Config:originals} map
	 */
	@Override
	public boolean equals(Object o) {
		return super.equals(o);
	}

	/**
	 * Use the {@link org.apache.kafka.common.config.Config} hashcode implementation,
	 * which calculates the hash on the {@link org.apache.kafka.common.config.Config:originals} map
	 */
	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
