package com.ontotext.kafka;

import com.ontotext.kafka.logging.LoggerFactory;
import com.ontotext.kafka.util.EnumValidator;
import com.ontotext.kafka.util.RDFFormatValidator;
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
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkConfig extends AbstractConfig {

	public static final ConfigDef CONFIG_DEFINITION = createConfigDef();


	private final int batchSize;
	private final Long processorRecordPollTimeoutMs;
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
	private final String authMTlsClientCertificate;
	private final String authMTlsClientCertificateKey;
	private final Password authMTlsClientCertificateKeyPassword;
	private final String authCertificateHeaderString;
	private final String authCertificateHeaderName;
	private final String connectorName;
	private final long backOffTimeoutMs;
	private final String tlsThumbprint;
	private final boolean hostnameVerificationEnabled;


	public enum AuthenticationType {
		NONE,
		BASIC,
		MTLS,
		X509_HEADER,
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

	public static final String LOG_LEVEL_OVERRIDE = "log.level.override";
	public static final String LOG_LEVEL_OVERRIDE_DOC = "Log level override for Kafka Sink Connector. Overrides the log level that may be set in log4j properties. May work with some loggers (example, MSK)";
	public static final String LOG_LEVEL_OVERRIDE_DEFAULT = "INFO";

	public static final String LOGGER_TYPE = "logger.type";
	public static final String LOGGER_TYPE_DOC = "Log level override for Kafka Sink Connector. Overrides the log level that may be set in log4j properties. May work with some loggers (example, MSK)";
	public static final String LOGGER_TYPE_DEFAULT = "default";


	public static final String AUTH_BASIC_PASS = "graphdb.auth.basic.password";
	public static final String AUTH_BASIC_PASS_DOC = "GraphDB basic authentication password";
	public static final String DEFAULT_AUTH_BASIC_PASS = "root";

	public static final String MTLS_CERTIFICATE_STRING = "graphdb.auth.mtls.client.certificate";
	public static final String MTLS_CERTIFICATE_STRING_DOC = "Client certificate, Base64-encoded (when using mTLS)";

	public static final String MTLS_CERTIFICATE_KEY_STRING = "graphdb.auth.mtls.client.certificateKey";
	public static final String MTLS_CERTIFICATE_KEY_STRING_DOC = "Client certificate key, Base64-encoded (when using mTLS)";

	public static final String MTLS_CERTIFICATE_KEY_PASSWORD = "graphdb.auth.mtls.client.certificateKeyPassword";
	public static final String MTLS_CERTIFICATE_KEY_PASSWORD_DOC = "Client certificate key password, if the key is encrypted (when using mTLS)";

	public static final String AUTH_HEADER_CERTIFICATE_STRING = "graphdb.auth.header.certificate";
	public static final String AUTH_HEADER_CERTIFICATE_STRING_DOC = "Client certificate, Base64-encoded (when using header-based authentication)";

	public static final String AUTH_HEADER_NAME = "graphdb.auth.header.name";
	public static final String AUTH_HEADER_NAME_DOC = "Client certificate header name (when using header-based authentication)";

	public static final String DEFAULT_AUTH_HEADER_NAME = "ssl_client_cert";

	public static final String RDF_FORMAT = "graphdb.update.rdf.format";
	public static final String DEFAULT_RDF_TYPE = "ttl";
	public static final String RDF_FORMAT_DOC = "The RDF format for streaming data to GraphDB";

	public static final String TRANSACTION_TYPE = "graphdb.update.type";
	public static final String TRANSACTION_TYPE_DOC = "The RDF update type";
	public static final String DEFAULT_TRANSACTION_TYPE = "ADD";

	public static final String BATCH_SIZE = "graphdb.batch.size";
	public static final int DEFAULT_BATCH_SIZE = 64;
	public static final String BATCH_SIZE_DOC = "The number of sink records aggregated before being committed";

	public static final String RECORD_POLL_TIMEOUT = "graphdb.batch.commit.limit.ms";
	public static final long DEFAULT_RECORD_POLL_TIMEOUT = 3000;
	public static final String RECORD_POLL_TIMEOUT_DOC = "The timeout applied per batch that is not full before it is committed";

	public static final String POLL_BACKOFF_TIMEOUT = "graphdb.sink.poll.backoff.timeout.ms";
	public static final long DEFAULT_POLL_BACKOFF_TIMEOUT = 10000;
	public static final String POLL_BACKOFF_TIMEOUT_DOC = "Backoff time (in ms) which forces the task to pause ingestion in case of retriable exceptions downstream";

	public static final String TEMPLATE_ID = "graphdb.template.id";
	public static final String TEMPLATE_ID_DOC = "The id(IRI) of GraphDB Template to be used by in SPARQL Update";

	public static final String TLS_THUMBPRINT = "graphdb.tls.thumbprint";
	public static final String TLS_THUMBPRINT_DOC = "The TLS certificate thumbprint of the GraphDB instance (required only one if using certificate chain)";

	public static final String HOSTNAME_VERIFICATION = "graphdb.tls.hostname.verification.enabled";
	public static final String HOSTNAME_VERIFICATION_DOC = "Enable the hostname verification when performing TLS certificate verification (Default enabled)";

	public static final String ERROR_GROUP = "Error Handling";
	public static final String DLQ_TOPIC_DISPLAY = "Dead Letter Queue Topic Name";

	public GraphDBSinkConfig(Map<?, ?> originals) {
		super(CONFIG_DEFINITION, originals, true); // log the configuration after setup is complete
		this.batchSize = getInt(BATCH_SIZE);
		this.processorRecordPollTimeoutMs = getLong(RECORD_POLL_TIMEOUT);
		this.backOffTimeoutMs = getLong(POLL_BACKOFF_TIMEOUT);
		this.transactionType = TransactionType.valueOf(getString(TRANSACTION_TYPE).toUpperCase()); // NPE-safe because a default value is set for this field
		this.rdfFormat = ValueUtil.getRDFFormat(getString(RDF_FORMAT));
		this.templateId = getString(TEMPLATE_ID);
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
		this.connectorName = (String) originals.get(NAME_CONFIG);
		this.tlsThumbprint = (String) originals.get(TLS_THUMBPRINT);
		this.hostnameVerificationEnabled = Boolean.parseBoolean((String) originals.get(HOSTNAME_VERIFICATION));
		this.authMTlsClientCertificate = getString(MTLS_CERTIFICATE_STRING);
		this.authMTlsClientCertificateKey = getString(MTLS_CERTIFICATE_KEY_STRING);
		this.authMTlsClientCertificateKeyPassword = getPassword(MTLS_CERTIFICATE_KEY_PASSWORD);
		this.authCertificateHeaderName = getString(AUTH_HEADER_NAME);
		this.authCertificateHeaderString = getString(AUTH_HEADER_CERTIFICATE_STRING);
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
				new RDFFormatValidator(),
				ConfigDef.Importance.HIGH,
				RDF_FORMAT_DOC
			)
			.define(
				TRANSACTION_TYPE,
				ConfigDef.Type.STRING,
				DEFAULT_TRANSACTION_TYPE,
				new EnumValidator(TransactionType.class),
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
				RECORD_POLL_TIMEOUT,
				ConfigDef.Type.LONG,
				DEFAULT_RECORD_POLL_TIMEOUT,
				ConfigDef.Importance.HIGH,
				RECORD_POLL_TIMEOUT_DOC
			)
			.define(
				POLL_BACKOFF_TIMEOUT,
				ConfigDef.Type.LONG,
				DEFAULT_POLL_BACKOFF_TIMEOUT,
				ConfigDef.Importance.HIGH,
				POLL_BACKOFF_TIMEOUT_DOC
			)
			.define(
				AUTH_TYPE,
				ConfigDef.Type.STRING,
				DEFAULT_AUTH_TYPE,
				new EnumValidator(AuthenticationType.class),
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
				AUTH_HEADER_CERTIFICATE_STRING,
				ConfigDef.Type.STRING,
				"",
				ConfigDef.Importance.LOW,
				AUTH_HEADER_CERTIFICATE_STRING_DOC
			)
			.define(
				LOG_LEVEL_OVERRIDE,
				ConfigDef.Type.STRING,
				LOG_LEVEL_OVERRIDE_DEFAULT,
				ConfigDef.Importance.LOW,
				LOG_LEVEL_OVERRIDE_DOC
			)
			.define(
				LOGGER_TYPE,
				ConfigDef.Type.STRING,
				LOGGER_TYPE_DEFAULT,
				new EnumValidator(LoggerFactory.LoggerType.class),
				ConfigDef.Importance.LOW,
				LOGGER_TYPE_DOC
			)
			.define(
				AUTH_HEADER_NAME,
				ConfigDef.Type.STRING,
				DEFAULT_AUTH_HEADER_NAME,
				ConfigDef.Importance.LOW,
				AUTH_HEADER_NAME_DOC
			)
			.define(
				TEMPLATE_ID,
				ConfigDef.Type.STRING,
				null,
				ConfigDef.Importance.MEDIUM,
				TEMPLATE_ID_DOC
			)
			.define(
				TLS_THUMBPRINT,
				ConfigDef.Type.STRING,
				null,
				ConfigDef.Importance.MEDIUM,
				TLS_THUMBPRINT_DOC
			)
			.define(
				MTLS_CERTIFICATE_STRING,
				ConfigDef.Type.STRING,
				null,
				ConfigDef.Importance.MEDIUM,
				MTLS_CERTIFICATE_STRING_DOC
			)
			.define(
				MTLS_CERTIFICATE_KEY_STRING,
				ConfigDef.Type.STRING,
				null,
				ConfigDef.Importance.MEDIUM,
				MTLS_CERTIFICATE_KEY_STRING_DOC
			)
			.define(
				MTLS_CERTIFICATE_KEY_PASSWORD,
				ConfigDef.Type.PASSWORD,
				null,
				ConfigDef.Importance.MEDIUM,
				MTLS_CERTIFICATE_KEY_PASSWORD_DOC
			)
			.define(
				HOSTNAME_VERIFICATION,
				ConfigDef.Type.BOOLEAN,
				true,
				ConfigDef.Importance.MEDIUM,
				HOSTNAME_VERIFICATION_DOC
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

	public Long getProcessorRecordPollTimeoutMs() {
		return processorRecordPollTimeoutMs;
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

	public String getDlqTopicName() {
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

	public String getTopicName() {
		return topicName;
	}

	public long getBackOffTimeoutMs() {
		return backOffTimeoutMs;
	}

	public String getTlsThumbprint() {
		return tlsThumbprint;
	}

	public boolean isHostnameVerificationEnabled() {
		return hostnameVerificationEnabled;
	}

	public String getAuthMTlsClientCertificate() {
		return authMTlsClientCertificate;
	}

	public String getAuthMTlsClientCertificateKey() {
		return authMTlsClientCertificateKey;
	}

	public Password getAuthMTlsClientCertificateKeyPassword() {
		return authMTlsClientCertificateKeyPassword;
	}

	public String getAuthCertificateHeaderString() {
		return authCertificateHeaderString;
	}

	public String getAuthCertificateHeaderName() {
		return authCertificateHeaderName;
	}

	public static class GraphDBConfigDef extends ConfigDef {
		@Override
		public Map<String, ConfigValue> validateAll(Map<String, String> props) {
			return super.validateAll(props);
		}
	}

}
