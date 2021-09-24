package com.ontotext.kafka;

import com.ontotext.kafka.util.ValidateEnum;
import com.ontotext.kafka.util.ValidateRDFFormat;
import com.ontotext.kafka.util.VisibleIfRecommender;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.errors.ToleranceType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.ValidString.in;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.*;

/**
 * Config implementation used to store and validate main Connector properties including Authentication and Transaction Type.
 *
 * @author Tomas Kovachev tomas.kovachev@ontotext.com
 */
public class GraphDBSinkConfig extends AbstractConfig {

	public static final ConfigDef CONFIG = createConfig();

	public enum AuthenticationType {
		NONE,
		BASIC,
		CUSTOM;

		private static final Map<String, AuthenticationType> MAP = new HashMap<>();

		static {
			for (AuthenticationType type : values()) {
				MAP.put(type.toString().toLowerCase(), type);
			}
		}

		public static AuthenticationType of(String type) {
			return MAP.get(type.toLowerCase());
		}
	}

	public enum TransactionType {
		ADD,
		REPLACE_GRAPH,
		SMART_UPDATE;

		private static final Map<String, TransactionType> MAP = new HashMap<>();

		static {
			for (TransactionType type : values()) {
				MAP.put(type.toString().toLowerCase(), type);
			}
		}

		public static TransactionType of(String type) {
			return MAP.get(type.toLowerCase());
		}
	}

	public static final String SERVER_IRI = "graphdb.server.iri";
	public static final String SERVER_IRI_DOC = "GraphDB Server connection IRI";

	public static final String REPOSITORY = "graphdb.server.repository";
	public static final String REPOSITORY_DOC = "Repository to which data is streamed";

	public static final String AUTH_TYPE = "graphdb.auth.type";
	public static final String AUTH_TYPE_DOC = "The authentication type used by GraphDB";
	public static final String DEFAULT_AUTH_TYPE = "NONE";

	public static final String AUTH_BASIC_USER = "graphdb.auth.basic.username";
	public static final String AUTH_BASIC_USER_DOC = "GraphDB basic security username";
	public static final String DEFAULT_AUTH_BASIC_USER = "admin";

	public static final String AUTH_BASIC_PASS = "graphdb.auth.basic.password";
	public static final String AUTH_BASIC_PASS_DOC = "GraphDB basic security password";
	public static final String DEFAULT_AUTH_BASIC_PASS = "root";

	public static final String AUTH_HEADER_TOKEN = "graphdb.auth.header.token";
	public static final String AUTH_HEADER_TOKEN_DOC = "GraphDB custom header token";

	public static final String RDF_FORMAT = "graphdb.transaction.rdf.format";
	public static final String DEFAULT_RDF_TYPE = "ttl";
	public static final String RDF_FORMAT_DOC = "The RDF format for streaming data to GraphDB";

	public static final String TRANSACTION_TYPE = "graphdb.transaction.type";
	public static final String TRANSACTION_TYPE_DOC = "The RDF update transaction type";
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
		super(CONFIG, originals);
	}

	public static ConfigDef createConfig() {
		int orderInErrorGroup = 0;
		return new GraphDBConfigDef()
			.define(SERVER_IRI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
				SERVER_IRI_DOC)
			.define(REPOSITORY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
				REPOSITORY_DOC)
			.define(RDF_FORMAT, ConfigDef.Type.STRING, DEFAULT_RDF_TYPE, new ValidateRDFFormat(),
				ConfigDef.Importance.HIGH, RDF_FORMAT_DOC)
			.define(TRANSACTION_TYPE, ConfigDef.Type.STRING, DEFAULT_TRANSACTION_TYPE,
				new ValidateEnum(GraphDBSinkConfig.TransactionType.class),
				ConfigDef.Importance.HIGH, TRANSACTION_TYPE_DOC)
			.define(BATCH_SIZE, ConfigDef.Type.INT, DEFAULT_BATCH_SIZE, ConfigDef.Importance.HIGH,
				BATCH_SIZE_DOC)
			.define(BATCH_COMMIT_SCHEDULER, ConfigDef.Type.LONG, DEFAULT_BATCH_COMMIT_SCHEDULER,
				ConfigDef.Importance.HIGH,
				BATCH_COMMIT_SCHEDULER_DOC)
			.define(AUTH_TYPE, ConfigDef.Type.STRING, DEFAULT_AUTH_TYPE,
				new ValidateEnum(GraphDBSinkConfig.AuthenticationType.class),
				ConfigDef.Importance.HIGH, AUTH_TYPE_DOC)
			.define(AUTH_BASIC_USER, ConfigDef.Type.STRING, DEFAULT_AUTH_BASIC_USER,
				ConfigDef.Importance.HIGH, AUTH_BASIC_USER_DOC,
				null, -1, ConfigDef.Width.NONE, AUTH_BASIC_USER,
				new VisibleIfRecommender(AUTH_TYPE, AuthenticationType.BASIC))
			.define(AUTH_BASIC_PASS, ConfigDef.Type.PASSWORD, DEFAULT_AUTH_BASIC_PASS,
				ConfigDef.Importance.HIGH, AUTH_BASIC_PASS_DOC,
				null, -1, ConfigDef.Width.NONE, AUTH_BASIC_PASS,
				new VisibleIfRecommender(AUTH_TYPE, AuthenticationType.BASIC))
			.define(AUTH_HEADER_TOKEN, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
				AUTH_HEADER_TOKEN_DOC)
			.define(TEMPLATE_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
				TEMPLATE_ID_DOC)
			//error handling
			.define(DLQ_TOPIC_NAME_CONFIG, ConfigDef.Type.STRING, DLQ_TOPIC_DEFAULT, ConfigDef.Importance.MEDIUM,
				DLQ_TOPIC_NAME_DOC, ERROR_GROUP, ++orderInErrorGroup, ConfigDef.Width.MEDIUM, DLQ_TOPIC_DISPLAY)
			.define(ERRORS_RETRY_TIMEOUT_CONFIG, ConfigDef.Type.LONG, ERRORS_RETRY_TIMEOUT_DEFAULT, ConfigDef.Importance.MEDIUM,
				ERRORS_RETRY_TIMEOUT_DOC, ERROR_GROUP, ++orderInErrorGroup, ConfigDef.Width.MEDIUM, ERRORS_RETRY_TIMEOUT_DISPLAY)
			.define(ERRORS_RETRY_MAX_DELAY_CONFIG, ConfigDef.Type.LONG, ERRORS_RETRY_MAX_DELAY_DEFAULT, ConfigDef.Importance.MEDIUM,
				ERRORS_RETRY_MAX_DELAY_DOC, ERROR_GROUP, ++orderInErrorGroup, ConfigDef.Width.MEDIUM, ERRORS_RETRY_MAX_DELAY_DISPLAY)
			.define(ERRORS_TOLERANCE_CONFIG, ConfigDef.Type.STRING, ERRORS_TOLERANCE_DEFAULT.value(),
				in(ToleranceType.NONE.value(), ToleranceType.ALL.value()), ConfigDef.Importance.MEDIUM,
				ERRORS_TOLERANCE_DOC, ERROR_GROUP, ++orderInErrorGroup, ConfigDef.Width.SHORT, ERRORS_TOLERANCE_DISPLAY)
			.define(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), new ConfigDef.NonNullValidator(),
				ConfigDef.Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
			;
	}

	public static class GraphDBConfigDef extends ConfigDef {
		@Override
		public Map<String, ConfigValue> validateAll(Map<String, String> props) {
			return super.validateAll(props);
		}
	}
}
