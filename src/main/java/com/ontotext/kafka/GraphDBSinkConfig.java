package com.ontotext.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

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
	public static final String DEFAULT_AUTH_TYPE = "none";

	public static final String AUTH_BASIC_USER = "graphdb.auth.basic.username";
	public static final String AUTH_BASIC_USER_DOC = "GraphDB basic security username";
	public static final String DEFAULT_AUTH_BASIC_USER = "admin";

	public static final String AUTH_BASIC_PASS = "graphdb.auth.basic.password";
	public static final String AUTH_BASIC_PASS_DOC = "GraphDB basic security password";
	public static final String DEFAULT_AUTH_BASIC_PASS = "root";

	public static final String AUTH_HEADER_TOKEN = "graphdb.auth.header.token";
	public static final String AUTH_HEADER_TOKEN_DOC = "GraphDB custom header token";

	public static final String RDF_FORMAT = "graphdb.transaction.rdf.format";
	public static final String RDF_FORMAT_DOC = "The RDF format for streaming data to GraphDB";

	public static final String TRANSACTION_TYPE = "graphdb.transaction.type";
	public static final String TRANSACTION_TYPE_DOC = "The RDF update transaction type";

	public static final String BATCH_SIZE = "graphdb.batch.size";
	public static final int DEFAULT_BATCH_SIZE = 64;
	public static final String BATCH_SIZE_DOC = "The number of sink records aggregated before being committed";

	public static final String BATCH_COMMIT_SCHEDULER = "graphdb.batch.commit.limit.ms";
	public static final long DEFAULT_BATCH_COMMIT_SCHEDULER = 3000;
	public static final String BATCH_COMMIT_SCHEDULER_DOC = "The timeout applied per batch that is not full before it is committed";

	//todo we need to create topic for failed messages in kafka with name from - properties - errors.deadletterqueue.topic.name
	public static final String DEAD_LETTER_QUEUE_TOPIC_NAME = "errors.deadletterqueue.topic.name";
	public static final String DEAD_LETTER_QUEUE_TOPIC_NAME_DOC = "The name of topic in Kafka for failed messages";
	public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME = "failed-messages-01";

	public static final String ERRORS_TOLERANCE = "errors.tolerance";
	public static final String ERRORS_TOLERANCE_DOC = "all or none\n" +
			" all - ignores all errors and continue\n" +
			" none - causes the connector task to immediately fail\n" +
			" We use all as default and send all failed messages to dedicated topic: "
			+ ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME;
	public static final String DEFAULT_ERRORS_TOLERANCE = "all";

	public static final String ERRORS_ENABLE_HEADERS = "errors.deadletterqueue.context.headers.enable";
	public static final String ERRORS_ENABLE_HEADERS_DOC = "Stores information about the error caused the failure";
	public static final boolean DEFAULT_ERRORS_ENABLE_HEADERS = true;

	public static final String ERRORS_TOPIC_REPLICATION = "errors.deadletterqueue.topic.replication.factor";
	public static final String ERRORS_TOPIC_REPLICATION_DOC = "If you’re running on a single-node Kafka cluster, " +
			" you will also need to set it because the default is 3";
	public static final short DEFAULT_ERRORS_TOPIC_REPLICATION = 1;

	public GraphDBSinkConfig(Map<?, ?> originals) {
		super(CONFIG, originals);
	}

	public static ConfigDef createConfig() {
		return new GraphDBConfigDef()
				       .define(SERVER_IRI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
						       SERVER_IRI_DOC)
				       .define(REPOSITORY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
						       REPOSITORY_DOC)
				       .define(RDF_FORMAT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
						       RDF_FORMAT_DOC)
				       .define(TRANSACTION_TYPE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
						       TRANSACTION_TYPE_DOC)
				       .define(BATCH_SIZE, ConfigDef.Type.INT, DEFAULT_BATCH_SIZE, ConfigDef.Importance.HIGH,
						       BATCH_SIZE_DOC)
				       .define(BATCH_COMMIT_SCHEDULER, ConfigDef.Type.LONG, DEFAULT_BATCH_COMMIT_SCHEDULER,
						       ConfigDef.Importance.HIGH,
						       BATCH_COMMIT_SCHEDULER_DOC)
				       .define(AUTH_TYPE, ConfigDef.Type.STRING, DEFAULT_AUTH_TYPE, ConfigDef.Importance.HIGH,
						       AUTH_TYPE_DOC)
				       .define(AUTH_BASIC_USER, ConfigDef.Type.STRING, DEFAULT_AUTH_BASIC_USER, ConfigDef.Importance.LOW,
						       AUTH_BASIC_USER_DOC)
				       .define(AUTH_BASIC_PASS, ConfigDef.Type.STRING, DEFAULT_AUTH_BASIC_PASS, ConfigDef.Importance.LOW,
						       AUTH_BASIC_PASS_DOC)
				       .define(AUTH_HEADER_TOKEN, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
						       AUTH_HEADER_TOKEN_DOC)
					   .define(ERRORS_TOLERANCE, ConfigDef.Type.STRING, DEFAULT_ERRORS_TOLERANCE, ConfigDef.Importance.MEDIUM,
							   ERRORS_TOLERANCE_DOC)
					   .define(DEAD_LETTER_QUEUE_TOPIC_NAME, ConfigDef.Type.STRING, ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME,
						       ConfigDef.Importance.MEDIUM,
							   DEAD_LETTER_QUEUE_TOPIC_NAME_DOC)
				       .define(ERRORS_ENABLE_HEADERS, ConfigDef.Type.BOOLEAN, DEFAULT_ERRORS_ENABLE_HEADERS,
							   ConfigDef.Importance.MEDIUM,
							   ERRORS_ENABLE_HEADERS_DOC)
				       .define(ERRORS_TOPIC_REPLICATION, ConfigDef.Type.SHORT, DEFAULT_ERRORS_TOPIC_REPLICATION,
							   ConfigDef.Importance.MEDIUM,
							   ERRORS_TOPIC_REPLICATION_DOC)
				;
	}

	public static class GraphDBConfigDef extends ConfigDef {
		@Override
		public Map<String, ConfigValue> validateAll(Map<String, String> props) {
			//todo add validation of properties
			return super.validateAll(props);
		}
	}
}