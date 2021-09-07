package com.ontotext.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
						       AUTH_HEADER_TOKEN_DOC);
	}

	public static class GraphDBConfigDef extends ConfigDef {
		@Override
		public Map<String, ConfigValue> validateAll(Map<String, String> props) {
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("VALIDATE ALL");
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			Set<ConfigKey> values = (Set<ConfigKey>) configKeys().values();
			for (ConfigKey k : values) {
				System.out.println(k.type + " " + k.name);
			}

			return super.validateAll(props);
		}

		private void validateAuthType(){}
	}
}