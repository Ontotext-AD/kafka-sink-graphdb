package com.ontotext.kafka;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class GraphDBSinkConfig extends AbstractConfig {

	public static final ConfigDef CONFIG = createConfig();

	public enum AuthenticationType {
		NONE,
		BASIC,
		CUSTOM;
	}

	public enum TransactionType {
		ADD,
		REPLACE_GRAPH,
		SMART_UPDATE
	}

	public enum BatchingType {
		TIME_PERIOD,
		MESSAGE_NUMBER,
		STATEMENTS_NUMBER
	}

	public static final String SERVER_IRI = "graphdb.server.iri";
	public static final String REPOSITORY = "graphdb.server.repository";

	//when Kafka messages contain RDF data as value
	public static final String RDF_FORMAT = "graphdb.transaction.rdf.format";

	public GraphDBSinkConfig(Map<?, ?> originals) {
		super(CONFIG, originals);
	}

	public static ConfigDef createConfig() {
		return new ConfigDef();
	}
}