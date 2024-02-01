package com.ontotext.kafka.error;

import static com.ontotext.kafka.GraphDBSinkConfig.SERVER_IRI;
import static com.ontotext.kafka.error.LogErrorHandler.CONNECT_ENV_PREFIX;
import static com.ontotext.kafka.error.LogErrorHandler.PRODUCER_OVERRIDE_PREFIX;
import static com.ontotext.kafka.error.LogErrorHandler.escapeNewLinesFromString;
import static java.util.Map.entry;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROVIDER_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
class LogErrorHandlerTest {
	@SystemStub
	private EnvironmentVariables environmentVariables;

	private static final Map<Object, Object> ENV_VARIABLES = Map.ofEntries(entry("CONNECT_BOOTSTRAP_SERVERS", "SSL://example.test.com:9094"),
		entry("CONNECT_REST_ADVERTISED_HOST_NAME", "example.test.com"),
		entry("CONNECT_REST_PORT", "8083"),
		entry("CONNECT_REST_HOST_NAME", "example.test.com"),
		entry("CONNECT_GROUP_ID", "compose-connect-group-1"),
		entry("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter"),
		entry("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.converters.ByteArrayConverter"),
		entry("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter"),
		entry("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter"),
		entry("CONNECT_CONFIG_STORAGE_TOPIC", "docker-connect-configs-1"),
		entry("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1"),
		entry("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "10000"),
		entry("CONNECT_OFFSET_STORAGE_TOPIC", "docker-connect-offsets-1"),
		entry("CONNECT_SASL_MECHANISM", "PLAIN"),
		entry("CONNECT_CONSUMER_SASL_MECHANISM", "PLAIN"),
		entry("CONNECT_PRODUCER_SASL_MECHANISM", "PLAIN"),
		entry("CONNECT_SASL_JAAS_CONFIG",
			"        org.apache.kafka.common.security.plain.PlainLoginModule required \\\n" +
			"        username=\"fester\" \\\n" +
			"        serviceName=\"kafka\" \\\n" +
			"        password=\"festerpass\";"),
		entry("CONNECT_CONSUMER_SASL_JAAS_CONFIG",
			"        org.apache.kafka.common.security.plain.PlainLoginModule required \\\n" +
			"        username=\"fester\" \\\n" +
			"        serviceName=\"kafka\" \\\n" +
			"        password=\"festerpass\";"),
		entry("CONNECT_PRODUCER_SASL_JAAS_CONFIG",
			"        org.apache.kafka.common.security.plain.PlainLoginModule required \\\n" +
			"        username=\"fester\" \\\n" +
			"        serviceName=\"kafka\" \\\n" +
			"        password=\"festerpass\";"),
		entry("CONNECT_SECURITY_PROTOCOL", "SASL_SSL"),
		entry("CONNECT_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/keystore7200.jks"),
		entry("CONNECT_CONSUMER_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/keystore7200.jks"),
		entry("CONNECT_PRODUCER_SSL_TRUSTSTORE_LOCATION", "/etc/kafka/secrets/keystore7200.jks"),
		entry("CONNECT_SSL_KEY_PASSWORD", "192.168.129.24-node-7200pass"),
		entry("CONNECT_CONSUMER_SSL_KEY_PASSWORD", "192.168.129.24-node-7200pass"),
		entry("CONNECT_PRODUCER_SSL_KEY_PASSWORD", "192.168.129.24-node-7200pass"));

	@BeforeEach
	void beforeEach() {
		environmentVariables.set(ENV_VARIABLES);
	}

	@Test
	void testFailedRecordProducerConfiguration() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {
		var kafkaConnectProps = Map.of(ERRORS_TOLERANCE_CONFIG, "all",
			SERVER_IRI, "http://localhost:7200",
			DLQ_TOPIC_NAME_CONFIG, "error_topic",
			BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
			PRODUCER_OVERRIDE_PREFIX + SSL_KEY_PASSWORD_CONFIG, new Password("my_pass"),
			PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, new Password("my_pass"),
			PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_KEY_CONFIG, new Password("my_pass"),
			PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_LOCATION_CONFIG, "/keystore_resource_file_path",
			PRODUCER_OVERRIDE_PREFIX + SSL_ENGINE_FACTORY_CLASS_CONFIG, LogErrorHandlerTest.class,
			PRODUCER_OVERRIDE_PREFIX + SSL_PROVIDER_CONFIG, "/path_to_ssl_provider_config");
		var mockedLockHandler = mock(LogErrorHandler.class);
		Method getProperties = mockedLockHandler.getClass().getDeclaredMethod("getProperties", Map.class);
		getProperties.setAccessible(true);
		var convertedProps = (Properties) getProperties.invoke(mockedLockHandler, kafkaConnectProps);
		for (var entry : convertedProps.entrySet()) {
			var entryKey = entry.getKey();
			var entryValue = entry.getValue();
			if (ERRORS_TOLERANCE_CONFIG.equals(entryKey)
				|| DLQ_TOPIC_NAME_CONFIG.equals(entryKey)
				|| BOOTSTRAP_SERVERS_CONFIG.equals(entryKey)
				|| SERVER_IRI.equals(entryKey)
				|| CLIENT_ID_CONFIG.equals(entryKey)) {
				if (!CLIENT_ID_CONFIG.equals(entryKey)) {
					assertTrue(kafkaConnectProps.containsKey(entryKey));
					assertEquals(kafkaConnectProps.get(entryKey), entryValue);
				}
				continue;
			}
			var keyWithPrefix = PRODUCER_OVERRIDE_PREFIX + entryKey;
			assertTrue(kafkaConnectProps.containsKey(keyWithPrefix));
			assertEquals(kafkaConnectProps.get(keyWithPrefix), entryValue);
		}
	}

	@Test
	void testFailedRecordProducerConfigurationFromEnvironmentVariables() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		var kafkaConnectProps = Map.of(ERRORS_TOLERANCE_CONFIG, "all",
			SERVER_IRI, "http://localhost:7200",
			DLQ_TOPIC_NAME_CONFIG, "error_topic");
		var mockedLockHandler = mock(LogErrorHandler.class);
		Method getProperties = mockedLockHandler.getClass().getDeclaredMethod("getProperties", Map.class);
		getProperties.setAccessible(true);
		var convertedProps = (Properties) getProperties.invoke(mockedLockHandler, kafkaConnectProps);
		for (var entry : convertedProps.entrySet()) {
			var entryKey = (String) entry.getKey();
			var entryValue = entry.getValue();
			if (ERRORS_TOLERANCE_CONFIG.equals(entryKey)
				|| DLQ_TOPIC_NAME_CONFIG.equals(entryKey)
				|| SERVER_IRI.equals(entryKey)
				|| CLIENT_ID_CONFIG.equals(entryKey)) {
				if (!CLIENT_ID_CONFIG.equals(entryKey)) {
					assertTrue(kafkaConnectProps.containsKey(entryKey));
					assertEquals(kafkaConnectProps.get(entryKey), entryValue);
				}
				continue;
			}
			var keyWithPrefix = CONNECT_ENV_PREFIX + (entryKey.replace(".", "_").toUpperCase());
			assertTrue(ENV_VARIABLES.containsKey(keyWithPrefix));
			assertEquals(escapeNewLinesFromString((String) ENV_VARIABLES.get(keyWithPrefix)), entryValue);
		}
	}
}
