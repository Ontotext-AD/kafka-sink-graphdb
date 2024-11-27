package com.ontotext.kafka.error;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.ontotext.kafka.GraphDBSinkConfig.REPOSITORY;
import static com.ontotext.kafka.GraphDBSinkConfig.SERVER_URL;
import static com.ontotext.kafka.error.LogErrorHandler.*;
import static java.util.Map.entry;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.*;
import static org.apache.kafka.connect.runtime.ConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

@ExtendWith(SystemStubsExtension.class)
class LogErrorHandlerTest {
	@SystemStub
	private EnvironmentVariables environmentVariables;

	private static final Map<Object, Object> ENV_VARIABLES = Map.ofEntries(
		entry("CONNECT_BOOTSTRAP_SERVERS", "SSL://example.test.com:9094"),
		entry("CONNECT_SSL_KEYSTORE_KEY", "this_pass"),
		entry("CONNECT_SSL_KEYSTORE_LOCATION", "/keystore_another_resource_file_path"),
		entry("CONNECT_SSL_PROVIDER", "/path_to_another_ssl_provider_config"),
		entry("CONNECT_SSL_KEYSTORE_CERTIFICATE_CHAIN", "another_pass"),
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
		entry("CONNECT_PRODUCER_SSL_KEY_PASSWORD", "192.168.129.24-node-7200pass"),
		entry("CONNECT_SSL_ENGINE_FACTORY_CLASS", "MyClass"),
		entry("CONNECT_CONSUMER_SSL_ENGINE_FACTORY_CLASS", "MyClass"),
		entry("CONNECT_PRODUCER_SSL_ENGINE_FACTORY_CLASS", "MyClass"));

	@Test
	void testFailedRecordProducerConfiguration() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Map<String, Object> kafkaConnectProps = Map.ofEntries(
			entry(ERRORS_TOLERANCE_CONFIG, "all"),
			entry(SERVER_URL, "http://localhost:7200"),
			entry(REPOSITORY, "test"),
			entry(DLQ_TOPIC_NAME_CONFIG, "error_topic"),
			entry(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_KEY_PASSWORD_CONFIG, new Password("my_pass")),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, new Password("my_pass")),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_KEY_CONFIG, new Password("my_pass")),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_LOCATION_CONFIG, "/keystore_resource_file_path"),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_ENGINE_FACTORY_CLASS_CONFIG, LogErrorHandlerTest.class),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_PROVIDER_CONFIG, "/path_to_ssl_provider_config"));

		GraphDBSinkConfig config = new GraphDBSinkConfig(kafkaConnectProps);
		LogErrorHandler handler = new LogErrorHandler(config);

		Properties convertedProps = handler.getProperties(config);
		assertThat(convertedProps).containsKey(BOOTSTRAP_SERVERS_CONFIG)
			.containsValue(Collections.singletonList(kafkaConnectProps.get(BOOTSTRAP_SERVERS_CONFIG)));
		assertThat(convertedProps).containsKey(CLIENT_ID_CONFIG).containsValue(FailedRecordProducer.class.getSimpleName());

		assertThat(convertedProps).containsKey(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).containsValue(
			kafkaConnectProps.get(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG));
		assertThat(convertedProps).containsKey(SSL_KEY_PASSWORD_CONFIG).containsValue(
			kafkaConnectProps.get(PRODUCER_OVERRIDE_PREFIX + SSL_KEY_PASSWORD_CONFIG));
		assertThat(convertedProps).containsKey(SSL_KEYSTORE_KEY_CONFIG).containsValue(
			kafkaConnectProps.get(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_KEY_CONFIG));
		assertThat(convertedProps).containsKey(SSL_KEYSTORE_LOCATION_CONFIG).containsValue(
			kafkaConnectProps.get(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_LOCATION_CONFIG));
		assertThat(convertedProps).containsKey(SSL_ENGINE_FACTORY_CLASS_CONFIG).containsValue(
			kafkaConnectProps.get(PRODUCER_OVERRIDE_PREFIX + SSL_ENGINE_FACTORY_CLASS_CONFIG));
		assertThat(convertedProps).containsKey(SSL_PROVIDER_CONFIG).containsValue(kafkaConnectProps.get(PRODUCER_OVERRIDE_PREFIX + SSL_PROVIDER_CONFIG));
	}

	@Test
	void testFailedRecordProducerConfigurationFromEnvironmentVariables() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		environmentVariables.set(ENV_VARIABLES);
		Map<String, Object> kafkaConnectProps = Map.of(ERRORS_TOLERANCE_CONFIG, "all",
			SERVER_URL, "http://localhost:7200",
			REPOSITORY, "test",
			DLQ_TOPIC_NAME_CONFIG, "error_topic");


		GraphDBSinkConfig config = new GraphDBSinkConfig(kafkaConnectProps);
		LogErrorHandler handler = mock(LogErrorHandler.class);
		doCallRealMethod().when(handler).getProperties(eq(config));

		Properties convertedProps = handler.getProperties(config);
		assertThat(convertedProps).containsKey(CLIENT_ID_CONFIG).containsValue(FailedRecordProducer.class.getSimpleName());

		convertedProps.remove(CLIENT_ID_CONFIG);
		convertedProps.entrySet().forEach(entry -> {
			String key = CONNECT_ENV_PREFIX + (entry.getKey().toString().replace(".", "_").toUpperCase());
			Object value = entry.getValue();
			assertThat(ENV_VARIABLES).containsKey(key);
			assertThat(escapeNewLinesFromString((String) ENV_VARIABLES.get(key))).isEqualTo(value);
		});
	}

	@Test
	void testFailedRecordProducerConfigurationWillBeSetFromEnvVariables() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		environmentVariables.set(ENV_VARIABLES);
		Map<String, Object> kafkaConnectProps = Map.ofEntries(
			entry(ERRORS_TOLERANCE_CONFIG, "all"),
			entry(SERVER_URL, "http://localhost:7200"),
			entry(REPOSITORY, "test"),
			entry(DLQ_TOPIC_NAME_CONFIG, "error_topic"),
			entry(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_KEY_PASSWORD_CONFIG, new Password("my_pass")),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, new Password("my_pass")),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_KEY_CONFIG, new Password("my_pass")),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_KEYSTORE_LOCATION_CONFIG, "/keystore_resource_file_path"),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_ENGINE_FACTORY_CLASS_CONFIG, LogErrorHandlerTest.class),
			entry(PRODUCER_OVERRIDE_PREFIX + SSL_PROVIDER_CONFIG, "/path_to_ssl_provider_config"));

		GraphDBSinkConfig config = new GraphDBSinkConfig(kafkaConnectProps);
		LogErrorHandler handler = mock(LogErrorHandler.class);
		doCallRealMethod().when(handler).getProperties(eq(config));

		Properties convertedProps = handler.getProperties(config);
		assertThat(convertedProps).containsKey(CLIENT_ID_CONFIG).containsValue(FailedRecordProducer.class.getSimpleName());

		convertedProps.remove(CLIENT_ID_CONFIG);
		convertedProps.entrySet().forEach(entry -> {
			String key = CONNECT_ENV_PREFIX + (entry.getKey().toString().replace(".", "_").toUpperCase());
			Object value = entry.getValue();
			assertThat(ENV_VARIABLES).containsKey(key);
			assertThat(escapeNewLinesFromString((String) ENV_VARIABLES.get(key))).isEqualTo(value);
		});
	}
}
