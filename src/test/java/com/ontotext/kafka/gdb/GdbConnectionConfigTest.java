package com.ontotext.kafka.gdb;

import com.ontotext.kafka.GraphDBSinkConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThatCode;

class GdbConnectionConfigTest {

	@Test
	void test_gdbConfigurationUsingSinkConfig_ok() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "BASIC");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_USER, "user");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_PASS, new Password("pass"));
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, "Password");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, "Authorization");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString("Key".getBytes(StandardCharsets.UTF_8)));
		GraphDBSinkConfig config = new GraphDBSinkConfig(originals);
		assertThatCode(() -> new GdbConnectionConfig(config)).doesNotThrowAnyException();
	}

	@Test
	void test_gdbConfigurationUsingSinkConfig2_ok() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "BASIC");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_USER, "user");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_PASS, new Password("pass"));
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		GraphDBSinkConfig config = new GraphDBSinkConfig(originals);
		assertThatCode(() -> new GdbConnectionConfig(config)).doesNotThrowAnyException();
	}

	@Test
	void test_gdbConfigurationUsingSinkConfig3_ok() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "MTLS");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, "Password");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString("Key".getBytes(StandardCharsets.UTF_8)));
		GraphDBSinkConfig config = new GraphDBSinkConfig(originals);
		assertThatCode(() -> new GdbConnectionConfig(config)).doesNotThrowAnyException();
	}

	@Test
	void test_gdbConfigurationUsingSinkConfig4_ok() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "X509_HEADER");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, "Authorization");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		GraphDBSinkConfig config = new GraphDBSinkConfig(originals);
		assertThatCode(() -> new GdbConnectionConfig(config)).doesNotThrowAnyException();
	}


	@Test
	void test_gdbConfigurationUsingSinkConfig_fail() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "MTLS");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, "Authorization");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		GraphDBSinkConfig config = new GraphDBSinkConfig(originals);
		assertThatCode(() -> new GdbConnectionConfig(config)).isInstanceOf(GdbConnectionConfigException.class);
	}

	@Test
	void test_gdbConfigurationUsingSinkConfig_fail2() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "MTLS");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, "Password");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, "Authorization");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString("Key".getBytes(StandardCharsets.UTF_8)));
		GraphDBSinkConfig config = new GraphDBSinkConfig(originals);
		assertThatCode(() -> new GdbConnectionConfig(config)).isInstanceOf(GdbConnectionConfigException.class);
	}

	@Test
	void test_gdbConfigurationUsingSinkConfig_fail3() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "X509_HEADER");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, "Password");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString("Key".getBytes(StandardCharsets.UTF_8)));
		GraphDBSinkConfig config = new GraphDBSinkConfig(originals);
		assertThatCode(() -> new GdbConnectionConfig(config)).isInstanceOf(GdbConnectionConfigException.class);
	}


	// Via config object


	@Test
	void test_gdbConfigurationUsingConfig_ok() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "BASIC");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_USER, "user");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_PASS, new Password("pass"));
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, "Password");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, "Authorization");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString("Key".getBytes(StandardCharsets.UTF_8)));
		Config config = new Config(new GraphDBSinkConfig(originals).values().entrySet().stream().map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), null, null)).collect(Collectors.toList()));
		assertThatCode(() -> new GdbConnectionConfig(config)).doesNotThrowAnyException();
	}

	@Test
	void test_gdbConfigurationUsingConfig2_ok() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "BASIC");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_USER, "user");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_PASS, new Password("pass"));
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values().entrySet().stream().map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), null, null)).collect(Collectors.toList()));
		assertThatCode(() -> new GdbConnectionConfig(config)).doesNotThrowAnyException();
	}

	@Test
	void test_gdbConfigurationUsingConfig3_ok() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "MTLS");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, "Password");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString("Key".getBytes(StandardCharsets.UTF_8)));
		Config config = new Config(new GraphDBSinkConfig(originals).values().entrySet().stream().map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), null, null)).collect(Collectors.toList()));
		assertThatCode(() -> new GdbConnectionConfig(config)).doesNotThrowAnyException();
	}

	@Test
	void test_gdbConfigurationUsingConfig4_ok() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "X509_HEADER");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, "Authorization");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		Config config = new Config(new GraphDBSinkConfig(originals).values().entrySet().stream().map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), null, null)).collect(Collectors.toList()));
		assertThatCode(() -> new GdbConnectionConfig(config)).doesNotThrowAnyException();
	}


	@Test
	void test_gdbConfigurationUsingConfig_fail() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "MTLS");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, "Authorization");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		Config config = new Config(new GraphDBSinkConfig(originals).values().entrySet().stream().map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), null, null)).collect(Collectors.toList()));
		assertThatCode(() -> new GdbConnectionConfig(config)).isInstanceOf(GdbConnectionConfigException.class);
	}

	@Test
	void test_gdbConfigurationUsingConfig_fail2() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "MTLS");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, "Password");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, "Authorization");
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString("Key".getBytes(StandardCharsets.UTF_8)));
		Config config = new Config(new GraphDBSinkConfig(originals).values().entrySet().stream().map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), null, null)).collect(Collectors.toList()));
		assertThatCode(() -> new GdbConnectionConfig(config)).isInstanceOf(GdbConnectionConfigException.class);
	}

	@Test
	void test_gdbConfigurationUsingConfig_fail3() {
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, "repo");
		originals.put(GraphDBSinkConfig.SERVER_URL, "http://localhost:8080");
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "X509_HEADER");
		originals.put(GraphDBSinkConfig.TEMPLATE_ID, "templateID");
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, "Password");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, Base64.getEncoder().encodeToString("Certificate".getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.HOSTNAME_VERIFICATION, "false");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, "ab:bc:cd:de:ef:ff:12:23:34:45:56:67:78:89:90");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString("Key".getBytes(StandardCharsets.UTF_8)));
		Config config = new Config(new GraphDBSinkConfig(originals).values().entrySet().stream().map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), null, null)).collect(Collectors.toList()));
		assertThatCode(() -> new GdbConnectionConfig(config)).isInstanceOf(GdbConnectionConfigException.class);
	}

}
