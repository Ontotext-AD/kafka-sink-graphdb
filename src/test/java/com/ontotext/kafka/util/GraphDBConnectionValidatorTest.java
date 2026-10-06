package com.ontotext.kafka.util;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.test.framework.mockServer.MockServerTest;
import com.ontotext.kafka.test.framework.tls.TlsUtil;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class GraphDBConnectionValidatorTest extends MockServerTest {


	@TempDir
	File tempDir;

	@Test
	void test_validateConfig_ok() {
		String repoName = "repo";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("http://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "BASIC");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_USER, "user");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_PASS, new Password("pass"));
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", "11.0.1"))
			.build()
			.build();


		createExpectation()
			.request()
			.method("GET")
			.path("/protocol")
			.build()
			.response()
			.statusCode(200)
			.body("12")
			.build()
			.build();

		createExpectation()
			.request()
			.method("POST")
			.path(String.format("/repositories/%s/transactions", repoName))
			.build()
			.response()
			.statusCode(201)
			.header("Location", String.format("http://localhost:%d/transaction1", mockServerPort))
			.build()
			.build();


		createExpectation()
			.request()
			.method("DELETE")
			.path("/transaction1")
			.build()
			.response()
			.statusCode(204)
			.build()
			.build();

		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).isEmpty();
	}


	@Test
	void test_validateConfig_tls_ok() {
		String repoName = "repo";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("https://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "BASIC");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_USER, "user");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, thumbprint);
		originals.put(GraphDBSinkConfig.AUTH_BASIC_PASS, new Password("pass"));
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", "11.0.1"))
			.build()
			.build();


		createExpectation()
			.request()
			.method("GET")
			.path("/protocol")
			.build()
			.response()
			.statusCode(200)
			.body("12")
			.build()
			.build();

		createExpectation()
			.request()
			.method("POST")
			.path(String.format("/repositories/%s/transactions", repoName))
			.build()
			.response()
			.statusCode(201)
			.header("Location", String.format("http://localhost:%d/transaction1", mockServerPort))
			.build()
			.build();


		createExpectation()
			.request()
			.method("DELETE")
			.path("/transaction1")
			.build()
			.response()
			.statusCode(204)
			.build()
			.build();
		final Config[] result = new Config[]{config};
		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).isEmpty();
	}

	@Test
	void test_validateConfig_tls_thumbprintMismatch_fail() {
		String repoName = "repo";
		String invalidThumbprint = "aa:bb:cc:dd:e";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("https://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "BASIC");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_USER, "user");
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, invalidThumbprint);
		originals.put(GraphDBSinkConfig.AUTH_BASIC_PASS, new Password("pass"));
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", "11.0.1"))
			.build()
			.build();


		createExpectation()
			.request()
			.method("GET")
			.path("/protocol")
			.build()
			.response()
			.statusCode(200)
			.body("12")
			.build()
			.build();

		createExpectation()
			.request()
			.method("POST")
			.path(String.format("/repositories/%s/transactions", repoName))
			.build()
			.response()
			.statusCode(201)
			.header("Location", String.format("http://localhost:%d/transaction1", mockServerPort))
			.build()
			.build();


		createExpectation()
			.request()
			.method("DELETE")
			.path("/transaction1")
			.build()
			.response()
			.statusCode(204)
			.build()
			.build();

		final Config[] result = new Config[]{config};
		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).isNotEmpty()
			.contains(
				String.format("Invalid value %s for configuration graphdb.tls.thumbprint: Did not find certificate that matches the thumbprint %s", invalidThumbprint, invalidThumbprint,
					invalidThumbprint));
	}

	@Test
	void test_validateConfig_mtls_ok() throws IOException {
		String repoName = "repo";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("https://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "MTLS");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, Base64.getEncoder().encodeToString(TlsUtil.CERTIFICATE1.getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, new Password("pass"));
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString(TlsUtil.KEY1.getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, thumbprint);
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", "11.0.1"))
			.build()
			.build();


		createExpectation()
			.request()
			.method("GET")
			.path("/protocol")
			.build()
			.response()
			.statusCode(200)
			.body("12")
			.build()
			.build();

		createExpectation()
			.request()
			.method("POST")
			.path(String.format("/repositories/%s/transactions", repoName))
			.build()
			.response()
			.statusCode(201)
			.header("Location", String.format("https://localhost:%d/transaction1", mockServerPort))
			.build()
			.build();


		createExpectation()
			.request()
			.method("DELETE")
			.path("/transaction1")
			.build()
			.response()
			.statusCode(204)
			.build()
			.build();

		enableMTLS();
		Path chainPath = Paths.get(tempDir.getAbsolutePath(), "chain.pem");
		Files.writeString(chainPath, TlsUtil.CERTIFICATE1);
		setMTlsClientCertificateChain(chainPath.toString());

		final Config[] result = new Config[]{config};
		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).isEmpty();
	}

	@Test
	void test_validateConfig_mtls_invalidClientCertificate_fail() throws IOException {
		String repoName = "repo";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("https://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "MTLS");
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, Base64.getEncoder().encodeToString(TlsUtil.CERTIFICATE2.getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_PASSWORD, new Password("pass"));
		originals.put(GraphDBSinkConfig.MTLS_CERTIFICATE_KEY_STRING, Base64.getEncoder().encodeToString(TlsUtil.KEY1.getBytes(StandardCharsets.UTF_8)));
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, thumbprint);
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", "11.0.1"))
			.build()
			.build();


		createExpectation()
			.request()
			.method("GET")
			.path("/protocol")
			.build()
			.response()
			.statusCode(200)
			.body("12")
			.build()
			.build();

		createExpectation()
			.request()
			.method("POST")
			.path(String.format("/repositories/%s/transactions", repoName))
			.build()
			.response()
			.statusCode(201)
			.header("Location", String.format("https://localhost:%d/transaction1", mockServerPort))
			.build()
			.build();


		createExpectation()
			.request()
			.method("DELETE")
			.path("/transaction1")
			.build()
			.response()
			.statusCode(204)
			.build()
			.build();

		enableMTLS();
		Path chainPath = Paths.get(tempDir.getAbsolutePath(), "chain.pem");
		Files.writeString(chainPath, TlsUtil.CERTIFICATE1);
		setMTlsClientCertificateChain(chainPath.toString());

		final Config[] result = new Config[]{config};
		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).contains("Invalid value [redacted] for configuration graphdb.auth.mtls.client.certificate: mTLS handshake failed most probably due to invalid client certificate");
	}

	@Test
	void test_validateConfig_x509Header_ok() throws IOException {
		String repoName = "repo";
		String x509Header = Base64.getEncoder().encodeToString(TlsUtil.CERTIFICATE1.getBytes(StandardCharsets.UTF_8));
		String headerName = "X.509";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("https://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "X509_HEADER");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, x509Header);
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, headerName);
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, thumbprint);
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.headers(headerName, TlsUtil.CERTIFICATE1.replace("\n", " "))
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", "11.0.1"))
			.build()
			.build();


		createExpectation()
			.request()
			.method("GET")
			.headers(headerName, TlsUtil.CERTIFICATE1.replace("\n", " "))
			.path("/protocol")
			.build()
			.response()
			.statusCode(200)
			.body("12")
			.build()
			.build();

		createExpectation()
			.request()
			.method("POST")
			.headers(headerName, TlsUtil.CERTIFICATE1.replace("\n", " "))
			.path(String.format("/repositories/%s/transactions", repoName))
			.build()
			.response()
			.statusCode(201)
			.header("Location", String.format("https://localhost:%d/transaction1", mockServerPort))
			.build()
			.build();


		createExpectation()
			.request()
			.method("DELETE")
			.headers(headerName, TlsUtil.CERTIFICATE1.replace("\n", " "))
			.path("/transaction1")
			.build()
			.response()
			.statusCode(204)
			.build()
			.build();

		final Config[] result = new Config[]{config};
		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).isEmpty();
	}

	@Test
	void test_validateConfig_x509Header_invalidClientCertificate_fail() throws IOException {
		String repoName = "repo";
		String x509Header = Base64.getEncoder().encodeToString(TlsUtil.CERTIFICATE1.getBytes(StandardCharsets.UTF_8));
		String headerName = "X.509";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("https://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "X509_HEADER");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, x509Header);
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, headerName);
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, thumbprint);
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.headers(headerName, TlsUtil.CERTIFICATE1.replace("\n", " "))
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(401)
			.body("UNAUTHORIZED")
			.build()
			.build();


		final Config[] result = new Config[]{config};
		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).contains(String.format("Invalid value https://localhost:%d for configuration graphdb.server.url: Authentication failed", mockServerPort));
	}


	// --------------- Version tests -----------------//
	@ParameterizedTest
	@ValueSource(strings = {"9.11.0", "9.12.0", "10.8.0", "10.9.0", "10.10.0", "11.0.0", "11.1.0", "11.2.0", "11.3.0", "11.4.0", "11.5.0", "12.0.0", "12.1.0", "12.2.0", "12.3.0"})
	void test_validateConfig_validVersions_ok(String serverVersion) {
		String repoName = "repo";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("http://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "BASIC");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_USER, "user");
		originals.put(GraphDBSinkConfig.AUTH_BASIC_PASS, new Password("pass"));
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", serverVersion))
			.build()
			.build();


		createExpectation()
			.request()
			.method("GET")
			.path("/protocol")
			.build()
			.response()
			.statusCode(200)
			.body("12")
			.build()
			.build();

		createExpectation()
			.request()
			.method("POST")
			.path(String.format("/repositories/%s/transactions", repoName))
			.build()
			.response()
			.statusCode(201)
			.header("Location", String.format("http://localhost:%d/transaction1", mockServerPort))
			.build()
			.build();


		createExpectation()
			.request()
			.method("DELETE")
			.path("/transaction1")
			.build()
			.response()
			.statusCode(204)
			.build()
			.build();

		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).isEmpty();
	}


	@ParameterizedTest
	@ValueSource(strings = {"8.9.0", "9.0.0", "9.1.0", "9.2.0", "9.3.0", "9.4.0", "9.5.0", "9.6.0", "9.7.0", "9.8.0", "9.9.0", "9.10.0"})
	void test_validateConfig_invalidGDBVersion9_fail(String serverVersion) throws IOException {
		String repoName = "repo";
		String x509Header = Base64.getEncoder().encodeToString(TlsUtil.CERTIFICATE1.getBytes(StandardCharsets.UTF_8));
		String headerName = "X.509";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("https://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "X509_HEADER");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, x509Header);
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, headerName);
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, thumbprint);
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.headers(headerName, TlsUtil.CERTIFICATE1.replace("\n", " "))
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", serverVersion))
			.build()
			.build();

		final Config[] result = new Config[]{config};
		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).contains(String.format(
			"Invalid value https://localhost:%d for configuration graphdb.server.url: Unsupported GraphDB version %s. Kafka sink is supported on GraphDB versions 9.11+, 10.8+, and versions 11 and above. Please update your GraphDB",
			mockServerPort, serverVersion));
	}

	@ParameterizedTest
	@ValueSource(strings = {"10.0.0", "10.1.0", "10.2.0", "10.3.0", "10.4.0", "10.5.0", "10.6.0", "10.7.0"})
	void test_validateConfig_invalidGDBVersion10_fail(String serverVersion) throws IOException {
		String repoName = "repo";
		String x509Header = Base64.getEncoder().encodeToString(TlsUtil.CERTIFICATE1.getBytes(StandardCharsets.UTF_8));
		String headerName = "X.509";
		Map<String, Object> originals = new HashMap<>();
		originals.put(GraphDBSinkConfig.BATCH_SIZE, 10);
		originals.put(GraphDBSinkConfig.REPOSITORY, repoName);
		originals.put(GraphDBSinkConfig.SERVER_URL, String.format("https://localhost:%d", mockServerPort));
		originals.put(GraphDBSinkConfig.AUTH_TYPE, "X509_HEADER");
		originals.put(GraphDBSinkConfig.AUTH_HEADER_CERTIFICATE_STRING, x509Header);
		originals.put(GraphDBSinkConfig.AUTH_HEADER_NAME, headerName);
		originals.put(GraphDBSinkConfig.TLS_THUMBPRINT, thumbprint);
		originals.put(GraphDBSinkConfig.TRANSACTION_TYPE, "ADD");
		Config config = new Config(new GraphDBSinkConfig(originals).values()
			.entrySet()
			.stream()
			.map(entry -> new ConfigValue(entry.getKey(), entry.getValue(), new ArrayList<>(), new ArrayList<>()))
			.collect(Collectors.toList()));

		createExpectation()
			.request()
			.method("GET")
			.headers(headerName, TlsUtil.CERTIFICATE1.replace("\n", " "))
			.path("/rest/info/version")
			.build()
			.response()
			.statusCode(200)
			.body(Collections.singletonMap("productVersion", serverVersion))
			.build()
			.build();

		final Config[] result = new Config[]{config};
		assertThatCode(() -> new GraphDBConnectionValidator().validate(config, Collections.emptyMap())).doesNotThrowAnyException();
		Collection<String> errors = config.configValues()
			.stream()
			.map(ConfigValue::errorMessages)
			.flatMap(List::stream)
			.collect(Collectors.toList());


		assertThat(errors).contains(String.format(
			"Invalid value https://localhost:%d for configuration graphdb.server.url: Unsupported GraphDB version %s. Kafka sink is supported on GraphDB versions 9.11+, 10.8+, and versions 11 and above. Please update your GraphDB",
			mockServerPort, serverVersion));
	}


}
