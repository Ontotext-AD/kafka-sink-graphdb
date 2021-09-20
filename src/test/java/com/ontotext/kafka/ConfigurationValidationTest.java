package com.ontotext.kafka;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockserver.client.MockServerClient;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.junit.jupiter.MockServerSettings;

@ExtendWith(MockServerExtension.class)
@MockServerSettings(ports = 12345)
public class ConfigurationValidationTest {
	private Map<String, String> configs;
	private GraphDBSinkConnector sinkconnector;
	private List<ConfigValue> results;

	public ConfigurationValidationTest() {
		ConfigurationProperties.logLevel("OFF");
	}

	@BeforeEach
	public void setup() {
		sinkconnector = new GraphDBSinkConnector();
	}

	@Test
	@DisplayName("Test correct config")
	@Timeout(5)
	void testWithAllCorrect(MockServerClient mockedGraphDBClient) {
		mockedGraphDBClient = new MockServerClient("localhost", 12345);
		setupMockClientResponses(mockedGraphDBClient, "basic", "9.10.0");
		configs =
			new HashMap<>() {
				{
					put("graphdb.batch.size", "5");
					put("graphdb.batch.commit.limit.ms", "3000");
					put("graphdb.auth.type", "basic");
					put("graphdb.auth.basic.username", "johnnyawesome");
					put("graphdb.auth.basic.password", "luben1");
					put("graphdb.transaction.type", "ADD");
					put("graphdb.transaction.rdf.format", "nq");
					put("graphdb.server.iri", "http://localhost:12345/");
					put("graphdb.server.repository", "Test");
				}
			};
		results = sinkconnector.validate(configs).configValues();

		mockedGraphDBClient.reset();
		Assertions.assertTrue(results.stream().allMatch(cv -> cv.errorMessages().isEmpty()), getErrorMessages(results));
	}

	@Test
	@DisplayName("Test default configs")
	@Timeout(5)
	void testWithDefaults(MockServerClient mockedGraphDBClient) {
		mockedGraphDBClient = new MockServerClient("localhost", 12345);
		setupMockClientResponses(mockedGraphDBClient, "none", "9.10.0");
		configs =
			new HashMap<>() {
				{
					put("graphdb.server.iri", "http://localhost:12345/");
					put("graphdb.server.repository", "Test");
				}
			};
		results = sinkconnector.validate(configs).configValues();
		mockedGraphDBClient.reset();
		Assertions.assertTrue(results.stream().allMatch(cv -> cv.errorMessages().isEmpty()),
			"Got an error using default configs: " + getErrorMessages(results));
	}

	@Test
	@DisplayName("Test with wrong credentials")
	@Timeout(5)
	void testWithWrongCredentials(MockServerClient mockedGraphDBClient) {
		mockedGraphDBClient = new MockServerClient("localhost", 12345);
		setupMockClientResponses(mockedGraphDBClient, "basic", "9.10.0");
		configs =
			new HashMap<>() {
				{
					put("graphdb.server.iri", "http://localhost:12345/");
					put("graphdb.server.repository", "Test");
					put("graphdb.auth.type", "basic");
					put("graphdb.auth.basic.username", "invalid");
					put("graphdb.auth.basic.password", "credentials");
				}
			};
		results = sinkconnector.validate(configs).configValues();
		mockedGraphDBClient.reset();
		Assertions.assertTrue(results.stream()
			                      .filter(cv -> cv.name().equals("graphdb.auth.type"))
			                      .noneMatch(cv -> cv.errorMessages().isEmpty()),
			"Didn't get an error for invalid credentials");
	}

	@Test
	@DisplayName("Test with wrong GraphDB version")
	@Timeout(5)
	void testWithWrongGraphDBVersion(MockServerClient mockedGraphDBClient) {
		mockedGraphDBClient = new MockServerClient("localhost", 12345);
		setupMockClientResponses(mockedGraphDBClient, "none", "9.9.0");
		configs =
			new HashMap<>() {
				{
					put("graphdb.server.iri", "http://localhost:12345/");
					put("graphdb.server.repository", "Test");
				}
			};
		results = sinkconnector.validate(configs).configValues();
		mockedGraphDBClient.reset();
		Assertions.assertTrue(results.stream()
			                      .filter(cv -> cv.name().equals("graphdb.server.iri"))
			                      .noneMatch(cv -> cv.errorMessages().isEmpty()),
			"Didn't get an error for GraphDB version");
	}

	@Test
	@DisplayName("Test with no GraphDB")
	@Timeout(5)
	void testWithNoGraphDB() {
		configs =
			new HashMap<>() {
				{
					put("graphdb.server.iri", "http://localhost:12345/");
					put("graphdb.server.repository", "Test");
				}
			};
		results = sinkconnector.validate(configs).configValues();
		Assertions.assertTrue(results.stream()
			                      .filter(cv -> cv.name().equals("graphdb.server.iri"))
			                      .noneMatch(cv -> cv.errorMessages().isEmpty()),
			"Didn't get an error for missing GraphDB");
	}

	@ParameterizedTest
	@CsvSource({ "graphdb.batch.size, 5a",
		"graphdb.batch.commit.limit.ms, 3000a",
		"graphdb.auth.type, noners",
		"graphdb.transaction.type, ADDer",
		"graphdb.transaction.rdf.format, nqq" })
	@DisplayName("Test wrong configs")
	@Timeout(5)
	void testWithWrongValues(String key, String value) {
		configs =
			new HashMap<>() {
				{
					put(key, value);
					put("graphdb.server.iri", "1");
					put("graphdb.server.repository", "2");
				}
			};
		results = sinkconnector.validate(configs).configValues();
		Assertions.assertTrue(
			results.stream().filter(cv -> cv.name().equals(key)).noneMatch(cv -> cv.errorMessages().isEmpty()),
			"Didn't get an error for an invalid " + key);
	}

	private String getErrorMessages(List<ConfigValue> results) {

		StringBuilder msg = new StringBuilder();
		results.stream().filter(cv -> !cv.errorMessages().isEmpty()).forEach(cv -> msg.append(cv.errorMessages()));
		return msg.toString();
	}

	private void setupMockClientResponses(MockServerClient mockedGraphDBClient, String authenticationType,
		String productVersion) {

		mockedGraphDBClient.when(
			request()
				.withMethod("GET")
				.withPath("/rest/info/version")
		)
			.respond(
				response()
					.withStatusCode(200)
					.withHeader(
						"Content-Type", "application/json;charset=UTF-8"
					)
					.withBody("{\"productVersion\":\"" + productVersion + "\"}")
			);

		mockedGraphDBClient.when(
			request()
				.withMethod("GET")
				.withPath("/protocol")
		)
			.respond(
				response()
					.withStatusCode(200)
					.withHeader("Content-Type", "text/plain;charset=UTF-8")
					.withBody("12")
			);

		if (!Objects.equals(authenticationType, "basic")) {
			mockedGraphDBClient.when(
				request()
					.withMethod("POST")
					.withPath("/repositories/Test/transactions")
			)
				.respond(
					response()
						.withStatusCode(201)
						.withHeader("Location",
							"http://localhost:12345/repositories/Test/transactions/a7d5630a-4ef1-4028-897d-2dfc1ab804d1")
				);
		} else {
			mockedGraphDBClient.when(
				request()
					.withMethod("POST")
					.withPath("/repositories/Test/transactions")
					.withHeader("Authorization", "Basic am9obm55YXdlc29tZTpsdWJlbjE=")
			)
				.respond(
					response()
						.withStatusCode(201)
						.withHeader("Location",
							"http://localhost:12345/repositories/Test/transactions/a7d5630a-4ef1-4028-897d-2dfc1ab804d1")
				);
			mockedGraphDBClient.when(
				request()
					.withMethod("POST")
					.withPath("/repositories/Test/transactions")
			)
				.respond(
					response()
						.withStatusCode(401)
						.withBody("Unauthorized (HTTP status 401)")
				);
		}

		mockedGraphDBClient.when(
			request()
				.withMethod("DELETE")
				.withPath("/repositories/Test/transactions/a7d5630a-4ef1-4028-897d-2dfc1ab804d1")
		)
			.respond(
				response()
					.withStatusCode(204)
			);
	}
}






