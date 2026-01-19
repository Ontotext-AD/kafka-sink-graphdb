package com.ontotext.kafka.test.framework.mockServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ontotext.kafka.util.CertificateUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.socket.PortFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public abstract class MockServerTest {

	protected X509Certificate mockServerCA;
	protected String thumbprint;
	protected ClientAndServer mockServer;
	protected int mockServerPort;

	private static final ObjectMapper OM = new ObjectMapper();

	@BeforeEach
	public void configureMockServer() throws IOException, CertificateException {
		this.mockServerCA = CertificateUtil.getCertificateFromPEM(loadFileFromLocation("/org/mockserver/socket/CertificateAuthorityCertificate.pem"));
		this.thumbprint = DigestUtils.sha256Hex(mockServerCA.getEncoded());
	}

	@BeforeEach
	void startServer() {
		mockServerPort = PortFactory.findFreePort();
		mockServer = ClientAndServer.startClientAndServer(mockServerPort);
	}

	@AfterEach
	void stopServer() {
		disableMTLS();
		mockServer.stop();
	}

	protected void enableMTLS() {
		ConfigurationProperties.tlsMutualAuthenticationRequired(true);
	}

	protected void disableMTLS() {
		ConfigurationProperties.tlsMutualAuthenticationRequired(false);
		ConfigurationProperties.tlsMutualAuthenticationCertificateChain("");
	}

	protected void setMTlsClientCertificateChain(String certificateChain) {
		ConfigurationProperties.tlsMutualAuthenticationCertificateChain(certificateChain);
	}

	public String loadFileFromLocation(String location) throws IOException {
		location = location.trim()
			.replaceAll("\\\\", "/");

		Path path;
		if (location.toLowerCase()
			.startsWith("file:")) {
			path = Paths.get(URI.create(location));
		} else {
			path = Paths.get(location);
		}

		if (Files.exists(path)) {
			// org.apache.commons.io.FileUtils
			return FileUtils.readFileToString(path.toFile(), "UTF-8");
		} else {
			return loadFileFromClasspath(location);
		}
	}

	private String loadFileFromClasspath(String location) {
		try {
			InputStream inputStream = this.getClass()
				.getResourceAsStream(location);
			try {
				if (inputStream == null) {
					inputStream = this.getClass()
						.getClassLoader()
						.getResourceAsStream(location);
				}

				if (inputStream == null) {
					inputStream = ClassLoader.getSystemResourceAsStream(location);
				}

				if (inputStream != null) {
					try {
						// org.apache.commons.io.IOUtils
						return IOUtils.toString(inputStream, Charsets.UTF_8);
					} catch (IOException e) {
						throw new RuntimeException("Could not read " + location + " from the classpath", e);
					}
				}

				throw new RuntimeException("Could not find " + location + " on the classpath");
			} finally {
				if (inputStream != null) {
					inputStream.close();
				}
			}
		} catch (IOException ioe) {
			throw new RuntimeException("Exception closing input stream for " + location, ioe);
		}
	}

	public MockRequestBuilder createExpectation() {
		return new MockRequestBuilder(mockServer);
	}

	public static class MockRequestBuilder {

		private HttpRequest request;
		private HttpResponse response;
		private final ClientAndServer mockServer;

		private MockRequestBuilder(ClientAndServer mockServer) {
			this.mockServer = mockServer;
		}

		public RequestBuilder request() {
			return new RequestBuilder(this);
		}

		public ResponseBuilder response() {
			return new ResponseBuilder(this);
		}


		public void build() {
			mockServer.when(request)
				.respond(response);
		}

		public static class RequestBuilder {

			private final HttpRequest request;
			private final MockRequestBuilder mockRequestBuilder;

			private RequestBuilder(MockRequestBuilder mockRequestBuilder) {
				this.mockRequestBuilder = mockRequestBuilder;
				this.request = HttpRequest.request();
			}

			public RequestBuilder method(String method) {
				request.withMethod(method);
				return this;
			}

			public RequestBuilder path(String path) {
				request.withPath(path);
				return this;
			}

			public RequestBuilder body(String body) {
				request.withPath(body);
				return this;
			}

			public RequestBuilder headers(String key, String... values) {
				request.withHeader(key, values);
				return this;
			}

			public RequestBuilder cookie(String key, String value) {
				request.withCookie(key, value);
				return this;
			}

			public RequestBuilder query(String key, String... values) {
				request.withQueryStringParameter(key, values);
				return this;
			}

			public MockRequestBuilder build() {
				mockRequestBuilder.request = request;
				return mockRequestBuilder;
			}
		}

		public static class ResponseBuilder {
			private final HttpResponse response;
			private final MockRequestBuilder mockRequestBuilder;

			private ResponseBuilder(MockRequestBuilder mockRequestBuilder) {
				this.mockRequestBuilder = mockRequestBuilder;
				response = HttpResponse.response();
			}


			public ResponseBuilder statusCode(int code) {
				response.withStatusCode(code);
				return this;
			}

			public ResponseBuilder cookie(String key, String value) {
				response.withCookie(key, value);
				return this;
			}

			public ResponseBuilder body(String body) {
				response.withBody(body);
				return this;
			}

			public ResponseBuilder body(Object obj) {
				try {
					response.withBody(OM.writeValueAsBytes(obj));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return this;
			}

			public ResponseBuilder header(String key, String... values) {
				response.withHeader(key, values);
				return this;
			}

			public MockRequestBuilder build() {
				mockRequestBuilder.response = response;
				return mockRequestBuilder;
			}

		}


	}

}
