package com.ontotext.kafka.tls;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpConnection;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

import static com.ontotext.kafka.tls.TrustAllSSLContext.SSL_CONTEXT;

/**
 * A utility class to confiure the TLS connection between the Kafka Sink Connector (client) and downstream GraphDB instance (server)
 * The class takes care of configuring the default {@link javax.net.ssl.SSLContext} for all following connections by using a CompositeTrustManager
 */
public final class HttpsClientManager {
	private static final Logger log = LoggerFactory.getLogger(HttpsClientManager.class);

	private HttpsClientManager() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Parses the certificates provided in the connection to `connectionUrl` and finds the certificate that matches the provided thumbprint (SHA-256)
	 *
	 * @param connectionUrl    - the server url to perform the check against
	 * @param sha256Thumbprint - the SHA-256 thumbprint of the certificate to retrieve
	 * @return The certificate, or `null` if no certificate was found that matches the thumbprint
	 * @throws IOException              if any error occurs while establishing or working with an active connection to the server
	 * @throws IllegalArgumentException if the `connectionUrl` is invalid
	 * @throws RuntimeException         if any other error occurs during validation that are not expected
	 */
	public static X509Certificate getCertificate(String connectionUrl, String sha256Thumbprint) throws IOException {
		if (!isUrlHttps(connectionUrl)) {
			throw new IllegalArgumentException("Invalid connection URL: " + connectionUrl);
		}
		log.debug("Fetching certificates from {}", connectionUrl);
		try {
			URLConnection connection = new URL(connectionUrl).openConnection();
			HttpsURLConnection conn = null;
			if (connection instanceof HttpsURLConnection) {
				conn = (HttpsURLConnection) connection;
			} else {
				throw new IllegalArgumentException("Not an HTTPS connection");
			}
			log.debug("Creating the trustAll trust manager for this connection only");
			conn.setSSLSocketFactory(SSL_CONTEXT.getSocketFactory());
			conn.setHostnameVerifier((hostname, session) -> true);
			log.info("Connecting to {}", connectionUrl);
			conn.connect();
			Certificate[] certs = conn.getServerCertificates();
			log.info("Got {} certificates", certs.length);
			for (Certificate cert : certs) {
				log.debug("Certificate is {}", cert);
				if (cert instanceof X509Certificate) {
					X509Certificate x509 = (X509Certificate) cert;
					log.debug("Computing certificate thumbprint");
					String thumbprint = DigestUtils.sha256Hex(x509.getEncoded());
					log.debug("Certificate thumbprint is {}", thumbprint);
					if (thumbprint.equals(sanitize(sha256Thumbprint))) {
						log.info("Found certificate that matches {}", thumbprint);
						return x509;
					}
				}
			}
			log.error("Found no certificates in the certificate chain that matches thumbprint {}", sha256Thumbprint);
			return null;
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Invalid URL", e);
		} catch (CertificateEncodingException e) {
			throw new IllegalArgumentException("Invalid certificates from server", e);
		}
	}


	/**
	 * Initialize the TLS context for working with the provided server. This will configure the default {@link SSLContext} for all communication
	 *
	 * @param serverUrl                   - the server url
	 * @param sha256Thumbprint            - the SHA-256 thumbprint of the server certificate
	 * @param hostnameVerificationEnabled
	 * @throws SSLException             - if any error occured during configuration
	 * @throws IllegalArgumentException - if no thumbprint provided by the server url is https
	 */
	public static CloseableHttpClient createHttpClient(String serverUrl, String sha256Thumbprint, boolean hostnameVerificationEnabled) throws SSLException {
		if (isUrlHttps(serverUrl)) {
			if (StringUtils.isEmpty(sha256Thumbprint)) {
				throw new IllegalArgumentException("Thumbprint should not be empty if using TLS");
			}
		} else {
			return getClientBuilder().build();
		}
		log.info("Getting the certificate that has the thumbprint {} from {}", sha256Thumbprint, serverUrl);
		try {
			X509Certificate certificate = getCertificate(serverUrl, sha256Thumbprint);
			if (certificate == null) {
				throw new SSLException(String.format("Did not find certificate that matches the thumbprint %s", sha256Thumbprint));
			}

			log.debug("Creating a Trust Store to hold the connection certificate");
			KeyStore gdbTrustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			gdbTrustStore.load(null, null);
			log.debug("Importing the certificate to the new Trust Store");
			gdbTrustStore.setCertificateEntry(sha256Thumbprint, certificate);

			X509TrustManager defaultTrustManager = getTrustManagerForStore(null);
			X509TrustManager gdbTrustManager = getTrustManagerForStore(gdbTrustStore);
			log.debug("Creating the composite Trust Manager");
			CompositeTrustManager compositeTrustManager = new CompositeTrustManager(defaultTrustManager, gdbTrustManager);

			log.debug("Registering the composite Trust Manager");
			SSLContext context = SSLContext.getInstance("TLS");
			context.init(null, new TrustManager[]{compositeTrustManager}, null);
			HttpClientBuilder builder = getClientBuilder()
				.setSSLContext(context);
			if (!hostnameVerificationEnabled) {
				builder.setSSLHostnameVerifier((hostname, session) -> true);
			}

			return builder.build();
		} catch (Exception e) {
			log.error("Failed to initialize the TLS context due to exception", e);
			throw new SSLException(e);
		}

	}

	private static HttpClientBuilder getClientBuilder() {
		return HttpClientBuilder.create()
			.evictExpiredConnections()
			.setRetryHandler(new RetryHandlerStale())
			.setServiceUnavailableRetryStrategy(new ServiceUnavailableRetryHandler())
			.useSystemProperties()
			.setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build());
	}

	private static X509TrustManager getTrustManagerForStore(KeyStore keyStore) throws NoSuchAlgorithmException, KeyStoreException {
		log.debug("Getting a trust manager instance from the Trust Store");
		TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(keyStore);
		for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
			if (trustManager instanceof X509TrustManager) {
				return (X509TrustManager) trustManager;
			}
		}
		return null;
	}


	public static boolean isUrlHttps(String url) {
		return url != null && url.startsWith("https");
	}

	private static String sanitize(String sha256Thumbprint) {
		return sha256Thumbprint.replaceAll(":", "").toLowerCase();
	}


	/**
	 * Copy of {@link org.eclipse.rdf4j.http.client.SharedHttpClientSessionManager}'s inner RetryHandlerStale which is private and, therefore inaccessible
	 */
	private static class RetryHandlerStale implements HttpRequestRetryHandler {
		private final Logger logger = LoggerFactory.getLogger(RetryHandlerStale.class);

		@Override
		public boolean retryRequest(IOException ioe, int count, HttpContext context) {
			// only try this once
			if (count > 1) {
				return false;
			}
			HttpClientContext clientContext = HttpClientContext.adapt(context);
			HttpConnection conn = clientContext.getConnection();
			if (conn != null) {
				synchronized (this) {
					if (conn.isStale()) {
						try {
							logger.warn("Closing stale connection");
							conn.close();
							return true;
						} catch (IOException e) {
							logger.error("Error closing stale connection", e);
						}
					}
				}
			}
			return false;
		}
	}

	/**
	 * Copy of {@link org.eclipse.rdf4j.http.client.SharedHttpClientSessionManager}'s inner ServiceUnavailableRetryHandler which is private and, therefore inaccessible
	 */
	private static class ServiceUnavailableRetryHandler implements ServiceUnavailableRetryStrategy {
		private final Logger logger = LoggerFactory.getLogger(ServiceUnavailableRetryHandler.class);

		@Override
		public boolean retryRequest(HttpResponse response, int executionCount, HttpContext context) {
			// only retry on `408`
			if (response.getStatusLine().getStatusCode() != HttpURLConnection.HTTP_CLIENT_TIMEOUT) {
				return false;
			}

			// when `keepAlive` is disabled every connection is fresh (with the default `useSystemProperties` http
			// client configuration we use), a 408 in that case is an unexpected issue we don't handle here
			String keepAlive = System.getProperty("http.keepAlive", "true");
			if (!"true".equalsIgnoreCase(keepAlive)) {
				return false;
			}

			// worst case, the connection pool is filled to the max and all of them idled out on the server already
			// we then need to clean up the pool and finally retry with a fresh connection. Hence, we need at most
			// pooledConnections+1 retries.
			// the pool size setting used here is taken from `HttpClientBuilder` when `useSystemProperties()` is used
			int pooledConnections = Integer.parseInt(System.getProperty("http.maxConnections", "5"));
			if (executionCount > (pooledConnections + 1)) {
				return false;
			}

			HttpClientContext clientContext = HttpClientContext.adapt(context);
			HttpConnection conn = clientContext.getConnection();

			synchronized (this) {
				try {
					logger.info("Cleaning up closed connection");
					conn.close();
					return true;
				} catch (IOException e) {
					logger.error("Error cleaning up closed connection", e);
				}
			}
			return false;
		}

		@Override
		public long getRetryInterval() {
			return 1000;
		}
	}
}
