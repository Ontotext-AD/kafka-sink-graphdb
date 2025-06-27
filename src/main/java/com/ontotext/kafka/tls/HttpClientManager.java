package com.ontotext.kafka.tls;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.gdb.GdbConnectionConfig;
import com.ontotext.kafka.gdb.GdbConnectionConfigException;
import com.ontotext.kafka.gdb.auth.AuthHeaderConfig;
import com.ontotext.kafka.gdb.auth.MtlsConfig;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpConnection;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.*;
import java.security.cert.*;
import java.security.cert.Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collection;

/**
 * A utility class to confiure the TLS connection between the Kafka Sink Connector (client) and downstream GraphDB instance (server)
 * The class takes care of configuring the default {@link javax.net.ssl.SSLContext} for all following connections by using a CompositeTrustManager
 */
public final class HttpClientManager {
	private static final Logger log = LoggerFactory.getLogger(HttpClientManager.class);

	private HttpClientManager() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Parses the certificates provided in the connection to `connectionUrl` and finds the certificate that matches the provided thumbprint (SHA-256)
	 * @param connectionUrl    - the server url to perform the check against
	 * @param sha256Thumbprint - the SHA-256 thumbprint of the certificate to retrieve
	 * @param keyManagers       - Key manager that contains the client's credentials (if using mTLS)
	 * @param clientCertificate - The Client certificate (if using mTLS)
	 * @return The certificate, or `null` if no certificate was found that matches the thumbprint
	 * @throws IOException              if any error occurs while establishing or working with an active connection to the server
	 * @throws IllegalArgumentException if the `connectionUrl` is invalid
	 * @throws RuntimeException         if any other error occurs during validation that are not expected
	 */
	private static X509Certificate getCertificate(String connectionUrl, String sha256Thumbprint, KeyManager[] keyManagers, X509Certificate clientCertificate) throws IOException {
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
			SSLContext ctx = SSLContext.getInstance("TLS");
			ctx.init(keyManagers, new TrustManager[]{new TrustAllManager(clientCertificate)}, null);
			conn.setSSLSocketFactory(ctx.getSocketFactory());
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
			throw new GdbConnectionConfigException(GraphDBSinkConfig.SERVER_URL, connectionUrl, String.format("Invalid URL - %s", e.getMessage()));
		} catch (CertificateEncodingException e) {
			throw new GdbConnectionConfigException(GraphDBSinkConfig.TLS_THUMBPRINT, sha256Thumbprint, String.format(String.format("Invalid thumbprint provided - %s", e.getMessage())));
		} catch (SSLException e) {
			String msg = e.getMessage();
			if (StringUtils.containsIgnoreCase(msg, "handshake") && clientCertificate != null && keyManagers != null) {
				throw new GdbConnectionConfigException(GraphDBSinkConfig.MTLS_CERTIFICATE_STRING, "[redacted]", "mTLS handshake failed most probably due to invalid client certificate");
			}
			throw new GdbConnectionConfigException(GraphDBSinkConfig.SERVER_URL, connectionUrl, msg);
		} catch (Exception e) {
			throw new GdbConnectionConfigException(GraphDBSinkConfig.SERVER_URL, connectionUrl, e.getMessage());
		}
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
	private static X509Certificate getCertificate(String connectionUrl, String sha256Thumbprint) throws IOException {
		return getCertificate(connectionUrl, sha256Thumbprint, null, null);
	}


	/**
	 * Initialize the TLS context for working with the provided server. This will configure the default {@link SSLContext} for all communication
	 *
	 * @param config = The connection configuration that contains all required parameters for creating an HTTP(S) connection
	 * @throws SSLException             - if any error occured during configuration
	 * @throws IllegalArgumentException - if no thumbprint provided by the server url is https
	 */
	public static CloseableHttpClient createHttpClient(GdbConnectionConfig config) throws SSLException {
		String serverUrl = config.getServerUrl();
		String sha256Thumbprint = config.getTlsThumbprint();
		if (!isUrlHttps(serverUrl) || StringUtils.isEmpty(sha256Thumbprint)) {
			log.info("Not an HTTPS connection, or no thumbprint provided. Skipping creation of custom HTTPClient");
			return getClientBuilder().build();
		}
		try {
			KeyManager[] keyManagers = null;
			Collection<BasicHeader> headers = new ArrayList<>();
			X509Certificate serverCertificate = null;
			if (config.getAuthType() == GraphDBSinkConfig.AuthenticationType.MTLS) {
				X509Certificate clientCertificate = getCertificateFromPEM(config.getMtlsConfig().getCertificate());
				keyManagers = createKeyManagers(config, clientCertificate);
				serverCertificate = getCertificate(serverUrl, sha256Thumbprint, keyManagers, clientCertificate);
			} else {
				serverCertificate = getCertificate(serverUrl, sha256Thumbprint);
			}
			if (config.getAuthType() == GraphDBSinkConfig.AuthenticationType.X509_HEADER) {
				headers = createX509Header(config);
			}

			log.info("Certificate that matches thumbprint {} found for server URL {}", sha256Thumbprint, serverUrl);

			if (serverCertificate == null) {
				throw new GdbConnectionConfigException(GraphDBSinkConfig.TLS_THUMBPRINT, sha256Thumbprint, String.format("Did not find certificate that matches the thumbprint %s", sha256Thumbprint));
			}

			log.debug("Creating a Trust Store to hold the connection certificate");
			KeyStore gdbTrustStore = KeyStore.getInstance(KeyStore.getDefaultType());
			gdbTrustStore.load(null, null);
			log.debug("Importing the certificate to the new Trust Store");
			gdbTrustStore.setCertificateEntry(sha256Thumbprint, serverCertificate);

			X509TrustManager defaultTrustManager = getTrustManagerForStore(null);
			X509TrustManager gdbTrustManager = getTrustManagerForStore(gdbTrustStore);
			log.debug("Creating the composite Trust Manager");
			CompositeTrustManager compositeTrustManager = new CompositeTrustManager(defaultTrustManager, gdbTrustManager);

			log.debug("Registering the composite Trust Manager");
			SSLContext context = SSLContext.getInstance("TLS");
			context.init(keyManagers, new TrustManager[]{compositeTrustManager}, null);
			HttpClientBuilder builder = getClientBuilder()
				.setDefaultHeaders(headers)
				.setSSLContext(context);
			if (!config.isHostnameVerificationEnabled()) {
				builder.setSSLHostnameVerifier((hostname, session) -> true);
			}

			return builder.build();
		}
		catch (Exception e) {
			log.error("Failed to initialize the TLS context due to exception", e);
			if (e instanceof GdbConnectionConfigException) {
				throw (GdbConnectionConfigException) e;
			}
			throw new SSLException(e);
		}

	}


	private static Collection<BasicHeader> createX509Header(GdbConnectionConfig config) throws SSLException {
		Collection<BasicHeader> headers = new ArrayList<>();
		AuthHeaderConfig headerConfig = config.getAuthHeaderConfig();
		if (StringUtils.isEmpty(headerConfig.getHeaderName()) || StringUtils.isEmpty(headerConfig.getCertificate())) {
			throw new SSLException("Using header-based certificate authentication but no certificate provided");
		}
		headers.add(new BasicHeader(headerConfig.getHeaderName(), headerConfig.getCertificate()));
		return headers;
	}

	private static KeyManager[] createKeyManagers(
		GdbConnectionConfig config,
		X509Certificate clientCertificate) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, OperatorCreationException, PKCSException, CertificateException, KeyStoreException, UnrecoverableKeyException {
		MtlsConfig mtlsConfig = config.getMtlsConfig();
		char[] password;
		PrivateKey privateKey;
		if (mtlsConfig.getKeyPassword() != null && StringUtils.isNotEmpty(mtlsConfig.getKeyPassword().value())) {
			password = mtlsConfig.getKeyPassword().value().toCharArray();
			privateKey = getPrivateKeyFromPEM(mtlsConfig.getKey(), password);
		} else {
			privateKey = getPrivateKeyFromPEM(mtlsConfig.getKey(), null);
			password = RandomStringUtils.randomAlphanumeric(10).toCharArray();
		}
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
		keyStore.load(null, password);
		keyStore.setKeyEntry("key", privateKey, password, new Certificate[]{clientCertificate});
		kmf.init(keyStore, password);
		return kmf.getKeyManagers();
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

	public static X509Certificate getCertificateFromPEM(String pem) throws IOException, CertificateException {
		Object pemObject = readPEMObject(pem);

		if (pemObject instanceof X509CertificateHolder) {
			X509CertificateHolder holder = (X509CertificateHolder) pemObject;
			InputStream in = new ByteArrayInputStream(holder.getEncoded());
			CertificateFactory cf = CertificateFactory.getInstance("X.509");
			Certificate c = cf.generateCertificate(in);
			if (c instanceof X509Certificate) {
				return (X509Certificate) c;
			}
			throw new IllegalArgumentException("Certificate is not a X509 certificate");
		}
		throw new IllegalArgumentException("PEM object is not a X509 certificate");
	}

	public static PrivateKey getPrivateKeyFromPEM(String pem, char[] password) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, PKCSException, OperatorCreationException {
		Object pemObject = readPEMObject(pem);
		KeyFactory kf = KeyFactory.getInstance("RSA");

		if (pemObject instanceof PrivateKeyInfo) {
			return kf.generatePrivate(new PKCS8EncodedKeySpec(((PrivateKeyInfo) pemObject).getEncoded()));
		}
		if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo && ArrayUtils.isNotEmpty(password)) {
			PKCS8EncryptedPrivateKeyInfo pkey = (PKCS8EncryptedPrivateKeyInfo) pemObject;
			InputDecryptorProvider decryptor = new JceOpenSSLPKCS8DecryptorProviderBuilder().setProvider(new BouncyCastleProvider()).build(password);
			PrivateKeyInfo keyInfo = pkey.decryptPrivateKeyInfo(decryptor);
			return kf.generatePrivate(new PKCS8EncodedKeySpec(keyInfo.getEncoded()));
		}
		if (ArrayUtils.isEmpty(password)) {
			throw new IllegalArgumentException("Private Key is encrypted but no password provided");
		}
		throw new IllegalArgumentException("PEM object is not a Private Key");
	}


	private static Object readPEMObject(String str) throws IOException {
		PEMParser pemParser = new PEMParser(new StringReader(str));
		return pemParser.readObject();

	}


	private static class TrustAllManager implements X509TrustManager {
		private final X509Certificate clientCertificate;

		public TrustAllManager(X509Certificate clientCertificate) {
			this.clientCertificate = clientCertificate;
		}

		@Override
		public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

		}

		@Override
		public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

		}

		@Override
		public X509Certificate[] getAcceptedIssuers() {
			if (clientCertificate != null) {
				return new X509Certificate[]{clientCertificate};
			}
			return new X509Certificate[0];
		}
	}
}
