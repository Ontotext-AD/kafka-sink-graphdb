package com.ontotext.kafka.tls;


import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Trusts all connections without performing any validation on the server certificate. This is only for initial validation purposes, not to be used during normal operations
 */
public class TrustAllSSLContext {

	public static final SSLContext SSL_CONTEXT = createContext();

	public static SSLContext createContext() {
		try {
			SSLContext ctx = SSLContext.getInstance("TLS");
			ctx.init(null, new TrustManager[]{new TrustAllManager()}, null);
			return ctx;
		} catch (Exception e) {
			// We should not really get here
			throw new RuntimeException(e);
		}

	}

	private static class TrustAllManager implements X509TrustManager {
		@Override
		public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

		}

		@Override
		public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {

		}

		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return new X509Certificate[0];
		}
	}


}
