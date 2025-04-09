package com.ontotext.kafka.tls;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;

public class CompositeTrustManager implements X509TrustManager {

	private final X509TrustManager defaultTrustManager;
	private final X509TrustManager gdbTrustManager;

	public CompositeTrustManager(X509TrustManager defaultTrustManager, X509TrustManager gdbTrustManager) {
		this.defaultTrustManager = defaultTrustManager;
		this.gdbTrustManager = gdbTrustManager;
	}

	private X509Certificate[] mergeCertificates() {
		ArrayList<X509Certificate> resultingCerts = new ArrayList<>();
		resultingCerts.addAll(Arrays.asList(defaultTrustManager.getAcceptedIssuers()));
		resultingCerts.addAll(Arrays.asList(gdbTrustManager.getAcceptedIssuers()));
		return resultingCerts.toArray(new X509Certificate[0]);
	}

	@Override
	public X509Certificate[] getAcceptedIssuers() {
		return mergeCertificates();
	}

	@Override
	public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
		try {
			gdbTrustManager.checkServerTrusted(chain, authType);
		} catch (CertificateException e) {
			defaultTrustManager.checkServerTrusted(chain, authType);
		}
	}

	@Override
	public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
		defaultTrustManager.checkClientTrusted(mergeCertificates(), authType);
	}
}
