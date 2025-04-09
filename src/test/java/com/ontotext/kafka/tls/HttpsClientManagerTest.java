package com.ontotext.kafka.tls;

import org.junit.jupiter.api.Test;

import javax.net.ssl.X509TrustManager;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

class HttpClientManagerTest {

	@Test
	void test() throws NoSuchAlgorithmException, KeyStoreException {
		X509TrustManager a = HttpClientManager.getTrustManagerForStore(null);
	}

}
