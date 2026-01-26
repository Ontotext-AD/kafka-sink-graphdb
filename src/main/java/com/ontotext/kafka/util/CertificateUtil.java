package com.ontotext.kafka.util;

import org.apache.commons.lang3.ArrayUtils;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

/**
 * A utility class that reads, parses and processes X509 objects
 */
public final class CertificateUtil {

	private CertificateUtil() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Parses a certificate string and returns the corresponding X509 certificate object
	 *
	 * @param pem The certificate string
	 * @return The X509 Certificate
	 * @throws IOException          if the certificate string cannot be parsed
	 * @throws CertificateException if the provided string does not correspond to a valid certificate
	 */
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

	/**
	 * Parses ac certificate string and retrieves the private key
	 * @param pem The certificate string
	 * @param password If the key is encrypted, holds the decryption passphrase
	 * @return The private key
	 */
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
}
