package com.ontotext.kafka.gdb;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.gdb.auth.AuthHeaderConfig;
import com.ontotext.kafka.gdb.auth.MtlsConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;

import java.util.Base64;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ontotext.kafka.GraphDBSinkConfig.*;

public class GdbConnectionConfig {

	private final String serverUrl;
	private final String repositoryId;
	private final String tlsThumbprint;
	private final GraphDBSinkConfig.AuthenticationType authType;
	private final String username;
	private final Password password;
	private final boolean hostnameVerificationEnabled;
	private final MtlsConfig mtlsConfig;
	private final AuthHeaderConfig authHeaderConfig;


	public GdbConnectionConfig(GraphDBSinkConfig config) {
		this(config.getServerUrl(), config.getRepositoryId(), config.getTlsThumbprint(), config.isHostnameVerificationEnabled(), config.getAuthType(),
			config.getAuthBasicUser(),
			config.getAuthBasicPassword(),
			config.getAuthMTlsClientCertificate(),
			config.getAuthMTlsClientCertificateKey(),
			config.getAuthMTlsClientCertificateKeyPassword(),
			config.getAuthCertificateHeaderString(),
			config.getAuthCertificateHeaderName());
	}

	public GdbConnectionConfig(Config config) {
		Map<String, ConfigValue> valueMap = config.configValues().stream()
			.collect(Collectors.toMap(ConfigValue::name, Function.identity()));
		this.serverUrl = getValue(valueMap.get(SERVER_URL), true);
		this.repositoryId = getValue(valueMap.get(REPOSITORY), true);
		this.tlsThumbprint = getValue(valueMap.get(TLS_THUMBPRINT), false);
		this.authType = AuthenticationType.valueOf(getValue(valueMap.get(AUTH_TYPE), true).toUpperCase());
		this.username = getValue(valueMap.get(AUTH_BASIC_USER), this.authType == AuthenticationType.BASIC);
		this.password = getValue(valueMap.get(AUTH_BASIC_PASS), Password.class, this.authType == AuthenticationType.BASIC);
		this.hostnameVerificationEnabled = getValue(valueMap.get(HOSTNAME_VERIFICATION), Boolean.class, false);

		Base64.Decoder b64Decoder = Base64.getDecoder();
		String mTlsClientCertificate = decode(getValue(valueMap.get(MTLS_CERTIFICATE_STRING), String.class, this.authType == AuthenticationType.MTLS));
		String mTlsClientCertificateKey = decode(getValue(valueMap.get(MTLS_CERTIFICATE_KEY_STRING), String.class, this.authType == AuthenticationType.MTLS));
		Password mTlsClientCertificateKeyPassword = getValue(valueMap.get(MTLS_CERTIFICATE_KEY_PASSWORD), Password.class, false);
		String authHeaderCertificate = decode(getValue(valueMap.get(AUTH_HEADER_CERTIFICATE_STRING), String.class, this.authType == AuthenticationType.X509_HEADER));
		String authHeaderName = getValue(valueMap.get(AUTH_HEADER_NAME), String.class, this.authType == AuthenticationType.X509_HEADER);

		this.mtlsConfig = new MtlsConfig(mTlsClientCertificate, mTlsClientCertificateKey, mTlsClientCertificateKeyPassword);
		this.authHeaderConfig = new AuthHeaderConfig(authHeaderCertificate, authHeaderName);
	}

	public GdbConnectionConfig(String serverUrl, String repositoryId, String tlsThumbprint, boolean hostnameVerificationEnabled,
							   AuthenticationType authType, String authBasicUser,
							   Password authBasicPassword, String mTlsClientCertificate, String mTlsClientCertificateKey, Password mTlsClientCertificateKeyPassword, String authHeaderCertificate,
							   String authHeaderName) {
		this.serverUrl = getValue(SERVER_URL, serverUrl, true);
		this.repositoryId = getValue(REPOSITORY, repositoryId, true);
		this.tlsThumbprint = getValue(TLS_THUMBPRINT, tlsThumbprint, false);
		this.authType = getValue(AUTH_TYPE, authType, true);
		this.username = getValue(AUTH_BASIC_USER, authBasicUser, this.authType == AuthenticationType.MTLS);
		this.password = getValue(AUTH_BASIC_PASS, authBasicPassword, this.authType == AuthenticationType.MTLS);
		this.hostnameVerificationEnabled = getValue(HOSTNAME_VERIFICATION, hostnameVerificationEnabled, false);

		Base64.Decoder b64Decoder = Base64.getDecoder();
		// Validation of values provided
		mTlsClientCertificate = decode(getValue(MTLS_CERTIFICATE_STRING, mTlsClientCertificate, this.authType == AuthenticationType.MTLS));
		mTlsClientCertificateKey = decode(getValue(MTLS_CERTIFICATE_KEY_STRING, mTlsClientCertificateKey, this.authType == AuthenticationType.MTLS));
		authHeaderCertificate = decode(getValue(AUTH_HEADER_CERTIFICATE_STRING, authHeaderCertificate, this.authType == AuthenticationType.X509_HEADER));
		authHeaderName = getValue(AUTH_HEADER_NAME, authHeaderName, this.authType == AuthenticationType.X509_HEADER);
		this.mtlsConfig = new MtlsConfig(mTlsClientCertificate, mTlsClientCertificateKey, mTlsClientCertificateKeyPassword);
		this.authHeaderConfig = new AuthHeaderConfig(authHeaderCertificate, authHeaderName);

	}

	private String decode(String value) {
		return StringUtils.isNotEmpty(value) ? new String(Base64.getDecoder().decode(value)) : value;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public String getRepositoryId() {
		return repositoryId;
	}

	public String getTlsThumbprint() {
		return tlsThumbprint;
	}

	public AuthenticationType getAuthType() {
		return authType;
	}

	public String getUsername() {
		return username;
	}

	public Password getPassword() {
		return password;
	}


	public MtlsConfig getMtlsConfig() {
		return mtlsConfig;
	}

	public AuthHeaderConfig getAuthHeaderConfig() {
		return authHeaderConfig;
	}

	public boolean isHostnameVerificationEnabled() {
		return hostnameVerificationEnabled;
	}

	private String getValue(ConfigValue value, boolean failOnNull) {
		return getValue(value, String.class, failOnNull);
	}


	private <T> T getValue(ConfigValue value, Class<T> type, boolean failOnNull) {
		try {
			return type.cast(getValue(value.name(), value.value(), failOnNull));
		} catch (NullPointerException e) {
			throw new GdbConnectionConfigException(value.name(), null, "Must provide value");
		} catch (ClassCastException e) {
			throw new GdbConnectionConfigException(value.name(), value.value(), "Invalid value");
		}

	}

	private <T> T getValue(String configName, T val, boolean failOnNull) {
		if (failOnNull && val == null) {
			throw new GdbConnectionConfigException(configName, null, "Must provide value");
		}
		return val;
	}

}
