package com.ontotext.kafka.gdb;

import com.ontotext.kafka.GraphDBSinkConfig;
import com.ontotext.kafka.gdb.auth.AuthHeaderConfig;
import com.ontotext.kafka.gdb.auth.MtlsConfig;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;
import java.util.Objects;
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
			config.getAuthMTlsClientCertificate(),
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

		String mTlsClientCertificate = getValue(valueMap.get(MTLS_CERTIFICATE_STRING), String.class, this.authType == AuthenticationType.MTLS);
		String mTlsClientCertificateKey = getValue(valueMap.get(MTLS_CERTIFICATE_KEY_STRING), String.class, this.authType == AuthenticationType.MTLS);
		String mTlsClientCertificateKeyPassword = getValue(valueMap.get(MTLS_CERTIFICATE_KEY_PASSWORD), String.class, false);
		String authHeaderCertificate = getValue(valueMap.get(AUTH_HEADER_CERTIFICATE_STRING), String.class, this.authType == AuthenticationType.X509_HEADER);
		String authHeaderName = getValue(valueMap.get(AUTH_HEADER_NAME), String.class, this.authType == AuthenticationType.X509_HEADER);

		this.mtlsConfig = new MtlsConfig(mTlsClientCertificate, mTlsClientCertificateKey, authHeaderCertificate);
		this.authHeaderConfig = new AuthHeaderConfig(mTlsClientCertificateKey, mTlsClientCertificateKeyPassword);
	}

	public GdbConnectionConfig(String serverUrl, String repositoryId, String tlsThumbprint, boolean hostnameVerificationEnabled,
							   GraphDBSinkConfig.AuthenticationType authType, String authBasicUser,
							   Password authBasicPassword, String mTlsClientCertificate, String mTlsClientCertificateKey, String mTlsClientCertificateKeyPassword, String authHeaderCertificate,
							   String authHeaderName) {
		this.serverUrl = getValue(serverUrl, true);
		this.repositoryId = getValue(repositoryId, true);
		this.tlsThumbprint = getValue(tlsThumbprint, false);
		this.authType = getValue(authType, true);
		this.username = getValue(authBasicUser, this.authType == AuthenticationType.MTLS);
		this.password = getValue(authBasicPassword, this.authType == AuthenticationType.MTLS);
		this.hostnameVerificationEnabled = getValue(hostnameVerificationEnabled, false);

		// Validation of values provided
		mTlsClientCertificate = getValue(mTlsClientCertificate, this.authType == AuthenticationType.MTLS);
		mTlsClientCertificateKey = getValue(mTlsClientCertificateKey, this.authType == AuthenticationType.MTLS);
		mTlsClientCertificateKeyPassword = getValue(mTlsClientCertificateKeyPassword, false);
		authHeaderCertificate = getValue(authHeaderCertificate, this.authType == AuthenticationType.X509_HEADER);
		authHeaderName = getValue(authHeaderName, this.authType == AuthenticationType.X509_HEADER);
		this.mtlsConfig = new MtlsConfig(mTlsClientCertificate, mTlsClientCertificateKey, authHeaderCertificate);
		this.authHeaderConfig = new AuthHeaderConfig(mTlsClientCertificateKey, mTlsClientCertificateKeyPassword);

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
			return type.cast(getValue(value.value(), failOnNull));
		} catch (NullPointerException e) {
			throw new GdbConnectionConfigException(value.name(), null, "Must provide value");
		} catch (ClassCastException e) {
			throw new GdbConnectionConfigException(value.name(), value.value(), "Invalid value");
		}

	}

	private <T> T getValue(T val, boolean failOnNull) {
		if (failOnNull) {
			return Objects.requireNonNull(val);
		}
		return val;
	}

}
