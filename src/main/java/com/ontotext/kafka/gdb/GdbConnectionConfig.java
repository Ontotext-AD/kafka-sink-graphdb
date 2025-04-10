package com.ontotext.kafka.gdb;

import com.ontotext.kafka.GraphDBSinkConfig;
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

	public GdbConnectionConfig(GraphDBSinkConfig config) {
		this(config.getServerUrl(), config.getRepositoryId(), config.getTlsThumbprint(), config.getAuthType(), config.getAuthBasicUser(),
			config.getAuthBasicPassword());
	}

	public GdbConnectionConfig(Config config) {
		Map<String, ConfigValue> valueMap = config.configValues().stream()
			.collect(Collectors.toMap(ConfigValue::name, Function.identity()));
		this.serverUrl = getValue(valueMap.get(SERVER_URL), true);
		this.repositoryId = getValue(valueMap.get(REPOSITORY), true);
		this.tlsThumbprint = getValue(valueMap.get(TLS_THUMBPRINT), false);
		this.authType = AuthenticationType.valueOf(getValue(valueMap.get(AUTH_TYPE), true).toUpperCase());
		this.username = getValue(valueMap.get(AUTH_BASIC_USER), this.authType != GraphDBSinkConfig.AuthenticationType.NONE);
		this.password = getValue(valueMap.get(AUTH_BASIC_PASS), Password.class, this.authType != GraphDBSinkConfig.AuthenticationType.NONE);
	}

	public GdbConnectionConfig(String serverUrl, String repositoryId, String tlsThumbprint, GraphDBSinkConfig.AuthenticationType authType, String authBasicUser,
							   Password authBasicPassword) {
		this.serverUrl = getValue(serverUrl, true);
		this.repositoryId = getValue(repositoryId, true);
		this.tlsThumbprint = getValue(tlsThumbprint, false);
		this.authType = getValue(authType, true);
		this.username = getValue(authBasicUser, this.authType != GraphDBSinkConfig.AuthenticationType.NONE);
		this.password = getValue(authBasicPassword, this.authType != GraphDBSinkConfig.AuthenticationType.NONE);
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
