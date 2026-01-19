package com.ontotext.kafka.util;

import com.ontotext.kafka.transformation.RdfTransformation;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Map;

public class TransformationsValidator extends AbstractConfigValidator {


	@Override
	protected void doValidate(Config config, Map<String, String> connectorConfigs) {
		String transformationsNames = connectorConfigs.get("transforms");
		if (StringUtils.isNotEmpty(transformationsNames)) {
			getLog().debug("Got transformations {}", transformationsNames);
			for (String transformation : transformationsNames.split(",")) {
				if (transformation.isEmpty()) {
					getLog().warn("Invalid transformation name: {}", transformation);
					break;
				}
				String transformationClass = getTransformationClass(transformation.trim(), connectorConfigs);
				try {
					if (transformationClass == null) {
						getLog().warn("Did not find transformation class for transformation {}.", transformation);
						break;
					}
					Class<?> clazz = Class.forName(transformationClass);
					if (RdfTransformation.class.isAssignableFrom(clazz)) {
						RdfTransformation transform = (RdfTransformation) clazz
							.getDeclaredConstructor()
							.newInstance();
						transform.validateConfig(transformation, connectorConfigs);
						getLog().debug("Transformation {} validated", transformation);

					}
				} catch (ClassNotFoundException e) {
					getLog().warn("Transformation class not found: {}", transformationClass, e);
				} catch (ReflectiveOperationException e) {
					getLog().warn("Transformation class cannot be initialized: {}", transformationClass, e);
				} catch (ConfigException e) {
					getLog().error("Transformation class did not validate - {}", transformationClass, e);
					ConfigValue transformationValidationError = new ConfigValue(
						String.format("transforms.%s", transformation));
					transformationValidationError.addErrorMessage("Invalid transformation configurations.");
					config.configValues().add(transformationValidationError);
				}
			}
		}
	}

	private String getTransformationClass(String transformationName, final Map<String, String> connectorConfigs) {
		if (StringUtils.isNotEmpty(transformationName)) {
			String transformTypeKey = String.format("transforms.%s.type", transformationName);
			String transformType = connectorConfigs.get(transformTypeKey);
			if (StringUtils.isEmpty(transformType)) {
				getLog().warn("Missing or empty type for transformation '{}'. Expected key: {}", transformationName,
					transformTypeKey);
				return null;
			}
			return transformType;
		}
		return null;
	}
}
