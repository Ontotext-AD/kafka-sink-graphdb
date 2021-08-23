/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ontotext.kafka.util;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesUtil.class);

	private static String version = "unknown";

	private PropertiesUtil() {}

	static {
		try {
			Properties props = new Properties();
			props.load(PropertiesUtil.class.getResourceAsStream("/kafka-connect-mongodb-version.properties"));
			version = props.getProperty("version", version).trim();
		} catch (Exception e) {
			LOGGER.warn("error while loading version:", e);
		}
	}

	public static String getVersion() {
		return version;
	}

}
