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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class VersionUtil {

	private static final Logger LOG = LoggerFactory.getLogger(VersionUtil.class);
	private static String version = "0.0.1";
	private static Properties properties;

	private VersionUtil() {
	}

	static {
		try {
			properties = new Properties();
			properties.load(VersionUtil.class.getResourceAsStream("/graphdb-kafka-version.properties"));
			properties.load(VersionUtil.class.getResourceAsStream("/graphdb-kafka-sink.properties"));
			version = properties.getProperty("graphdb.version", version).trim();
		} catch (Exception e) {
			LOG.info("error while loading version:", e);
		}
	}

	public static String getVersion() {
		return version;
	}
}
