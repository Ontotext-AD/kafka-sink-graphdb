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


import com.ontotext.kafka.logging.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Properties;

public class VersionUtil {


	private static String version = "0.0.1";
	private static final Logger log = LoggerFactory.getLogger(VersionUtil.class);


	private VersionUtil() {
		throw new IllegalStateException("Utility class");
	}

	static {
		Properties properties = new Properties();
		try {
			properties.load(VersionUtil.class.getResourceAsStream("/graphdb-kafka-version.properties"));
		} catch (IOException e) {
			log.warn("Could not load graphdb-kafka-version.properties file. Check if file exists. Will use default version {}", version, e);
		}
		version = properties.getProperty("graphdb.version", version).trim();
	}

	public static String getVersion() {
		return version;
	}
}
