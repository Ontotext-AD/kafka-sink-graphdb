package com.ontotext.kafka.test.framework;

import org.apache.commons.lang3.RandomStringUtils;

public class TestUtils {

	public static String getRandomString(int len) {
		return RandomStringUtils.randomAlphanumeric(len);
	}
}
