package com.ontotext.kafka.convert;

import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class JsonDataConverterTest {


	@Test
	void test_convertToMap_ok() {
		String ttl = "<urn:a> <urn:b> <urn:c> .";
		JsonDataConverter converter = new JsonDataConverter(RDFFormat.TURTLE);
		Map<String, Object> data = converter.convert(ttl.getBytes(StandardCharsets.UTF_8));
		assertThat(data).isNotNull();
		assertThat(data).containsEntry("@id", "urn:a");
		assertThat(data).containsEntry("urn:b", Collections.singletonList(Collections.singletonMap("@id", "urn:c")));
	}
}
