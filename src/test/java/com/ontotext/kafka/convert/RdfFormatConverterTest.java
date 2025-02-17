package com.ontotext.kafka.convert;

import org.eclipse.rdf4j.rio.RDFParseException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.eclipse.rdf4j.rio.RDFFormat.JSONLD;
import static org.eclipse.rdf4j.rio.RDFFormat.TURTLE;

class RdfFormatConverterTest {


	@Test
	void test_convertTurtleToJsonLD_and_back_ok() throws IOException {
		RdfFormatConverter converter = new RdfFormatConverter(TURTLE, JSONLD);
		String ttl = "<urn:a> <urn:b> <urn:c> .";
		byte[] converted = converter.convertData(ttl.getBytes(Charset.defaultCharset()));
		assertThat(converted).isNotEmpty();
		converter = new RdfFormatConverter(JSONLD, TURTLE);
		byte[] reverseConverted = converter.convertData(converted);
		assertThat(reverseConverted).isNotEmpty();
		String s = new String(reverseConverted, Charset.defaultCharset());
		assertThat(s.trim()).isEqualTo(ttl);
	}

	@Test
	void test_convertInvalidDataToJsonLD_fail() throws IOException {
		RdfFormatConverter converter = new RdfFormatConverter(TURTLE, JSONLD);
		String ttl = "<urn:a> <urn:b> <urn:c> INVALID";
		assertThatCode(() -> converter.convertData(ttl.getBytes(Charset.defaultCharset()))).isInstanceOf(RDFParseException.class)
			.hasMessageContaining("Expected '.', found 'I'");
	}

	@Test
	void test_convertJsonLD_toJsonLD_skipConversionAndReturnData() {
		// Test with invalid data to verify that no actual conversion takes place
		RdfFormatConverter converter = new RdfFormatConverter(TURTLE, TURTLE);
		String ttl = "<urn:a> <urn:b> <urn:c> INVALID";
		assertThatCode(() -> converter.convertData(ttl.getBytes(Charset.defaultCharset()))).as(
			"Conversion between same formats does not take place, thus exception is not thrown").doesNotThrowAnyException();
	}

}
